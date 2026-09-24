use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::runs::{RunRecord, TaskStatus};
use crate::task::{JobCmd, Task};

pub type BodyFut = Pin<Box<dyn Future<Output = ()> + Send>>;
pub type BodyFn =
    Box<dyn Fn(CancellationToken, CmdTx) -> BodyFut + Send + Sync>;
pub type CmdTx = Option<mpsc::Sender<JobCmd>>;
const DEFAULT_GATE_RETRY_DELAY: Duration = Duration::from_secs(30);

// A Task bound to a resolved body and its own pre/concurrent/post children, built by startup.rs.
pub struct RunnableTask {
    pub task: Arc<Task>,
    pub run: BodyFn,
    pub pre: Vec<Arc<RunnableTask>>,
    pub concurrent: Vec<Arc<RunnableTask>>,
    pub post: Vec<Arc<RunnableTask>>,
}

enum IterationResult {
    Continue, // ran; loop again
    Skipped,  // pre rejected; wait then retry
    Stop,     // cmdStop; done
}

pub async fn run_task(task: Arc<RunnableTask>, run_tx: mpsc::Sender<RunRecord>) {
    let task_ctx = CancellationToken::new();

    loop {
        match run_iteration(&task_ctx, &task, &run_tx).await {
            IterationResult::Stop => return,
            IterationResult::Skipped => tokio::time::sleep(gate_retry_delay(&task.task)).await,
            IterationResult::Continue => {}
        }
    }
}

async fn run_iteration(
    task_ctx: &CancellationToken,
    j: &RunnableTask,
    run_tx: &mpsc::Sender<RunRecord>,
) -> IterationResult {
    let start = jiff::Timestamp::now();
    if let Some(cmd) = run_waiting_slot(task_ctx.clone(), &j.pre).await {
        let iteration_result = match cmd {
            JobCmd::Stop => IterationResult::Stop,
            _ => IterationResult::Skipped,
        };
        return iteration_result;
    }

    if j.task.command.is_empty() {
        return finish_iteration(task_ctx, j).await;
    }

    let mut child = match tokio::process::Command::new(&j.task.command)
        .kill_on_drop(true)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "job finished job={} error={:?}",
                &j.task.name,
                Some(e.to_string())
            );
            if let Err(err) = run_tx
                .send(RunRecord {
                    job_name: j.task.name.clone(),
                    run_id: uuid::Uuid::new_v4(),
                    status: TaskStatus::Failed,
                    start_time: start.to_string(),
                    end_time: jiff::Timestamp::now().to_string(),
                })
                .await
            {
                println!("failed to send record: {err}");
            }
            return finish_iteration(task_ctx, j).await;
        }
    };

    let run_ctx = task_ctx.child_token();
    let (concurrent_tx, mut concurrent_rx) = mpsc::channel(slot_capacity(&j.concurrent).max(1));
    for c in &j.concurrent {
        new_instance(
            run_ctx.clone(),
            c.clone(),
            Some(concurrent_tx.clone()),
            None,
        );
    }

    tokio::select! {
        status = child.wait() => {
            run_ctx.cancel();
            let err = match status {
                Ok(s) if s.success() => None,
                Ok(s) => Some(format!("exit status {s}")),
                Err(e) => Some(e.to_string()),
            };
            eprintln!("job finished job={} error={:?}", &j.task.name, err);
            let _ = run_tx.send(RunRecord {
                job_name: j.task.name.clone(),
                run_id: uuid::Uuid::new_v4(),
                status: TaskStatus::Completed,
                start_time: start.to_string(),
                end_time: jiff::Timestamp::now().to_string(),
            }).await;
        }
        cmd = concurrent_rx.recv() => {
            // a concurrent hook fired: the child is killed on drop
            run_ctx.cancel();
            if cmd == Some(JobCmd::Stop) {
                return IterationResult::Stop;
            }
        }
    }

    finish_iteration(task_ctx, j).await
}

async fn finish_iteration(task_ctx: &CancellationToken, j: &RunnableTask) -> IterationResult {
    match run_waiting_slot(task_ctx.clone(), &j.post).await {
        Some(JobCmd::Stop) => IterationResult::Stop,
        _ => IterationResult::Continue,
    }
}

// Fire-and-forget instances have no channel, so whatever they send goes nowhere.
pub async fn report(resp: &CmdTx, cmd: JobCmd) {
    if let Some(resp) = resp {
        let _ = resp.send(cmd).await;
    }
}

fn gate_retry_delay(t: &Task) -> Duration {
    t.gate_retry_delay.unwrap_or(DEFAULT_GATE_RETRY_DELAY)
}

// Waits for non-fire-and-forget instances, reports what they sent; cmdStop always outranks.
async fn run_waiting_slot(ctx: CancellationToken, tasks: &[Arc<RunnableTask>]) -> Option<JobCmd> {
    if tasks.is_empty() {
        return None;
    }

    let (tx, mut rx) = mpsc::channel(slot_capacity(tasks).max(1));
    let mut joinset = JoinSet::new();
    for r in tasks {
        new_instance(ctx.clone(), r.clone(), Some(tx.clone()), Some(&mut joinset));
    }
    drop(tx);

    while joinset.join_next().await.is_some() {}

    let mut fired = None;
    while let Ok(cmd) = rx.try_recv() {
        if cmd == JobCmd::Stop {
            return Some(JobCmd::Stop);
        }
        fired.get_or_insert(cmd);
    }
    fired
}

// Starts an instance of r against resp_tx; never blocks. join is None for concurrent slots.
fn new_instance(
    ctx: CancellationToken,
    r: Arc<RunnableTask>,
    resp_tx: CmdTx,
    join: Option<&mut JoinSet<()>>,
) {
    if r.task.fire_and_forget {
        tokio::spawn(run_composed(ctx, r, None));
        return;
    }
    let fut = run_composed(ctx, r, resp_tx);
    match join {
        Some(js) => {
            js.spawn(fut);
        }
        None => {
            tokio::spawn(fut);
        }
    }
}

// Pre gates the body, concurrent races it, post follows. Concurrent children share resp_tx;
// pre/post children report through their own slot channel, which is relayed up here.
async fn run_composed(
    ctx: CancellationToken,
    r: Arc<RunnableTask>,
    resp_tx: CmdTx,
) {
    if let Some(cmd) = Box::pin(run_waiting_slot(ctx.clone(), &r.pre)).await {
        report(&resp_tx, cmd).await;
        return;
    }

    let body_ctx = ctx.child_token();
    for c in &r.concurrent {
        new_instance(body_ctx.clone(), c.clone(), resp_tx.clone(), None);
    }

    (r.run)(body_ctx.clone(), resp_tx.clone()).await;
    body_ctx.cancel(); // nothing racing the body should outlive the post slot

    if let Some(cmd) = Box::pin(run_waiting_slot(ctx, &r.post)).await {
        report(&resp_tx, cmd).await;
    }
}

// Sizes slot channels: the most commands one instance of r could send, own body plus everything nested.
fn max_sends(r: &RunnableTask) -> usize {
    if r.task.fire_and_forget {
        return 0;
    }
    let mut own = 1usize;
    if r.task.task_type == Some(crate::task::TaskType::Dispatch) {
        if let Some(d) = &r.task.dispatch {
            if d.loops > own as i64 {
                own = d.loops as usize;
            }
        }
    }
    for nested in [&r.pre, &r.concurrent, &r.post] {
        own += slot_capacity(nested);
    }
    own
}

fn slot_capacity(tasks: &[Arc<RunnableTask>]) -> usize {
    tasks.iter().map(|r| max_sends(r)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;

    fn node(task: Task, run: BodyFn) -> RunnableTask {
        RunnableTask {
            task: Arc::new(task),
            run,
            pre: vec![],
            concurrent: vec![],
            post: vec![],
        }
    }

    fn fake(run: BodyFn, fire_and_forget: bool) -> Arc<RunnableTask> {
        Arc::new(node(
            Task {
                name: "fake".into(),
                fire_and_forget,
                ..Default::default()
            },
            run,
        ))
    }

    fn composed(
        body: BodyFn,
        pre: Vec<Arc<RunnableTask>>,
        concurrent: Vec<Arc<RunnableTask>>,
        post: Vec<Arc<RunnableTask>>,
    ) -> Arc<RunnableTask> {
        Arc::new(RunnableTask {
            pre,
            concurrent,
            post,
            ..node(
                Task {
                    name: "composed".into(),
                    ..Default::default()
                },
                body,
            )
        })
    }

    fn sends(cmd: JobCmd) -> BodyFn {
        Box::new(move |_ctx, resp| {
            Box::pin(async move {
                report(&resp, cmd).await;
            })
        })
    }

    fn silent() -> BodyFn {
        Box::new(|_ctx, _resp| Box::pin(async {}))
    }

    fn sleeps(d: StdDuration) -> BodyFn {
        Box::new(move |ctx, _resp| {
            Box::pin(async move {
                tokio::select! {
                    _ = tokio::time::sleep(d) => {}
                    _ = ctx.cancelled() => {}
                }
            })
        })
    }

    fn marks(ran_tx: mpsc::Sender<()>) -> BodyFn {
        Box::new(move |_ctx, _resp| {
            let ran_tx = ran_tx.clone();
            Box::pin(async move {
                let _ = ran_tx.send(()).await;
            })
        })
    }

    fn command_task(name: &str, script: &std::path::Path) -> Task {
        Task {
            name: name.into(),
            command: script.to_string_lossy().into_owned(),
            ..Default::default()
        }
    }

    fn write_script(body: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("gocron-rs-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("script.sh");
        std::fs::write(&path, format!("#!/bin/sh\n{body}")).unwrap();
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
        std::fs::set_permissions(&path, perms).unwrap();
        path
    }

    // --- run_waiting_slot ---

    #[tokio::test]
    async fn run_waiting_slot_empty_slot_passes_immediately() {
        assert_eq!(run_waiting_slot(CancellationToken::new(), &[]).await, None);
    }

    #[tokio::test]
    async fn run_waiting_slot_passes_when_every_instance_is_silent() {
        let tasks = vec![fake(silent(), false), fake(silent(), false)];
        assert_eq!(
            run_waiting_slot(CancellationToken::new(), &tasks).await,
            None
        );
    }

    #[tokio::test]
    async fn run_waiting_slot_reports_a_firing_instance() {
        let tasks = vec![fake(sends(JobCmd::Restart), false)];
        assert_eq!(
            run_waiting_slot(CancellationToken::new(), &tasks).await,
            Some(JobCmd::Restart)
        );
    }

    #[tokio::test]
    async fn run_waiting_slot_blocks_until_every_instance_finishes() {
        let tasks = vec![
            fake(silent(), false),
            fake(sleeps(StdDuration::from_millis(300)), false),
        ];
        let start = std::time::Instant::now();
        run_waiting_slot(CancellationToken::new(), &tasks).await;
        assert!(
            start.elapsed() >= StdDuration::from_millis(250),
            "should wait for the slow instance too"
        );
    }

    #[tokio::test]
    async fn run_waiting_slot_does_not_block_on_fire_and_forget() {
        let ctx = CancellationToken::new();
        let tasks = vec![
            fake(silent(), false),
            fake(sleeps(StdDuration::from_secs(2)), true),
        ];
        let start = std::time::Instant::now();
        run_waiting_slot(ctx.clone(), &tasks).await;
        assert!(
            start.elapsed() < StdDuration::from_millis(200),
            "should skip the fire-and-forget instance"
        );
        ctx.cancel(); // release the still-sleeping instance
    }

    #[tokio::test]
    async fn run_waiting_slot_cmd_stop_outranks_other_commands() {
        let tasks = vec![
            fake(sends(JobCmd::Restart), false),
            fake(sends(JobCmd::Stop), false),
        ];
        assert_eq!(
            run_waiting_slot(CancellationToken::new(), &tasks).await,
            Some(JobCmd::Stop)
        );
    }

    // --- run_composed ---

    #[tokio::test]
    async fn run_composed_runs_body_when_pre_slot_passes() {
        let (tx, mut rx) = mpsc::channel(4);
        let (ran_tx, mut ran_rx) = mpsc::channel(1);
        let r = composed(marks(ran_tx), vec![fake(silent(), false)], vec![], vec![]);
        run_composed(CancellationToken::new(), r, Some(tx)).await;
        assert!(ran_rx.try_recv().is_ok(), "body should have run");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn run_composed_pre_slot_gates_the_body() {
        let (tx, mut rx) = mpsc::channel(4);
        let (ran_tx, mut ran_rx) = mpsc::channel(1);
        let r = composed(
            marks(ran_tx),
            vec![fake(sends(JobCmd::Stop), false)],
            vec![],
            vec![],
        );
        run_composed(CancellationToken::new(), r, Some(tx)).await;
        assert!(
            ran_rx.try_recv().is_err(),
            "body must not run when pre rejects"
        );
        assert_eq!(rx.try_recv().unwrap(), JobCmd::Stop);
    }

    #[tokio::test]
    async fn run_composed_post_slot_reports_upward() {
        let (tx, mut rx) = mpsc::channel(4);
        let r = composed(
            silent(),
            vec![],
            vec![],
            vec![fake(sends(JobCmd::Stop), false)],
        );
        run_composed(CancellationToken::new(), r, Some(tx)).await;
        assert_eq!(rx.try_recv().unwrap(), JobCmd::Stop);
    }

    #[tokio::test]
    async fn run_composed_concurrent_child_shares_the_parents_channel() {
        let (tx, mut rx) = mpsc::channel(4);
        let r = composed(
            sleeps(StdDuration::from_millis(200)),
            vec![],
            vec![fake(sends(JobCmd::Restart), false)],
            vec![],
        );
        run_composed(CancellationToken::new(), r, Some(tx)).await;
        assert_eq!(rx.try_recv().unwrap(), JobCmd::Restart);
    }

    #[tokio::test]
    async fn run_composed_without_a_channel_still_runs_the_body() {
        let (ran_tx, mut ran_rx) = mpsc::channel(1);
        let r = composed(marks(ran_tx), vec![], vec![], vec![fake(sends(JobCmd::Stop), false)]);
        run_composed(CancellationToken::new(), r, None).await;
        assert!(ran_rx.try_recv().is_ok(), "body should run even with nowhere to report");
    }

    // --- fire_and_forget ---

    #[tokio::test]
    async fn fire_and_forget_command_does_not_reach_the_slot() {
        let tasks = vec![fake(sends(JobCmd::Stop), true)];
        assert_eq!(run_waiting_slot(CancellationToken::new(), &tasks).await, None);
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn fire_and_forget_still_runs_its_body() {
        let (ran_tx, mut ran_rx) = mpsc::channel(1);
        let r = Arc::new(node(
            Task {
                name: "faf".into(),
                fire_and_forget: true,
                ..Default::default()
            },
            marks(ran_tx),
        ));
        run_waiting_slot(CancellationToken::new(), &[r]).await;
        let ran = tokio::time::timeout(StdDuration::from_secs(1), ran_rx.recv()).await;
        assert!(matches!(ran, Ok(Some(()))), "fire-and-forget body never ran");
    }

    #[tokio::test]
    async fn fire_and_forget_hides_its_whole_subtree() {
        // the concurrent and post children of a fire-and-forget hook can't reach the slot either
        let faf = Arc::new(RunnableTask {
            concurrent: vec![fake(sends(JobCmd::Stop), false)],
            post: vec![fake(sends(JobCmd::Stop), false)],
            ..node(
                Task {
                    name: "faf".into(),
                    fire_and_forget: true,
                    ..Default::default()
                },
                sleeps(StdDuration::from_millis(50)),
            )
        });
        let tasks = vec![faf, fake(sleeps(StdDuration::from_millis(150)), false)];
        assert_eq!(run_waiting_slot(CancellationToken::new(), &tasks).await, None);
    }

    #[tokio::test]
    async fn run_task_fire_and_forget_concurrent_hook_does_not_kill_the_command() {
        let script = write_script("sleep 0.3\n");
        let j = Arc::new(RunnableTask {
            concurrent: vec![fake(sends(JobCmd::Stop), true)],
            post: vec![fake(sends(JobCmd::Stop), false)],
            ..node(command_task("unbothered", &script), silent())
        });
        let (run_tx, mut run_rx) = mpsc::channel(4);
        let result = tokio::time::timeout(StdDuration::from_secs(3), run_task(j, run_tx)).await;
        assert!(result.is_ok());
        assert!(
            run_rx.try_recv().is_ok(),
            "command should have run to completion and been recorded"
        );
    }

    #[tokio::test]
    async fn nested_task_reaches_the_root_through_its_parent() {
        // A task nested two levels down stops the root loop; nested commands aren't swallowed on the way up.
        let inner = fake(sends(JobCmd::Stop), false);
        let middle = composed(silent(), vec![], vec![], vec![inner]);
        let outer = composed(silent(), vec![], vec![], vec![middle]);

        let script = write_script("exit 0\n");
        let j = Arc::new(RunnableTask {
            post: vec![outer],
            ..node(command_task("nested", &script), silent())
        });

        let (run_tx, _run_rx) = mpsc::channel(4);
        let result = tokio::time::timeout(StdDuration::from_secs(3), run_task(j, run_tx)).await;
        assert!(
            result.is_ok(),
            "run_task did not return after a nested cmdStop"
        );
    }

    // --- depth counters ---

    fn dispatch_task(loops: i64) -> Task {
        Task {
            task_type: Some(crate::task::TaskType::Dispatch),
            dispatch: Some(crate::task::DispatchConfig {
                command: JobCmd::Restart,
                loops,
                unix_exec_path: String::new(),
                unix_read_path: String::new(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn max_sends_counts_dispatch_loops() {
        assert_eq!(max_sends(&node(dispatch_task(5), silent())), 5);
        assert_eq!(max_sends(&node(dispatch_task(0), silent())), 1);
    }

    #[test]
    fn max_sends_ignores_fire_and_forget() {
        assert_eq!(max_sends(&fake(silent(), true)), 0);
    }

    #[test]
    fn slot_capacity_counts_the_whole_nested_tree() {
        let leaf = fake(silent(), false);
        let mid = composed(silent(), vec![leaf.clone()], vec![leaf], vec![]); // 1 + 1 + 1 = 3
        let top = composed(silent(), vec![], vec![], vec![mid]); // 1 + 3 = 4
        assert_eq!(slot_capacity(&[top]), 4);
    }

    // --- run_task (root loop) ---

    #[tokio::test]
    async fn run_task_skips_command_while_pre_hook_rejects() {
        let script = write_script("touch ran\n");
        let marker = script.parent().unwrap().join("ran");
        let task = Task {
            gate_retry_delay: Some(StdDuration::from_millis(10)),
            ..command_task("guarded", &script)
        };
        let j = Arc::new(RunnableTask {
            pre: vec![fake(sends(JobCmd::Restart), false)],
            ..node(task, silent())
        });

        let (run_tx, _run_rx) = mpsc::channel(4);
        let result = tokio::time::timeout(StdDuration::from_millis(150), run_task(j, run_tx)).await;
        assert!(result.is_err(), "run_task should still be retrying");
        assert!(
            !marker.exists(),
            "command ran despite the pre slot rejecting every iteration"
        );
    }

    #[tokio::test]
    async fn run_task_runs_command_when_pre_hook_passes() {
        let script = write_script("exit 0\n");
        let j = Arc::new(RunnableTask {
            pre: vec![fake(silent(), false)],
            post: vec![fake(sends(JobCmd::Stop), false)],
            ..node(command_task("gated-open", &script), silent())
        });
        let (run_tx, mut run_rx) = mpsc::channel(4);
        let result = tokio::time::timeout(StdDuration::from_secs(2), run_task(j, run_tx)).await;
        assert!(
            result.is_ok(),
            "run_task did not return after the post slot sent cmdStop"
        );
        assert!(
            run_rx.try_recv().is_ok(),
            "expected a run record once the pre slot passed"
        );
    }

    #[tokio::test]
    async fn run_task_stops_when_pre_hook_sends_cmd_stop() {
        let script = write_script("touch ran\n");
        let marker = script.parent().unwrap().join("ran");
        let j = Arc::new(RunnableTask {
            pre: vec![fake(sends(JobCmd::Stop), false)],
            ..node(command_task("halted", &script), silent())
        });
        let (run_tx, _run_rx) = mpsc::channel(4);
        let result = tokio::time::timeout(StdDuration::from_secs(2), run_task(j, run_tx)).await;
        assert!(
            result.is_ok(),
            "run_task did not return after the pre slot sent cmdStop"
        );
        assert!(
            !marker.exists(),
            "command ran even though the pre slot stopped the task"
        );
    }

    #[tokio::test]
    async fn run_task_concurrent_hook_cancels_the_command() {
        let script = write_script("sleep 10\n");
        let j = Arc::new(RunnableTask {
            concurrent: vec![fake(sends(JobCmd::Stop), false)],
            ..node(command_task("interrupted", &script), silent())
        });
        let (run_tx, _run_rx) = mpsc::channel(4);
        let result = tokio::time::timeout(StdDuration::from_secs(3), run_task(j, run_tx)).await;
        assert!(
            result.is_ok(),
            "run_task did not return after a concurrent hook sent cmdStop mid-command"
        );
    }
}
