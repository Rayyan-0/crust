use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Result};
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::dispatch;
use crate::runtime::{report, BodyFn, CmdTx};
use crate::task::{JobCmd, Task, TaskType};

// TODO: use inotify or some other standerdized rather than polling. Consider platform-specific builds
const FILEWATCH_POLL_INTERVAL: Duration = Duration::from_secs(1);

// Builds the function that runs a task's body: type wins over command; a task with neither is rejected.
pub fn build_body(t: &Task, args: &[Value]) -> Result<BodyFn> {
    match t.task_type {
        Some(TaskType::Interval) | Some(TaskType::Timeout) => {
            let d = args
                .iter()
                .find_map(|arg| match arg {
                    Value::String(s) => humantime::parse_duration(s).ok(),
                    _ => None,
                })
                .ok_or_else(|| anyhow!("no valid duration in data_path"))?;
            let send = t.send_command();
            Ok(Box::new(move |ctx, resp| {
                Box::pin(run_timer(ctx, d, send, resp))
            }))
        }
        Some(TaskType::Healthcheck) => {
            let command = single_string(args)?;
            let send = t.send_command();
            Ok(Box::new(move |ctx, resp| {
                Box::pin(run_healthcheck(ctx, command.clone(), send, resp))
            }))
        }
        Some(TaskType::Filewatch) => {
            let path = single_string(args)?;
            let send = t.send_command();
            Ok(Box::new(move |ctx, resp| {
                Box::pin(run_filewatch(
                    ctx,
                    path.clone(),
                    FILEWATCH_POLL_INTERVAL,
                    send,
                    resp,
                ))
            }))
        }
        Some(TaskType::Dispatch) => {
            let cfg = dispatch::new_dispatch_body(t, args)?;
            Ok(Box::new(move |ctx, resp| {
                Box::pin(dispatch::run_dispatch(ctx, cfg.clone(), resp))
            }))
        }
        None if !t.command.is_empty() => {
            let command = t.command.clone();
            Ok(Box::new(move |_ctx, _resp| {
                let command = command.clone();
                Box::pin(async move {
                    let _ = tokio::process::Command::new(&command)
                        .kill_on_drop(true)
                        .status()
                        .await;
                })
            }))
        }
        None => bail!("invalid task type that can't be inferred: {}", t.name),
    }
}

// One value only; two paths on these types is a config mistake.
fn single_string(args: &[Value]) -> Result<String> {
    if args.len() != 1 {
        bail!("expected exactly one data_path, got {}", args.len());
    }
    match &args[0] {
        Value::String(s) => Ok(s.clone()),
        other => bail!("expected a string, got {other:?}"),
    }
}

// Sends after d, unless ctx ends first.
async fn run_timer(ctx: CancellationToken, d: Duration, send: JobCmd, resp: CmdTx) {
    tokio::select! {
        _ = tokio::time::sleep(d) => { report(&resp, send).await; }
        _ = ctx.cancelled() => {}
    }
}

// Sends only on failure (non-zero exit or exec error); silent on success.
async fn run_healthcheck(
    ctx: CancellationToken,
    command: String,
    send: JobCmd,
    resp: CmdTx,
) {
    let run = tokio::process::Command::new(&command)
        .kill_on_drop(true)
        .status();
    tokio::select! {
        status = run => {
            if !matches!(status, Ok(s) if s.success()) {
                report(&resp, send).await;
            }
        }
        _ = ctx.cancelled() => {}
    }
}

// Polls path's mtime; a missing file isn't an error, just "not modified yet".
async fn run_filewatch(
    ctx: CancellationToken,
    path: String,
    poll: Duration,
    send: JobCmd,
    resp: CmdTx,
) {
    let mut last_mod = mtime(&path).await;
    let mut ticker = tokio::time::interval(poll);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Some(m) = mtime(&path).await {
                    if last_mod.map(|l| m > l).unwrap_or(true) {
                        report(&resp, send).await;
                        return;
                    }
                    last_mod = Some(m);
                }
            }
            _ = ctx.cancelled() => return,
        }
    }
}

async fn mtime(path: &str) -> Option<SystemTime> {
    tokio::fs::metadata(path).await.ok()?.modified().ok()
}
