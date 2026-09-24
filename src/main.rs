mod bodies;
mod datapath;
mod dispatch;
mod runs;
mod runtime;
mod startup;
mod task;
use std::path::Path;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tasks_dir = Path::new(task::TASKS_FILE)
        .parent()
        .unwrap_or(Path::new("."));
    for dir in [tasks_dir, Path::new(runs::RUNS_DIR)] {
        if !dir.exists() {
            std::fs::create_dir_all(dir).unwrap();
        }
    }

    let tasks = task::load_tasks()?;
    let roots = startup::prepare_tasks(tasks)?;

    let (run_tx, run_rx) = mpsc::channel(16);
    tokio::spawn(runs::start_run_writer(run_rx));

    let mut running = tokio::task::JoinSet::new();
    for root in roots {
        running.spawn(runtime::run_task(root, run_tx.clone()));
    }

    tokio::select! {
        _ = async { while running.join_next().await.is_some() {} } => {}
        _ = tokio::signal::ctrl_c() => {}
    };
    Ok(())
}
