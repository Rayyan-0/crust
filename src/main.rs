mod bodies;
mod datapath;
mod dispatch;
mod runs;
mod runtime;
mod startup;
mod task;
use anyhow::{bail, Ok};
use clap::Parser;
use std::{panic, path::Path};
use tokio::sync::mpsc;

#[derive(clap::Parser)]
struct Args {
    #[arg(long)]
    validate: bool,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Path::new(task::TASKS_FILE).exists() {
        true => {}
        false => {
            bail!("tasks.yaml not found. Check README for the format")
        }
    };

    let args = Args::parse();
    let tasks = task::load_tasks()?;
    let roots = startup::prepare_tasks(tasks)?;
    if args.validate {
        println!("config ok: {} root task(s)", roots.len());
        return Ok(());
    }
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
