use serde::Serialize;
use tokio::sync::mpsc;
use uuid::Uuid;

pub const RUNS_DIR: &str = "config/runs";

#[derive(Serialize)]
pub enum TaskStatus {
    Failed,
    Completed,
}

#[derive(Serialize)]
pub struct RunRecord {
    pub start_time: String,
    pub end_time: String,
    pub status: TaskStatus,
    pub job_name: String,
    pub run_id: Uuid,
}

pub async fn start_run_writer(mut rx: mpsc::Receiver<RunRecord>) {
    while let Some(record) = rx.recv().await {
        let dir = std::path::Path::new(RUNS_DIR).join(&record.job_name);
        if let Err(e) = tokio::fs::create_dir_all(&dir).await {
            eprintln!("failed to create run dir for {}: {e}", record.job_name);
            continue;
        }
        match serde_yaml::to_string(&record) {
            Ok(body) => {
                let path = dir.join(format!("{}.yaml", record.run_id));
                if let Err(e) = tokio::fs::write(&path, body).await {
                    eprintln!("failed to write run record for {}: {e}", record.job_name);
                }
            }
            Err(e) => eprintln!("failed to marshal run record for {}: {e}", record.job_name),
        }
    }
}
