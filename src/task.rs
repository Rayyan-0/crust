use anyhow::Context;
use serde::Deserialize;
use serde_yaml::Value;
use std::time::Duration;

pub const TASKS_FILE: &str = "config/tasks.yaml";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskType {
    Interval,
    Timeout,
    Healthcheck,
    Filewatch,
    Dispatch,
}

#[derive(Debug, Clone, Deserialize, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobCmd {
    Stop,
    Restart,
}

pub type DataPath = Vec<Vec<Value>>;

// Every task is the same thing; run_on_boot is the only flag that makes one a root.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Task {
    pub name: String,
    pub run_on_boot: bool,
    pub command: String,
    pub data: Value,

    // TODO: find a way to deserializet this
    pub gate_retry_delay: Option<Duration>,

    pub pre_hooks: Vec<String>,
    pub concurrent_hooks: Vec<String>,
    pub post_hooks: Vec<String>,

    // below: only used when this task is referenced from a slot
    pub task_type: Option<TaskType>,
    pub data_path: DataPath,
    pub send_command: Option<JobCmd>,
    pub fire_and_forget: bool,
    pub dispatch: Option<DispatchConfig>,
}

#[derive(Debug, Deserialize)]
pub struct DispatchConfig {
    pub command: JobCmd,
    pub loops: i64,
    pub unix_exec_path: String,
    pub unix_read_path: String,
}

impl Task {
    pub fn send_command(&self) -> JobCmd {
        self.send_command.unwrap_or(JobCmd::Stop)
    }
}

pub fn load_tasks() -> anyhow::Result<Vec<Task>> {
    let bytes =
        std::fs::read(TASKS_FILE).with_context(|| format!("failed to read {TASKS_FILE}"))?;
    serde_yaml::from_slice(&bytes).with_context(|| format!("failed to unmarshal {TASKS_FILE}"))
}
