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

    #[serde(deserialize_with = "duration::opt")]
    pub gate_retry_delay: Option<Duration>,

    pub pre_hooks: Vec<String>,
    pub concurrent_hooks: Vec<String>,
    pub post_hooks: Vec<String>,

    // below: only used when this task is referenced from a slot
    #[serde(rename = "type")]
    pub task_type: Option<TaskType>,
    pub data_path: DataPath,
    pub send_command: Option<JobCmd>,
    pub fire_and_forget: bool,
    pub dispatch: Option<DispatchConfig>,
}

#[derive(Debug, Deserialize)]
pub struct DispatchConfig {
    #[serde(rename = "dispatcher_command")]
    pub command: JobCmd,
    #[serde(default)]
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

// yaml gives duration strings ("30s", "1h30m"), not a native Duration.
mod duration {
    use serde::{Deserialize, Deserializer};
    use std::time::Duration;

    pub fn opt<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        Option::<String>::deserialize(d)?
            .map(|s| humantime::parse_duration(&s).map_err(serde::de::Error::custom))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hook_fields() {
        let tasks: Vec<Task> = serde_yaml::from_str(
            r#"
- name: gate
  type: dispatch
  gate_retry_delay: "1m30s"
  dispatch:
    dispatcher_command: restart
    unix_exec_path: /tmp/exec.sock
    unix_read_path: /tmp/read.sock
"#,
        )
        .unwrap();
        let t = &tasks[0];
        assert_eq!(t.task_type, Some(TaskType::Dispatch));
        assert_eq!(t.gate_retry_delay, Some(Duration::from_secs(90)));
        let d = t.dispatch.as_ref().unwrap();
        assert_eq!(d.command, JobCmd::Restart);
        assert_eq!(d.loops, 0);
    }

    #[test]
    fn missing_gate_retry_delay_is_none() {
        let tasks: Vec<Task> = serde_yaml::from_str("- name: plain\n").unwrap();
        assert_eq!(tasks[0].gate_retry_delay, None);
    }
}
