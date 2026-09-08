use anyhow::{Result, anyhow};
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ffi::OsString, process::Command, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{
        RwLock,
        mpsc::{self, UnboundedSender},
    },
};
use uuid::Uuid;

#[derive(Hash, PartialEq, Eq, Clone, Copy)]
enum CrustAction {
    Run,
    Shutdown,
    ShutdownForced,
}
enum JobOutcome {
    Success,
    Failure(anyhow::Error),
    FailureHandled(anyhow::Error),
}
#[derive(Debug)]
enum Cmd {
    Edit {
        new_name: String,
        new_command: String,
        new_interval: Duration,
    },
    Delete(Uuid),
}

struct JobRun {
    id: Uuid,
    job_id: Uuid,
    job_name: String,
    outcome: JobOutcome,
    datetime: chrono::DateTime<chrono::Utc>,
}

struct Job {
    id: Uuid,
    name: String, // entry point to the app
    cmd: mpsc::UnboundedReceiver<Cmd>,
    interval_timer: tokio::time::Interval, // TODO: do we have a time type?
    action_interface: HashMap<CrustAction, OsString>, // string has to be the run executable
    error_interface: HashMap<Error, ErrorHandler>, // TODO: fix client having to swallow specific details when matching handlers
}

type Error = String;
#[derive(Clone)]
struct ErrorInterface(HashMap<Error, ErrorHandler>);
#[derive(Clone)]
struct ErrorHandler {
    name: String,
    receiver: fn(&str) -> HandlerResult<'_>,
    handlers: ErrorInterface, // allows more control over handler error thus more safety
}

enum HandlerResult<'a> {
    Ok,
    Error(anyhow::Error),
    Handler(ErrorHandler, &'a str),
}

impl ErrorHandler {
    fn handle(self, error: &str) -> HandlerResult<'_> {
        // avoid recursion by consuming. Refresh once we're done handling
        match (self.receiver)(error) {
            HandlerResult::Ok => HandlerResult::Ok,
            HandlerResult::Error(handle_err) => {
                let name = self.name.clone();
                HandlerResult::Error(anyhow!(
                    "handler {name} failed to handle error {error} with handle error: {handle_err}"
                ))
            }
            HandlerResult::Handler(handler, error_to_handle) => {
                HandlerResult::Handler(handler, error_to_handle)
            }
        }
    }
}

impl Job {
    async fn run(&self) -> Result<()> {
        let run_command = self.action_interface.get(&CrustAction::Run).cloned();
        if let Some(command) = run_command {
            Command::new(command).arg("").spawn()?;
        }
        Ok(())
    }
    async fn handle_error(&self, error: String) -> Result<()> {
        match self
            .error_interface
            .get(error.as_str())
            .cloned()
            .unwrap()
            .handle(error.as_str())
        {
            Ok(_) => Ok(()),
            // if it returns a top level error -> all lower level handlers failed
            // d and thus we kill the task
            Err((error, _)) => {
                panic!("error error error!!!!: original:{error} final propagated error: {error}")
            }
        }
    }
}

type DialList = HashMap<Uuid, UnboundedSender<Cmd>>;
type Runs = Arc<RwLock<Vec<JobRun>>>;

#[derive(Clone)]
struct AppState {
    job_dial: Arc<RwLock<DialList>>,
    runs: Arc<RwLock<Vec<JobRun>>>,
}
#[tokio::main]
async fn main() {
    let registered_jobs: HashMap<Uuid, Job> = HashMap::new();
    let mut cmd_channels: DialList = HashMap::new();
    let runs: Runs = Arc::new(RwLock::new(vec![]));

    for (_, mut job) in registered_jobs {
        let (tx, rx) = mpsc::unbounded_channel::<Cmd>();
        cmd_channels.insert(job.id.clone(), tx);
        job.cmd = rx;
        let runs = Arc::clone(&runs);
        tokio::spawn(async move {
            new_job_handler(job, runs).await;
        });
    }
    let cmd_channels: Arc<RwLock<DialList>> = Arc::new(RwLock::new(cmd_channels));

    // for adding new jobs while the server is up
    // TODO: consider an endpoints for one shot actions
    let app = Router::new()
        .route("/", post(handle_new))
        .route("/edit", post(handle_edit))
        .with_state(AppState {
            job_dial: cmd_channels,
            runs: Arc::clone(&runs), // TODO: consider a single inner field with one Arc
        });
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Deserialize, Serialize)]
struct AddJobRequest {
    name: String,
    command: String,
    interval: Duration, // TODO: figure out error handler integration
}
#[axum::debug_handler]
async fn handle_new(State(state): State<AppState>, Json(req): Json<AddJobRequest>) -> StatusCode {
    let uuid = Uuid::new_v4();
    let (tx, rx) = mpsc::unbounded_channel::<Cmd>();
    let job = Job {
        id: uuid,
        name: req.name,
        cmd: rx,
        interval_timer: tokio::time::interval(req.interval),
        error_interface: HashMap::new(),
        action_interface: HashMap::new(),
    };
    tokio::spawn(new_job_handler(job, Arc::clone(&state.runs)));
    state.job_dial.write().await.insert(uuid, tx);

    StatusCode::OK
}

#[derive(Deserialize)]
struct EditJobRequest {
    id: Uuid,
    new_name: String,
    new_command: String,
    new_interval: Duration,
}
#[axum::debug_handler]
async fn handle_edit(State(state): State<AppState>, Json(req): Json<EditJobRequest>) -> StatusCode {
    let dial = state.job_dial.read().await;
    let Some(job) = dial.get(&req.id).cloned() else {
        return StatusCode::NOT_FOUND;
    };
    match job.send(Cmd::Edit {
        new_name: req.new_name,
        new_command: req.new_command,
        new_interval: req.new_interval,
    }) {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn new_job_handler(mut job: Job, runs: Arc<RwLock<Vec<JobRun>>>) -> StatusCode {
    loop {
        select! {
            _ = job.interval_timer.tick() => {
                let outcome = match job.run().await {
                    Ok(()) => JobOutcome::Success,
                    Err(err) => match job.handle_error(err.to_string()).await {
                        Ok(_) => JobOutcome::FailureHandled(err),
                        Err(handler_err) => JobOutcome::Failure(handler_err),
                    },
                };

                runs.write().await.push(JobRun {
                    id: Uuid::new_v4(),
                    job_id: job.id,
                    job_name: job.name.clone(),
                    outcome,
                    datetime: Utc::now(),
                });
            }
            Some(cmd) = job.cmd.recv() => {
                println!("hehe got you {:?}", cmd);
                todo!("unget him please")
            }
        }
    }
}
