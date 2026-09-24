use anyhow::{anyhow, Context, Result};
use serde_yaml::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio_util::sync::CancellationToken;

use crate::runtime::{report, CmdTx};
use crate::task::{JobCmd, Task};

// The dispatch-only half of a Task, with its payload already encoded.
#[derive(Clone)]
pub struct DispatchBody {
    send_command: JobCmd,
    loops: i64,
    unix_exec_path: String,
    unix_read_path: String,
    args: Vec<u8>,
}

pub fn new_dispatch_body(t: &Task, args: &[Value]) -> Result<DispatchBody> {
    let d = t
        .dispatch
        .as_ref()
        .ok_or_else(|| anyhow!("task {}: type \"dispatch\" needs a dispatch block", t.name))?;

    let encoded = serde_json::to_vec(args).context("failed to marshal dispatch args")?;

    Ok(DispatchBody {
        send_command: d.command,
        loops: d.loops,
        unix_exec_path: d.unix_exec_path.clone(),
        unix_read_path: d.unix_read_path.clone(),
        args: encoded,
    })
}

// Bridges to an external process over a pair of unix sockets; loops <= 0 means run once.
pub async fn run_dispatch(ctx: CancellationToken, cfg: DispatchBody, resp: CmdTx) {
    let loops = if cfg.loops <= 0 { 1 } else { cfg.loops };
    for _ in 0..loops {
        if dispatch_once(&ctx, &cfg, &resp).await.is_err() {
            return;
        }
    }
}

async fn dispatch_once(ctx: &CancellationToken, cfg: &DispatchBody, resp: &CmdTx) -> Result<()> {
    let mut read_conn = UnixStream::connect(&cfg.unix_read_path).await?;
    let mut exec_conn = UnixStream::connect(&cfg.unix_exec_path).await?;
    exec_conn.write_all(&cfg.args).await?;

    let mut buf = [0u8; 1];
    tokio::select! {
        _ = read_conn.read(&mut buf) => {
            report(resp, cfg.send_command).await;
            Ok(())
        }
        _ = ctx.cancelled() => Err(anyhow!("dispatch cancelled")),
    }
}
