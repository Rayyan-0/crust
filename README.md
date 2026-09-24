# crust

A task runner daemon written in Rust. It started as a port of gocron and is
now maintained as an alternative implementation. It reads the same
`config/tasks.yaml` schema.

## Running

Run from the directory that contains `config/`, since paths are resolved
relative to the current directory:

```sh
cargo run
```

Tests:

```sh
cargo test
```

The process exits once every `run_on_boot` task has stopped.

## Config

`config/tasks.yaml` is a list of tasks. A task with `run_on_boot: true` is a
root and loops until it receives a stop command. Any other task only runs when
a root (or another task) references it from a hook slot.

```yaml
- name: server
  run_on_boot: true
  command: ./start-server.sh
  data:
    limits:
      runtime: "1h"
  concurrent_hooks:
    labels: [restart-hourly]

- name: restart-hourly
  type: timeout
  data_path: [[limits, runtime]]
  send_command: restart
```

Task fields:

| Field | Meaning |
|---|---|
| `name` | Unique name, used to reference the task from hook slots |
| `run_on_boot` | Makes the task a root |
| `command` | Executable to run (no arguments) |
| `data` | Values that referenced tasks can read via `data_path` |
| `gate_retry_delay` | Wait before retrying when a pre hook rejects, e.g. `"30s"` (default 30s) |
| `pre_hooks` | Run before the command; any command sent skips (or stops) the run |
| `concurrent_hooks` | Race the command; a command sent kills it |
| `post_hooks` | Run after the command |
| `type` | Hook type: `interval`, `timeout`, `healthcheck`, `filewatch`, `dispatch` |
| `data_path` | Paths into the root's `data` that supply the hook's arguments |
| `send_command` | Command the hook sends: `stop` or `restart` (default `stop`) |
| `fire_and_forget` | Don't wait for this hook to finish |
| `dispatch` | Unix socket settings for `dispatch` hooks |

Hooks can have their own hooks, and a command sent at any depth is passed up to
the root. Each finished run is recorded under `config/runs/<task>/<run-id>.yaml`.

## Layout

```
src/
  main.rs       loads config and runs the roots
  task.rs       task types and yaml loading
  startup.rs    resolves hook references into a task tree
  runtime.rs    the scheduler
  bodies.rs     built-in hook types
  dispatch.rs   dispatch hook (unix socket bridge)
  datapath.rs   data_path resolution
  runs.rs       run record writer
```
