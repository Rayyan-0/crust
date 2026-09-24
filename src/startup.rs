use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde_yaml::Value;

use crate::bodies::build_body;
use crate::datapath::resolve;
use crate::runtime::RunnableTask;
use crate::task::Task;

type Pool = HashMap<String, Arc<Task>>;

pub fn prepare_tasks(tasks: Vec<Task>) -> Result<Vec<Arc<RunnableTask>>> {
    let pool: Pool = tasks
        .into_iter()
        .map(|t| (t.name.clone(), Arc::new(t)))
        .collect();
    let mut roots = Vec::new();
    for t in pool.values().filter(|t| t.run_on_boot) {
        let chain = Vec::new();
        let pre = build_slot(&t.pre_hooks, &t.data, &pool, &chain)
            .with_context(|| format!("task {:?}", t.name))?;
        let concurrent = build_slot(&t.concurrent_hooks, &t.data, &pool, &chain)
            .with_context(|| format!("task {:?}", t.name))?;
        let post = build_slot(&t.post_hooks, &t.data, &pool, &chain)
            .with_context(|| format!("task {:?}", t.name))?;

        roots.push(Arc::new(RunnableTask {
            task: t.clone(),
            run: Box::new(|_ctx, _resp| Box::pin(async {})),
            pre,
            concurrent,
            post,
        }));
    }
    Ok(roots)
}

fn build_slot(
    slot: &[String],
    data: &Value,
    pool: &Pool,
    chain: &[String],
) -> Result<Vec<Arc<RunnableTask>>> {
    let mut built = Vec::with_capacity(slot.len());
    for name in slot {
        let t = pool
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("no task named {name:?}"))?;
        built.push(Arc::new(build_runnable(t, data, pool, chain)?));
    }
    Ok(built)
}

// chain tracks in-progress names, so a task referencing itself is reported, not infinitely recursed.
fn build_runnable(
    t: &Arc<Task>,
    data: &Value,
    pool: &Pool,
    chain: &[String],
) -> Result<RunnableTask> {
    if chain.contains(&t.name) {
        let mut path = chain.to_vec();
        path.push(t.name.clone());
        anyhow::bail!("task cycle: {}", path.join(" -> "));
    }
    let mut chain = chain.to_vec();
    chain.push(t.name.clone());

    let args = resolve(&t.data_path, data).with_context(|| format!("task {:?}", t.name))?;
    let run = build_body(t, args.as_slice()).with_context(|| format!("task {:?}", t.name))?;

    let pre = build_slot(&t.pre_hooks, data, pool, &chain)?;
    let concurrent = build_slot(&t.concurrent_hooks, data, pool, &chain)?;
    let post = build_slot(&t.post_hooks, data, pool, &chain)?;

    Ok(RunnableTask {
        task: t.clone(),
        run,
        pre,
        concurrent,
        post,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepare(yaml: &str) -> Result<Vec<Arc<RunnableTask>>> {
        prepare_tasks(serde_yaml::from_str(yaml).unwrap())
    }

    fn cycle_error(yaml: &str) -> String {
        match prepare(yaml) {
            Ok(_) => panic!("expected startup to fail"),
            Err(e) => format!("{e:#}"),
        }
    }

    #[test]
    fn root_referencing_itself_is_a_cycle() {
        let err = cycle_error(
            r#"
- name: r
  run_on_boot: true
  command: "true"
  pre_hooks: [r]
"#,
        );
        assert!(err.contains("task cycle: r -> r"), "{err}");
    }

    #[test]
    fn hook_pointing_back_at_its_root_is_a_cycle() {
        let err = cycle_error(
            r#"
- name: r
  run_on_boot: true
  command: "true"
  post_hooks: [h]
- name: h
  command: "true"
  post_hooks: [r]
"#,
        );
        assert!(err.contains("task cycle: h -> r -> h"), "{err}");
    }

    #[test]
    fn cycle_between_hooks_is_reported() {
        let err = cycle_error(
            r#"
- name: r
  run_on_boot: true
  command: "true"
  pre_hooks: [a]
- name: a
  command: "true"
  post_hooks: [b]
- name: b
  command: "true"
  concurrent_hooks: [a]
"#,
        );
        assert!(err.contains("task cycle: a -> b -> a"), "{err}");
    }

    #[test]
    fn shared_hook_is_not_a_cycle() {
        let roots = prepare(
            r#"
- name: r
  run_on_boot: true
  command: "true"
  pre_hooks: [a, b]
- name: a
  command: "true"
  post_hooks: [shared]
- name: b
  command: "true"
  post_hooks: [shared]
- name: shared
  command: "true"
"#,
        )
        .unwrap();
        let pre = &roots[0].pre;
        assert_eq!(pre.len(), 2);
        assert!(pre.iter().all(|h| h.post[0].task.name == "shared"));
    }

    #[test]
    fn missing_hook_name_is_reported() {
        let err = cycle_error(
            r#"
- name: r
  run_on_boot: true
  command: "true"
  concurrent_hooks: [ghost]
"#,
        );
        assert!(err.contains(r#"no task named "ghost""#), "{err}");
    }

    #[test]
    fn hooks_can_be_defined_after_their_root() {
        let roots = prepare(
            r#"
- name: r
  run_on_boot: true
  command: "true"
  post_hooks: [later]
- name: later
  command: "true"
"#,
        )
        .unwrap();
        assert_eq!(roots[0].post[0].task.name, "later");
    }
}
