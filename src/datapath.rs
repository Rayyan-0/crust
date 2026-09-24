use anyhow::{bail, Result};
use serde_yaml::Value;

use crate::task::DataPath;

// One walk per path; string keys index a map, int keys index a sequence, null asserts null.
pub fn resolve(paths: &DataPath, data: &Value) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(paths.len());
    for path in paths {
        let mut cur = data.clone();
        for key in path {
            cur = step(&cur, key)?;
        }
        out.push(cur);
    }
    Ok(out)
}

fn step(cur: &Value, key: &Value) -> Result<Value> {
    match key {
        Value::String(k) => {
            let map = cur
                .as_mapping()
                .ok_or_else(|| anyhow::anyhow!("key {k:?} expects a map, got {}", kind(cur)))?;
            map.get(Value::String(k.clone()))
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("key {k:?} not found"))
        }
        Value::Number(n) if n.is_i64() || n.is_u64() => {
            let idx = n.as_i64().unwrap();
            let seq = cur
                .as_sequence()
                .ok_or_else(|| anyhow::anyhow!("index {idx} expects an array, got {}", kind(cur)))?;
            if idx < 0 || idx as usize >= seq.len() {
                bail!("index {idx} out of range (len {})", seq.len());
            }
            Ok(seq[idx as usize].clone())
        }
        Value::Null => {
            if !cur.is_null() {
                bail!("expected null, got {}", kind(cur));
            }
            Ok(Value::Null)
        }
        other => bail!("unsupported key type {other:?}"),
    }
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Sequence(_) => "sequence",
        Value::Mapping(_) => "mapping",
        Value::Tagged(_) => "tagged",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yaml(s: &str) -> Value {
        serde_yaml::from_str(s).unwrap()
    }

    #[test]
    fn resolves_a_single_path() {
        let data = yaml("interval: 5s\n");
        let paths: DataPath = vec![vec![Value::String("interval".into())]];
        let got = resolve(&paths, &data).unwrap();
        assert_eq!(got, vec![Value::String("5s".into())]);
    }

    #[test]
    fn resolves_nested_map_then_sequence() {
        let data = yaml("nested:\n  retries:\n    - delay: 1s\n    - delay: 2s\n");
        let paths: DataPath = vec![vec![
            Value::String("nested".into()),
            Value::String("retries".into()),
            Value::Number(1.into()),
            Value::String("delay".into()),
        ]];
        let got = resolve(&paths, &data).unwrap();
        assert_eq!(got, vec![Value::String("2s".into())]);
    }

    #[test]
    fn missing_key_is_an_error() {
        let data = yaml("a: 1\n");
        let paths: DataPath = vec![vec![Value::String("b".into())]];
        assert!(resolve(&paths, &data).is_err());
    }

    #[test]
    fn multiple_paths_resolve_independently() {
        let data = yaml("a: 1\nb: 2\n");
        let paths: DataPath = vec![
            vec![Value::String("b".into())],
            vec![Value::String("a".into())],
        ];
        let got = resolve(&paths, &data).unwrap();
        assert_eq!(got, vec![Value::Number(2.into()), Value::Number(1.into())]);
    }
}
