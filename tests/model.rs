#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Model-based test: random operations against RustLog and a `BTreeMap`.
//! After every read, both must agree. Reopen and flush are operations too,
//! so recovery and compaction get exercised at random points.

mod common;

use std::collections::BTreeMap;

use bytes::Bytes;
use proptest::prelude::*;
use rustlog::Db;

#[derive(Debug, Clone)]
enum Op {
    Put(u16, u16),
    Delete(u16),
    Get(u16),
    Scan(u16, u16),
    Flush,
    Reopen,
    Compact,
}

fn op() -> impl Strategy<Value = Op> {
    // Small key space: lots of overwrites and deletes of the same keys.
    let k = 0u16..300;
    prop_oneof![
        40 => (k.clone(), any::<u16>()).prop_map(|(k, v)| Op::Put(k, v)),
        10 => k.clone().prop_map(Op::Delete),
        25 => k.clone().prop_map(Op::Get),
        5 => (k.clone(), k).prop_map(|(a, b)| Op::Scan(a.min(b), a.max(b))),
        2 => Just(Op::Flush),
        1 => Just(Op::Reopen),
        1 => Just(Op::Compact),
    ]
}

fn key(k: u16) -> Vec<u8> {
    format!("key{k:05}").into_bytes()
}

fn val(k: u16, v: u16) -> Vec<u8> {
    // Long-ish values so memtables fill and compactions run.
    format!("{k}:{v}:{}", "p".repeat(40)).into_bytes()
}

fn run(ops: Vec<Op>) {
    let rt = common::rt();
    let dir = tempfile::tempdir().unwrap();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    rt.block_on(async {
        let mut db = Db::open(dir.path(), common::small_opts()).await.unwrap();
        for (step, op) in ops.into_iter().enumerate() {
            match op {
                Op::Put(k, v) => {
                    db.put(key(k), val(k, v)).await.unwrap();
                    model.insert(key(k), val(k, v));
                }
                Op::Delete(k) => {
                    db.delete(key(k)).await.unwrap();
                    model.remove(&key(k));
                }
                Op::Get(k) => {
                    let got = db.get(&key(k)).await.unwrap();
                    let want = model.get(&key(k)).map(|v| Bytes::from(v.clone()));
                    assert_eq!(got, want, "step {step}: get key{k:05}");
                }
                Op::Scan(a, b) => {
                    let got = db.scan(key(a)..=key(b), 1000).await.unwrap();
                    let want: Vec<(Bytes, Bytes)> = model
                        .range(key(a)..=key(b))
                        .map(|(k, v)| (Bytes::from(k.clone()), Bytes::from(v.clone())))
                        .collect();
                    assert_eq!(got, want, "step {step}: scan {a}..={b}");
                }
                Op::Flush => db.flush().await.unwrap(),
                Op::Compact => {
                    db.flush().await.unwrap();
                    db.wait_for_compaction().await.unwrap();
                    db.check_invariants().unwrap();
                }
                Op::Reopen => {
                    db.close().await.unwrap();
                    db = Db::open(dir.path(), common::small_opts()).await.unwrap();
                }
            }
        }
        // Final full comparison.
        let all = db.scan::<&[u8]>(.., usize::MAX).await.unwrap();
        let want: Vec<(Bytes, Bytes)> =
            model.iter().map(|(k, v)| (Bytes::from(k.clone()), Bytes::from(v.clone()))).collect();
        assert_eq!(all, want, "final scan");
        db.check_invariants().unwrap();
        db.close().await.unwrap();
    });
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("MODEL_CASES").ok().and_then(|s| s.parse().ok()).unwrap_or(24),
        failure_persistence: None,
        .. ProptestConfig::default()
    })]

    #[test]
    fn matches_btreemap(ops in prop::collection::vec(op(), 1..1500)) {
        run(ops);
    }
}
