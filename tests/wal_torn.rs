#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Cut the WAL at EVERY byte offset and recover. Each cut simulates a crash
//! in the middle of a write. Recovery must return exactly the writes whose
//! record is fully on disk: never a partial record, never a missing one.

mod common;

use std::path::Path;

use bytes::Bytes;
use rustlog::{Db, Options};

const N: usize = 20;

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

#[test]
fn every_cut_point_recovers_a_clean_prefix() {
    let rt = common::rt();
    let base = tempfile::tempdir().unwrap();
    rt.block_on(async {
        let db = Db::open(base.path(), Options::default()).await.unwrap();
        for i in 0..N {
            db.put(format!("k{i:02}"), format!("v{i:02}")).await.unwrap();
        }
        db.close().await.unwrap();
    });

    let wal = std::fs::read_dir(base.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "log"))
        .unwrap();
    let full = std::fs::read(&wal).unwrap();
    // Each put is its own record: 8 header + 8 seq + 4 count + 1 kind
    // + (1 + 3) key + (1 + 3) value.
    let rec = 29;
    assert_eq!(full.len(), N * rec, "unexpected WAL layout");

    let scratch = tempfile::tempdir().unwrap();
    for cut in 0..=full.len() {
        let dir = scratch.path().join(format!("cut{cut}"));
        copy_dir(base.path(), &dir);
        let wal_copy = dir.join(wal.file_name().unwrap());
        std::fs::write(&wal_copy, &full[..cut]).unwrap();

        let expect = cut / rec;
        rt.block_on(async {
            let db = Db::open(&dir, Options::default()).await.unwrap();
            for i in 0..N {
                let got = db.get(format!("k{i:02}").as_bytes()).await.unwrap();
                if i < expect {
                    assert_eq!(got, Some(Bytes::from(format!("v{i:02}"))), "cut {cut}: k{i:02} lost");
                } else {
                    assert_eq!(got, None, "cut {cut}: k{i:02} came back from a torn record");
                }
            }
            db.close().await.unwrap();
        });
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[test]
fn damage_in_an_older_wal_is_reported_not_skipped() {
    let rt = common::rt();
    let dir = tempfile::tempdir().unwrap();
    rt.block_on(async {
        let db = Db::open(dir.path(), Options::default()).await.unwrap();
        db.put("a", "1").await.unwrap();
        db.close().await.unwrap();
    });
    // Fake an older WAL with garbage, then a newer intact one by reopening
    // with a fresh write. Simplest: put garbage in a low-numbered log.
    let logs: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .collect();
    assert_eq!(logs.len(), 1);
    let live = &logs[0];
    let num: u64 = live.file_stem().unwrap().to_str().unwrap().parse().unwrap();
    // Copy the live log to a higher number (the "newest"), and corrupt the
    // original (now an "older" log).
    std::fs::copy(live, dir.path().join(format!("{:06}.log", num + 100))).unwrap();
    let mut data = std::fs::read(live).unwrap();
    data.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef, 1, 2, 3]);
    std::fs::write(live, &data).unwrap();

    rt.block_on(async {
        match Db::open(dir.path(), Options::default()).await {
            Err(rustlog::Error::Corruption(msg)) => assert!(msg.contains("not the newest"), "{msg}"),
            other => panic!("expected corruption, got {other:?}"),
        }
    });
}
