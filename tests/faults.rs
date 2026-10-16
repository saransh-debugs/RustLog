#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Disk failures (full disk, dying device) injected at each write point.
//! The DB must stop accepting writes, keep serving reads, and come back
//! intact after a reopen. It must never corrupt itself.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use rustlog::{Db, Error, FailPoint, FailPoints, Options, WriteOptions};

fn opts_with(fp: &Arc<FailPoints>) -> Options {
    Options { fail_points: Some(Arc::clone(fp)), ..common::small_opts() }
}

async fn fill(db: &Db, from: u32, to: u32) -> Result<(), Error> {
    for i in from..to {
        db.put(format!("key{i:06}"), format!("val{i}-{}", "q".repeat(30))).await?;
    }
    Ok(())
}

async fn verify(db: &Db, upto: u32) {
    for i in 0..upto {
        let got = db.get(format!("key{i:06}").as_bytes()).await.unwrap();
        assert_eq!(got, Some(Bytes::from(format!("val{i}-{}", "q".repeat(30)))), "key{i:06}");
    }
}

/// Fail at `point` after some good writes. Returns how many writes were acked.
async fn run_case(point: FailPoint, sync: bool) {
    let dir = tempfile::tempdir().unwrap();
    let fp = Arc::new(FailPoints::default());
    let db = Db::open(dir.path(), opts_with(&fp)).await.unwrap();
    fill(&db, 0, 2000).await.unwrap();
    fp.set(point, true);

    // Keep writing until the failure surfaces. Background failures (flush,
    // compaction, MANIFEST) only show up once the DB needs to do that work.
    let mut acked = 2000u32;
    let mut failed = false;
    for i in 2000..20_000u32 {
        let mut b = rustlog::WriteBatch::new();
        b.put(format!("key{i:06}"), format!("val{i}-{}", "q".repeat(30)));
        match db.write(b, WriteOptions { sync }).await {
            Ok(()) => acked = i + 1,
            Err(_) => {
                failed = true;
                break;
            }
        }
    }
    assert!(failed, "{point:?}: failure never surfaced");
    assert!(fp.hits.load(std::sync::atomic::Ordering::SeqCst) > 0);

    // Read-only from now on, but reads still work.
    assert!(matches!(db.put("after", "x").await, Err(Error::ReadOnly(_))), "{point:?}");
    assert!(db.background_error().is_some());
    verify(&db, acked).await;

    // "Disk fixed", reopen: everything acked is there, and writes work again.
    fp.set(point, false);
    db.close().await.unwrap();
    let db = Db::open(dir.path(), Options::default()).await.unwrap();
    verify(&db, acked).await;
    db.put("after", "x").await.unwrap();
    db.check_invariants().unwrap();
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_write_failure() {
    run_case(FailPoint::WalWrite, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_fsync_failure() {
    run_case(FailPoint::WalSync, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sstable_write_failure() {
    run_case(FailPoint::SstWrite, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manifest_write_failure() {
    run_case(FailPoint::ManifestWrite, false).await;
}
