#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Readers racing writers while flushes and compactions run underneath.
//!
//! Each key's value is a version counter that only goes up. Checks:
//! 1. A reader never sees a key's version go backwards.
//! 2. After a write is acknowledged, every read started later sees it
//!    (or something newer).
//! 3. At the end, every key holds its last written version.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rustlog::Db;

const KEYS: usize = 200;
const WRITERS: usize = 4;
const READERS: usize = 8;
const WRITES_PER_WRITER: u64 = 3000;

fn k(i: usize) -> String {
    format!("key{i:04}")
}

fn parse(v: &[u8]) -> u64 {
    std::str::from_utf8(&v[..20]).unwrap().parse().unwrap()
}

fn encode(ver: u64) -> String {
    // Version first (fixed width), padding after so values are big enough
    // to rotate memtables often.
    format!("{ver:020}{}", "x".repeat(60))
}

#[test]
fn readers_never_go_back_in_time() {
    let rt = common::rt();
    let dir = tempfile::tempdir().unwrap();
    rt.block_on(async {
        let db = Db::open(dir.path(), common::small_opts()).await.unwrap();
        // acked[i] = highest version of key i that a writer got an Ok for.
        let acked: Arc<Vec<AtomicU64>> = Arc::new((0..KEYS).map(|_| AtomicU64::new(0)).collect());
        let next_ver = Arc::new(AtomicU64::new(1));
        // Each writer owns KEYS/WRITERS keys, so per key the versions written
        // are strictly increasing in ack order.
        let mut tasks = Vec::new();
        for w in 0..WRITERS {
            let (db, acked, next_ver) = (db.clone(), Arc::clone(&acked), Arc::clone(&next_ver));
            tasks.push(tokio::spawn(async move {
                for n in 0..WRITES_PER_WRITER {
                    let i = w + WRITERS * (n as usize % (KEYS / WRITERS));
                    let ver = next_ver.fetch_add(1, Ordering::SeqCst);
                    db.put(k(i), encode(ver)).await.unwrap();
                    acked[i].store(ver, Ordering::SeqCst);
                }
            }));
        }
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut readers = Vec::new();
        for r in 0..READERS {
            let (db, acked, stop) = (db.clone(), Arc::clone(&acked), Arc::clone(&stop));
            readers.push(tokio::spawn(async move {
                let mut last_seen = vec![0u64; KEYS];
                let mut reads = 0u64;
                let mut i = r;
                while !stop.load(Ordering::Relaxed) {
                    i = (i * 31 + 7) % KEYS;
                    let floor = acked[i].load(Ordering::SeqCst);
                    let got = db.get(k(i).as_bytes()).await.unwrap().map_or(0, |v| parse(&v));
                    assert!(got >= floor, "key {i}: read v{got} after v{floor} was acked");
                    assert!(got >= last_seen[i], "key {i}: went back from v{} to v{got}", last_seen[i]);
                    last_seen[i] = got;
                    reads += 1;
                    if reads.is_multiple_of(64) {
                        // A scan must also be internally consistent: sorted, no dupes.
                        let s = db.scan::<&[u8]>(.., 50).await.unwrap();
                        assert!(s.windows(2).all(|w| w[0].0 < w[1].0));
                    }
                }
                reads
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        let mut total_reads = 0;
        for r in readers {
            total_reads += r.await.unwrap();
        }
        assert!(total_reads > 1000, "readers barely ran: {total_reads}");

        db.flush().await.unwrap();
        db.wait_for_compaction().await.unwrap();
        db.check_invariants().unwrap();
        for i in 0..KEYS {
            let got = db.get(k(i).as_bytes()).await.unwrap().map_or(0, |v| parse(&v));
            assert_eq!(got, acked[i].load(Ordering::SeqCst), "key {i} final value");
        }
        let s = db.stats();
        assert!(s.flushes > 5 && s.compactions > 0, "background work didn't happen: {s:?}");
        db.close().await.unwrap();
    });
}

#[test]
fn group_commit_batches_concurrent_sync_writes() {
    let rt = common::rt();
    let dir = tempfile::tempdir().unwrap();
    rt.block_on(async {
        let db = Db::open(dir.path(), rustlog::Options::default()).await.unwrap();
        let mut tasks = Vec::new();
        for t in 0..64 {
            let db = db.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..20 {
                    let mut b = rustlog::WriteBatch::new();
                    b.put(format!("t{t}-{i}"), "v");
                    db.write(b, rustlog::WriteOptions { sync: true }).await.unwrap();
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let s = db.stats();
        assert_eq!(s.writes, 64 * 20);
        // 1280 sync writes. Without group commit that's 1280 fsyncs.
        assert!(s.wal_syncs < 1280 / 2, "group commit didn't group: {} fsyncs for 1280 writes", s.wal_syncs);
        db.close().await.unwrap();
    });
}
