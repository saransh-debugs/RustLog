#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Real crash test: a child process writes as fast as it can and prints each
//! acknowledged key. The parent `kill -9`s it at a random moment, reopens
//! the DB, and checks every acknowledged key is there with the right value.
//!
//! Repeated on the SAME directory, so we also recover from states that were
//! themselves left by a recovery, a flush, or a compaction cut short.
//!
//! The child is this same test binary, re-run with `CRASH_CHILD_DIR` set.
//!
//! `kill -9` loses the process, not the OS, so even `sync: false` writes
//! must survive (they're in the page cache). Power loss is a different
//! test this can't do; that's what `sync: true` + fsync are for.

mod common;

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use rand::Rng;
use rustlog::{Db, WriteOptions};

fn value(i: u64) -> String {
    format!("value-{i}-{}", "z".repeat((i % 50) as usize))
}

/// The child: only does anything when the parent set the env var.
#[test]
fn crash_child() {
    let Ok(dir) = std::env::var("CRASH_CHILD_DIR") else { return };
    let start: u64 = std::env::var("CRASH_CHILD_START").unwrap().parse().unwrap();
    let sync = std::env::var("CRASH_CHILD_SYNC").unwrap() == "1";
    let rt = common::rt();
    rt.block_on(async {
        let db = Db::open(&dir, common::small_opts()).await.unwrap();
        println!("OPEN");
        let mut i = start;
        loop {
            let mut b = rustlog::WriteBatch::new();
            b.put(format!("key{i:08}"), value(i));
            // Every 7th write also deletes an older key, so tombstones
            // are part of what must survive.
            if i.is_multiple_of(7) && i >= 10 {
                b.delete(format!("key{:08}", i - 10));
            }
            db.write(b, WriteOptions { sync }).await.unwrap();
            // Only print AFTER the ack. The parent trusts what it reads here.
            println!("{i}");
            i += 1;
        }
    });
}

/// Should key `i` exist, given writes `[.., end)` of its round were acked?
/// Write `j` deletes key `j - 10` when `j % 7 == 0`. If that deleting write
/// wasn't acked it may or may not have landed, so the answer is `None`.
fn expected(i: u64, end: u64) -> Option<bool> {
    let deleter = i + 10;
    if !deleter.is_multiple_of(7) {
        Some(true)
    } else if deleter < end {
        Some(false)
    } else {
        None
    }
}

fn crash_rounds(sync: bool) {
    let rounds: usize = std::env::var("CRASH_ROUNDS").ok().and_then(|s| s.parse().ok()).unwrap_or(30);
    let dir = tempfile::tempdir().unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut rng = rand::rng();
    let mut next = 0u64;
    // `[start, end)` of acked writes per round. Writes after `end` in a
    // round may or may not have landed, so nothing checks them.
    let mut acked_ranges: Vec<(u64, u64)> = Vec::new();
    let rt = common::rt();

    for round in 0..rounds {
        let mut child = Command::new(&exe)
            .args(["--exact", "crash_child", "--nocapture", "--test-threads=1"])
            .env("CRASH_CHILD_DIR", dir.path())
            .env("CRASH_CHILD_START", next.to_string())
            .env("CRASH_CHILD_SYNC", if sync { "1" } else { "0" })
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let out = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });

        // Wait for the open, then let it write for a random time.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match rx.recv_timeout(Duration::from_secs(30)) {
                // libtest prints "test crash_child ... " with no newline first.
                Ok(l) if l.ends_with("OPEN") => break,
                Ok(_) => {}
                Err(_) => panic!("child never opened the DB"),
            }
            assert!(Instant::now() < deadline);
        }
        let run_for = Duration::from_millis(rng.random_range(5..300));
        std::thread::sleep(run_for);
        child.kill().unwrap(); // SIGKILL: no destructors, no flush, nothing.
        child.wait().unwrap();
        reader.join().unwrap();

        let mut last_acked: Option<u64> = None;
        while let Ok(l) = rx.try_recv() {
            if let Ok(i) = l.parse::<u64>() {
                last_acked = Some(i);
            }
        }

        let first = next;
        let acked_end = last_acked.map_or(first, |i| i + 1);
        rt.block_on(async {
            let db = Db::open(dir.path(), common::small_opts()).await.unwrap();
            db.check_invariants().unwrap();
            let check = |i: u64, end: u64, got: Option<bytes::Bytes>, what: &str| match expected(i, end) {
                Some(true) => assert_eq!(
                    got.as_deref(),
                    Some(value(i).as_bytes()),
                    "round {round}: {what} key{i:08} lost (acked up to {end})"
                ),
                Some(false) => assert_eq!(got, None, "round {round}: {what} key{i:08} should be deleted"),
                None => {}
            };
            // Everything acked in this round.
            for i in first..acked_end {
                check(i, acked_end, db.get(format!("key{i:08}").as_bytes()).await.unwrap(), "new");
            }
            // Spot-check keys acked in earlier rounds.
            for &(s, e) in acked_ranges.iter().filter(|(s, e)| s < e) {
                for _ in 0..10 {
                    let i = rng.random_range(s..e);
                    check(i, e, db.get(format!("key{i:08}").as_bytes()).await.unwrap(), "old");
                }
            }
            db.close().await.unwrap();
        });
        acked_ranges.push((first, acked_end));
        // Start the next round past anything that might have been written,
        // acked or not, so later checks never depend on an unacked write.
        next = acked_end + 1000;
    }
    let total: u64 = acked_ranges.iter().map(|(s, e)| e - s).sum();
    eprintln!("crash test (sync={sync}): {rounds} kills, {total} acked writes verified");
    // Sync writes are slow on macOS (F_FULLFSYNC is ~10-30 ms), so a short
    // round may ack only a few. Just make sure the test isn't vacuous.
    assert!(total >= rounds as u64, "children barely wrote anything: {total} writes in {rounds} rounds");
}

#[test]
fn kill9_with_sync_writes() {
    crash_rounds(true);
}

#[test]
fn kill9_with_buffered_writes() {
    crash_rounds(false);
}
