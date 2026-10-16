#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use bytes::Bytes;
use rustlog::{Db, Error, Options, WriteBatch, WriteOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_get_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).await.unwrap();
    db.put("a", "1").await.unwrap();
    db.put("b", "2").await.unwrap();
    assert_eq!(db.get(b"a").await.unwrap(), Some(Bytes::from("1")));
    db.put("a", "3").await.unwrap();
    assert_eq!(db.get(b"a").await.unwrap(), Some(Bytes::from("3")));
    db.delete("a").await.unwrap();
    assert_eq!(db.get(b"a").await.unwrap(), None);
    assert_eq!(db.get(b"missing").await.unwrap(), None);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reopen_keeps_data_from_wal_and_sst() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), common::small_opts()).await.unwrap();
        for i in 0..5000u32 {
            db.put(format!("key{i:05}"), format!("val{i}")).await.unwrap();
        }
        for i in (0..5000u32).step_by(3) {
            db.delete(format!("key{i:05}")).await.unwrap();
        }
        db.close().await.unwrap();
    }
    for _round in 0..3 {
        let db = Db::open(dir.path(), common::small_opts()).await.unwrap();
        for i in 0..5000u32 {
            let got = db.get(format!("key{i:05}").as_bytes()).await.unwrap();
            if i % 3 == 0 {
                assert_eq!(got, None, "key{i:05} should be deleted");
            } else {
                assert_eq!(got, Some(Bytes::from(format!("val{i}"))), "key{i:05}");
            }
        }
        db.check_invariants().unwrap();
        db.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_builds_levels_and_drops_garbage() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), common::small_opts()).await.unwrap();
    // Overwrite the same 2000 keys 10 times: lots of garbage to compact away.
    for round in 0..10u32 {
        for i in 0..2000u32 {
            db.put(format!("key{i:05}"), format!("val{i}-{round}-{}", "x".repeat(20))).await.unwrap();
        }
    }
    db.flush().await.unwrap();
    db.wait_for_compaction().await.unwrap();
    db.check_invariants().unwrap();
    let s = db.stats();
    assert!(s.compactions > 0, "expected compactions, got {s:?}");
    let deeper: usize = s.levels.iter().skip(1).map(|(n, _)| n).sum();
    assert!(deeper > 0, "data should have moved below L0: {:?}", s.levels);
    let total: u64 = s.levels.iter().map(|(_, b)| b).sum();
    // Live data is ~2000 * ~45 bytes = 90 KB. Ten versions would be ~900 KB.
    assert!(total < 400 << 10, "old versions weren't dropped: {total} bytes on disk, levels {:?}", s.levels);
    for i in 0..2000u32 {
        let got = db.get(format!("key{i:05}").as_bytes()).await.unwrap().unwrap();
        assert!(got.starts_with(format!("val{i}-9-").as_bytes()));
    }
    db.close().await.unwrap();

    // Dead SSTables must actually be gone from disk.
    let files = std::fs::read_dir(dir.path()).unwrap().count();
    assert!(files < 60, "{files} files left in the DB dir");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_ranges_and_limits() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), common::small_opts()).await.unwrap();
    for i in 0..3000u32 {
        db.put(format!("k{i:05}"), format!("v{i}")).await.unwrap();
    }
    db.delete("k00010").await.unwrap();
    db.flush().await.unwrap();
    db.put("k00011", "new").await.unwrap(); // newer version in the memtable

    let r = db.scan("k00009".as_bytes().."k00013".as_bytes(), 100).await.unwrap();
    let keys: Vec<_> = r.iter().map(|(k, _)| String::from_utf8(k.to_vec()).unwrap()).collect();
    assert_eq!(keys, ["k00009", "k00011", "k00012"]);
    assert_eq!(r[1].1, Bytes::from("new"));

    let all = db.scan::<&[u8]>(.., usize::MAX).await.unwrap();
    assert_eq!(all.len(), 2999);
    assert!(all.windows(2).all(|w| w[0].0 < w[1].0));

    let page = db.scan::<&[u8]>(.., 10).await.unwrap();
    assert_eq!(page.len(), 10);
    let excl = db
        .scan::<&[u8]>(
            (std::ops::Bound::Excluded("k00000".as_bytes()), std::ops::Bound::Included("k00002".as_bytes())),
            10,
        )
        .await
        .unwrap();
    assert_eq!(excl.len(), 2);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batches_are_atomic_and_sync_works() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).await.unwrap();
    let mut b = WriteBatch::new();
    b.put("x", "1").put("y", "2").delete("x");
    db.write(b, WriteOptions { sync: true }).await.unwrap();
    assert_eq!(db.get(b"x").await.unwrap(), None);
    assert_eq!(db.get(b"y").await.unwrap(), Some(Bytes::from("2")));
    assert!(db.stats().wal_syncs >= 1);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_oversized_input() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options { max_key_size: 16, max_value_size: 64, ..Options::default() };
    let db = Db::open(dir.path(), opts).await.unwrap();
    assert!(matches!(db.put(vec![0u8; 17], "v").await, Err(Error::InvalidArgument(_))));
    assert!(matches!(db.put("k", vec![0u8; 65]).await, Err(Error::InvalidArgument(_))));
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_open_is_locked_out() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).await.unwrap();
    assert!(matches!(Db::open(dir.path(), Options::default()).await, Err(Error::Locked)));
    db.close().await.unwrap();
    // Released after close.
    let db = Db::open(dir.path(), Options::default()).await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_handle_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).await.unwrap();
    let other = db.clone();
    db.close().await.unwrap();
    assert!(matches!(other.put("a", "b").await, Err(Error::Closed)));
    assert!(matches!(other.get(b"a").await, Err(Error::Closed)));
}

#[tokio::test(flavor = "current_thread")]
async fn works_on_current_thread_runtime() {
    // Nothing in the engine assumes a multi-threaded runtime.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), common::small_opts()).await.unwrap();
    for i in 0..2000u32 {
        db.put(format!("k{i}"), "v").await.unwrap();
    }
    db.flush().await.unwrap();
    assert_eq!(db.get(b"k1999").await.unwrap(), Some(Bytes::from("v")));
    db.close().await.unwrap();
}
