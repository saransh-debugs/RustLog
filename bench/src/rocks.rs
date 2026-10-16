//! The same workloads on RocksDB, configured to match RustLog's defaults:
//! same memtable size and count, same L0 triggers, same level sizes, same
//! block size, 10-bit Bloom filters, no compression, one flush thread and
//! one compaction thread. RocksDB's API is blocking, so it runs on plain
//! OS threads (its natural mode), not Tokio tasks.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rocksdb::statistics::Ticker;
use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, Options, WaitForCompactOptions, WriteOptions, DB};

use crate::{dir_size, hist, key, shuffled, Args, Res, Row, Values, KEY_LEN, VALUE_LEN};

const _: usize = VALUE_LEN;

fn opts(cache_bytes: usize) -> Options {
    let mut o = Options::default();
    o.create_if_missing(true);
    o.set_compression_type(DBCompressionType::None);
    o.set_write_buffer_size(4 << 20);
    o.set_max_write_buffer_number(3); // 1 active + 2 immutable
    o.set_level_zero_file_num_compaction_trigger(4);
    o.set_level_zero_slowdown_writes_trigger(8);
    o.set_level_zero_stop_writes_trigger(12);
    o.set_target_file_size_base(2 << 20);
    o.set_max_bytes_for_level_base(10 << 20);
    o.set_max_bytes_for_level_multiplier(10.0);
    o.set_num_levels(7);
    o.set_max_background_jobs(2);
    o.enable_statistics();
    let mut t = BlockBasedOptions::default();
    t.set_block_size(4096);
    t.set_bloom_filter(10.0, false);
    t.set_block_cache(&Cache::new_lru_cache(cache_bytes));
    o.set_block_based_table_factory(&t);
    o
}

fn fill(db: &Arc<DB>, keys: &Arc<Vec<u64>>, threads: usize, sync: bool) -> Res<(Histogram<u64>, f64)> {
    let values = Arc::new(Values::new());
    let chunk = keys.len().div_ceil(threads);
    let t0 = Instant::now();
    let hs: Vec<Res<Histogram<u64>>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|t| {
                let (db, keys, values) = (Arc::clone(db), Arc::clone(keys), Arc::clone(&values));
                s.spawn(move || -> Res<Histogram<u64>> {
                    let mut wo = WriteOptions::default();
                    wo.set_sync(sync);
                    let mut h = hist();
                    let end = ((t + 1) * chunk).min(keys.len());
                    for &k in &keys[(t * chunk).min(end)..end] {
                        let st = Instant::now();
                        db.put_opt(key(k), values.get(k), &wo)?;
                        h.record(st.elapsed().as_nanos() as u64).ok();
                    }
                    Ok(h)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().map_err(|_| "writer thread panicked")?).collect()
    });
    let mut all = hist();
    for h in hs {
        all.add(h?).ok();
    }
    Ok((all, t0.elapsed().as_secs_f64()))
}

fn read(db: &Arc<DB>, n: u64, keyspace: u64, threads: usize, present: bool) -> Res<(Histogram<u64>, f64, u64)> {
    let t0 = Instant::now();
    let res: Vec<Res<(Histogram<u64>, u64)>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|t| {
                let db = Arc::clone(db);
                s.spawn(move || -> Res<(Histogram<u64>, u64)> {
                    let mut rng = StdRng::seed_from_u64(100 + t as u64);
                    let mut h = hist();
                    let mut found = 0;
                    for _ in 0..n / threads as u64 {
                        let mut kb = key(rng.random_range(0..keyspace));
                        if !present {
                            kb[KEY_LEN - 1] = b'x';
                        }
                        let st = Instant::now();
                        if db.get_pinned(kb)?.is_some() {
                            found += 1;
                        }
                        h.record(st.elapsed().as_nanos() as u64).ok();
                    }
                    Ok((h, found))
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().map_err(|_| "reader thread panicked")?).collect()
    });
    let mut all = hist();
    let mut found = 0;
    for r in res {
        let (h, f) = r?;
        all.add(h).ok();
        found += f;
    }
    Ok((all, t0.elapsed().as_secs_f64(), found))
}

fn settle(db: &DB) -> Res<f64> {
    let t = Instant::now();
    db.flush()?;
    let mut w = WaitForCompactOptions::default();
    w.set_flush(true);
    db.wait_for_compact(&w)?;
    Ok(t.elapsed().as_secs_f64())
}

fn write_amp(o: &Options, user_bytes: u64) -> f64 {
    let disk = o.get_ticker_count(Ticker::WalFileBytes)
        + o.get_ticker_count(Ticker::FlushWriteBytes)
        + o.get_ticker_count(Ticker::CompactWriteBytes);
    disk as f64 / user_bytes.max(1) as f64
}

fn levels(db: &DB) -> String {
    (0..7)
        .map(|l| {
            db.property_int_value(&format!("rocksdb.num-files-at-level{l}"))
                .ok()
                .flatten()
                .map_or("?".to_string(), |n| n.to_string())
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn fresh(dir: &Path) -> Res<()> {
    if dir.exists() {
        std::fs::remove_dir_all(dir)?;
    }
    Ok(())
}

pub fn suite(a: &Args, rows: &mut Vec<Row>) -> Res<()> {
    let want = |w: &str| a.only.as_deref().is_none_or(|o| w.contains(o) || o.contains(w));
    let raw = a.n * (KEY_LEN + VALUE_LEN) as u64;

    if want("fillseq") {
        let dir = a.dir.join("rocks-fillseq");
        fresh(&dir)?;
        let o = opts(64 << 20);
        let db = Arc::new(DB::open(&o, &dir)?);
        let (h, secs) = fill(&db, &Arc::new((0..a.n).collect()), 1, false)?;
        settle(&db)?;
        rows.push(Row::new(
            "rocksdb",
            "fillseq (1 thread)",
            a.n,
            secs,
            &h,
            format!("write amp {:.2}, levels {}", write_amp(&o, raw), levels(&db)),
        ));
        if let Some(r) = rows.last() {
            r.print();
        }
    }

    let fill_dir = a.dir.join("rocks-fillrandom");
    if want("fillrandom") || want("read") {
        fresh(&fill_dir)?;
        let o = opts(64 << 20);
        let db = Arc::new(DB::open(&o, &fill_dir)?);
        let (h, secs) = fill(&db, &Arc::new(shuffled(a.n, 1)), a.tasks, false)?;
        let settle_s = settle(&db)?;
        rows.push(Row::new(
            "rocksdb",
            &format!("fillrandom ({} threads)", a.tasks),
            a.n,
            secs,
            &h,
            format!(
                "write amp {:.2}, space amp {:.2}, stalled {:.1}s, +{settle_s:.1}s to settle, levels {}",
                write_amp(&o, raw),
                dir_size(&fill_dir) as f64 / raw as f64,
                o.get_ticker_count(Ticker::StallMicros) as f64 / 1e6,
                levels(&db)
            ),
        ));
        if let Some(r) = rows.last() {
            r.print();
        }
    }

    if want("fillsync") {
        let dir = a.dir.join("rocks-fillsync");
        for threads in [1usize, 64] {
            fresh(&dir)?;
            let o = opts(64 << 20);
            let db = Arc::new(DB::open(&o, &dir)?);
            let n = if threads == 1 { 2_000 } else { 20_000 };
            let (h, secs) = fill(&db, &Arc::new(shuffled(n, 3)), threads, true)?;
            rows.push(Row::new(
                "rocksdb",
                &format!("fillsync ({threads} threads)"),
                n,
                secs,
                &h,
                format!("{} WAL syncs", o.get_ticker_count(Ticker::WalFileSynced)),
            ));
            if let Some(r) = rows.last() {
                r.print();
            }
        }
    }

    if want("readrandom") {
        let o = opts(8 << 20);
        let db = Arc::new(DB::open(&o, &fill_dir)?);
        let (h, secs, found) = read(&db, a.reads, a.n, a.tasks, true)?;
        rows.push(Row::new(
            "rocksdb",
            &format!("readrandom ({} threads)", a.tasks),
            a.reads,
            secs,
            &h,
            format!("found {found}/{}", a.reads),
        ));
        if let Some(r) = rows.last() {
            r.print();
        }
    }

    if want("readmissing") {
        let o = opts(8 << 20);
        let db = Arc::new(DB::open(&o, &fill_dir)?);
        let (h, secs, found) = read(&db, a.reads, a.n, a.tasks, false)?;
        rows.push(Row::new(
            "rocksdb",
            &format!("readmissing ({} threads)", a.tasks),
            a.reads,
            secs,
            &h,
            format!("found {found}, bloom useful {}", o.get_ticker_count(Ticker::BloomFilterUseful)),
        ));
        if let Some(r) = rows.last() {
            r.print();
        }
    }
    Ok(())
}
