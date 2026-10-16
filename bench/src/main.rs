//! db_bench-style workloads for RustLog, plus RocksDB as a baseline.
//!
//! ```text
//! cargo run --release -p bench -- --n 1000000                         # RustLog only
//! cargo run --release -p bench --features rocksdb -- --n 1000000 --rocksdb
//! ```
//!
//! Keys are 16 bytes, values 100 bytes (the db_bench defaults). Writes are
//! not fsynced unless the workload says so (also the db_bench default).
//! Every number in the README comes from running this file.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rustlog::{Db, Options, WriteOptions};

#[cfg(feature = "rocksdb")]
mod rocks;

pub const KEY_LEN: usize = 16;
pub const VALUE_LEN: usize = 100;

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone)]
pub struct Args {
    pub n: u64,
    pub reads: u64,
    pub tasks: usize,
    pub dir: PathBuf,
    pub out: Option<PathBuf>,
    pub rocksdb: bool,
    pub only: Option<String>,
}

fn parse_args() -> Res<Args> {
    let mut a = Args {
        n: 1_000_000,
        reads: 200_000,
        tasks: 16,
        dir: std::env::temp_dir().join("rustlog-bench"),
        out: None,
        rocksdb: false,
        only: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--n" => a.n = val()?.parse()?,
            "--reads" => a.reads = val()?.parse()?,
            "--tasks" => a.tasks = val()?.parse()?,
            "--dir" => a.dir = val()?.into(),
            "--out" => a.out = Some(val()?.into()),
            "--only" => a.only = Some(val()?),
            "--rocksdb" => a.rocksdb = true,
            other => return Err(format!("unknown flag {other}").into()),
        }
    }
    Ok(a)
}

/// One result line.
#[derive(Debug, Clone)]
pub struct Row {
    pub engine: &'static str,
    pub workload: String,
    pub ops: u64,
    pub secs: f64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub note: String,
}

impl Row {
    pub fn new(engine: &'static str, workload: &str, ops: u64, secs: f64, h: &Histogram<u64>, note: String) -> Row {
        let q = |p: f64| h.value_at_quantile(p) as f64 / 1000.0;
        Row {
            engine,
            workload: workload.to_string(),
            ops,
            secs,
            p50_us: q(0.5),
            p99_us: q(0.99),
            p999_us: q(0.999),
            note,
        }
    }

    pub fn print(&self) {
        println!(
            "| {:<8} | {:<28} | {:>10.0} | {:>8.1} | {:>8.1} | {:>9.1} | {} |",
            self.engine,
            self.workload,
            self.ops as f64 / self.secs,
            self.p50_us,
            self.p99_us,
            self.p999_us,
            self.note
        );
    }
}

pub fn header() {
    println!("| engine   | workload                     |      ops/s | p50 (us) | p99 (us) | p99.9 (us) | notes |");
    println!("|----------|------------------------------|-----------:|---------:|---------:|----------:|-------|");
}

pub fn hist() -> Histogram<u64> {
    // 1 ns .. 100 s, 3 significant digits. Only fails on invalid bounds.
    match Histogram::new_with_bounds(1, 100_000_000_000, 3) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("bad histogram bounds: {e}");
            std::process::exit(2)
        }
    }
}

/// Fixed-width decimal key, written without allocating.
pub fn key(k: u64) -> [u8; KEY_LEN] {
    let mut b = [b'0'; KEY_LEN];
    let mut v = k;
    for i in (0..KEY_LEN).rev() {
        b[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    b
}

/// Random-looking values sliced out of one shared buffer: zero allocations
/// per write on the benchmark side.
pub struct Values(Bytes);

impl Default for Values {
    fn default() -> Self {
        Self::new()
    }
}

impl Values {
    pub fn new() -> Values {
        let mut rng = StdRng::seed_from_u64(7);
        let v: Vec<u8> = (0..(1 << 20)).map(|_| rng.random()).collect();
        Values(Bytes::from(v))
    }

    pub fn get(&self, k: u64) -> Bytes {
        let off = (k.wrapping_mul(7919) as usize) % (self.0.len() - VALUE_LEN);
        self.0.slice(off..off + VALUE_LEN)
    }
}

pub fn shuffled(n: u64, seed: u64) -> Vec<u64> {
    let mut v: Vec<u64> = (0..n).collect();
    v.shuffle(&mut StdRng::seed_from_u64(seed));
    v
}

pub fn dir_size(p: &Path) -> u64 {
    std::fs::read_dir(p).map(|d| d.filter_map(|e| e.ok()?.metadata().ok()).map(|m| m.len()).sum()).unwrap_or(0)
}

fn fresh(dir: &Path) -> Res<()> {
    if dir.exists() {
        std::fs::remove_dir_all(dir)?;
    }
    std::fs::create_dir_all(dir)?;
    Ok(())
}

// ---------------------------------------------------------------- RustLog --

/// Writes `keys` with `tasks` concurrent tasks. Returns the merged latency
/// histogram and elapsed seconds.
async fn rl_fill(db: &Db, keys: Arc<Vec<u64>>, tasks: usize, sync: bool) -> Res<(Histogram<u64>, f64)> {
    let values = Arc::new(Values::new());
    let t0 = Instant::now();
    let chunk = keys.len().div_ceil(tasks);
    let mut handles = Vec::new();
    for t in 0..tasks {
        let (db, keys, values) = (db.clone(), Arc::clone(&keys), Arc::clone(&values));
        handles.push(tokio::spawn(async move {
            let mut h = hist();
            let end = ((t + 1) * chunk).min(keys.len());
            for &k in &keys[(t * chunk).min(end)..end] {
                let s = Instant::now();
                let mut b = rustlog::WriteBatch::new();
                b.put(Bytes::copy_from_slice(&key(k)), values.get(k));
                db.write(b, WriteOptions { sync }).await?;
                h.record(s.elapsed().as_nanos() as u64).ok();
            }
            Ok::<_, rustlog::Error>(h)
        }));
    }
    let mut all = hist();
    for h in handles {
        all.add(h.await??).ok();
    }
    Ok((all, t0.elapsed().as_secs_f64()))
}

/// `tasks` concurrent readers doing `n` random gets in total.
/// `present`: read keys that exist (`0..keyspace`) or keys that don't.
async fn rl_read(db: &Db, n: u64, keyspace: u64, tasks: usize, present: bool) -> Res<(Histogram<u64>, f64, u64)> {
    let t0 = Instant::now();
    let found = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for t in 0..tasks {
        let (db, found) = (db.clone(), Arc::clone(&found));
        let per = n / tasks as u64;
        handles.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(100 + t as u64);
            let mut h = hist();
            for _ in 0..per {
                // Missing keys sit between real ones (odd suffix), so the
                // index alone can't rule them out: only the Bloom filter can.
                let k = rng.random_range(0..keyspace);
                let mut kb = key(k);
                if !present {
                    kb[KEY_LEN - 1] = b'x';
                }
                let s = Instant::now();
                if db.get(&kb).await?.is_some() {
                    found.fetch_add(1, Ordering::Relaxed);
                }
                h.record(s.elapsed().as_nanos() as u64).ok();
            }
            Ok::<_, rustlog::Error>(h)
        }));
    }
    let mut all = hist();
    for h in handles {
        all.add(h.await??).ok();
    }
    Ok((all, t0.elapsed().as_secs_f64(), found.load(Ordering::Relaxed)))
}

fn bench_opts() -> Options {
    Options::default()
}

fn levels(db: &Db) -> String {
    db.stats().levels.iter().map(|(n, _)| n.to_string()).collect::<Vec<_>>().join("/")
}

async fn rustlog_suite(a: &Args, rows: &mut Vec<Row>) -> Res<()> {
    let want = |w: &str| a.only.as_deref().is_none_or(|o| w.contains(o) || o.contains(w));
    let raw = a.n * (KEY_LEN + VALUE_LEN) as u64;

    if want("fillseq") {
        let dir = a.dir.join("rl-fillseq");
        fresh(&dir)?;
        let db = Db::open(&dir, bench_opts()).await?;
        let keys = Arc::new((0..a.n).collect::<Vec<_>>());
        let (h, secs) = rl_fill(&db, keys, 1, false).await?;
        db.flush().await?;
        db.wait_for_compaction().await?;
        let s = db.stats();
        rows.push(Row::new(
            "rustlog",
            "fillseq (1 task)",
            a.n,
            secs,
            &h,
            format!(
                "write amp {:.2}, {} trivial moves, levels {}",
                s.write_amplification(),
                s.trivial_moves,
                levels(&db)
            ),
        ));
        rows.last().map(Row::print);
        db.close().await?;
    }

    let fill_dir = a.dir.join("rl-fillrandom");
    if want("fillrandom") || want("read") || want("probe") || want("scan") {
        fresh(&fill_dir)?;
        let db = Db::open(&fill_dir, bench_opts()).await?;
        let keys = Arc::new(shuffled(a.n, 1));
        let (h, secs) = rl_fill(&db, keys, a.tasks, false).await?;
        let t = Instant::now();
        db.flush().await?;
        db.wait_for_compaction().await?;
        let settle = t.elapsed().as_secs_f64();
        let s = db.stats();
        let size = dir_size(&fill_dir);
        rows.push(Row::new(
            "rustlog",
            &format!("fillrandom ({} tasks)", a.tasks),
            a.n,
            secs,
            &h,
            format!(
                "write amp {:.2}, space amp {:.2}, stalled {:.1}s, +{settle:.1}s to settle, levels {}",
                s.write_amplification(),
                size as f64 / raw as f64,
                s.stall_micros as f64 / 1e6,
                levels(&db)
            ),
        ));
        rows.last().map(Row::print);
        db.close().await?;
    }

    if want("fillsync") {
        let dir = a.dir.join("rl-fillsync");
        for tasks in [1usize, 64] {
            fresh(&dir)?;
            let db = Db::open(&dir, bench_opts()).await?;
            let n = if tasks == 1 { 2_000 } else { 20_000 };
            let keys = Arc::new(shuffled(n, 3));
            let (h, secs) = rl_fill(&db, keys, tasks, true).await?;
            let s = db.stats();
            rows.push(Row::new(
                "rustlog",
                &format!("fillsync ({tasks} tasks)"),
                n,
                secs,
                &h,
                format!(
                    "{} fsyncs for {n} writes ({:.1} writes per fsync)",
                    s.wal_syncs,
                    n as f64 / s.wal_syncs as f64
                ),
            ));
            rows.last().map(Row::print);
            db.close().await?;
        }
    }

    if want("readrandom") {
        // Reopen: cold block cache, small enough that most reads miss it.
        let opts = Options { block_cache_bytes: 8 << 20, ..bench_opts() };
        let db = Db::open(&fill_dir, opts).await?;
        let (h, secs, found) = rl_read(&db, a.reads, a.n, a.tasks, true).await?;
        let s = db.stats();
        rows.push(Row::new(
            "rustlog",
            &format!("readrandom ({} tasks)", a.tasks),
            a.reads,
            secs,
            &h,
            format!(
                "found {found}/{}, {:.0}% served without leaving the async task, block cache hit {:.0}%",
                a.reads,
                100.0 * s.reads_without_io as f64 / (s.reads_without_io + s.reads_with_io).max(1) as f64,
                100.0 * s.cache_hits as f64 / (s.cache_hits + s.cache_misses).max(1) as f64
            ),
        ));
        rows.last().map(Row::print);
        db.close().await?;
    }

    if want("readmissing") {
        let db = Db::open(&fill_dir, Options { block_cache_bytes: 8 << 20, ..bench_opts() }).await?;
        let (h, secs, found) = rl_read(&db, a.reads, a.n, a.tasks, false).await?;
        let s = db.stats();
        rows.push(Row::new(
            "rustlog",
            &format!("readmissing ({} tasks)", a.tasks),
            a.reads,
            secs,
            &h,
            format!(
                "found {found}, bloom skipped {:.1}% of table checks, {:.3} block reads per lookup",
                100.0 * s.bloom_negatives as f64 / s.bloom_checks.max(1) as f64,
                s.disk_block_reads as f64 / a.reads as f64
            ),
        ));
        rows.last().map(Row::print);
        db.close().await?;
    }

    if want("bloom") {
        // Same data, Bloom filters on vs off. Smaller DB: it's filled twice.
        let n = (a.n / 4).max(10_000);
        for bits in [10usize, 0] {
            let dir = a.dir.join(format!("rl-bloom{bits}"));
            fresh(&dir)?;
            let opts = Options { bloom_bits_per_key: bits, block_cache_bytes: 0, ..bench_opts() };
            let db = Db::open(&dir, opts.clone()).await?;
            rl_fill(&db, Arc::new(shuffled(n, 5)), a.tasks, false).await?;
            db.flush().await?;
            db.wait_for_compaction().await?;
            db.close().await?;
            let db = Db::open(&dir, opts).await?;
            let (h, secs, _) = rl_read(&db, a.reads, n, a.tasks, false).await?;
            let s = db.stats();
            rows.push(Row::new(
                "rustlog",
                &format!("readmissing bloom={bits}"),
                a.reads,
                secs,
                &h,
                format!(
                    "{n} keys, no block cache: {:.2} block reads per lookup",
                    s.disk_block_reads as f64 / a.reads as f64
                ),
            ));
            rows.last().map(Row::print);
            db.close().await?;
        }
    }

    if want("scan") {
        let db = Db::open(&fill_dir, bench_opts()).await?;
        let scans = 2_000u64;
        let mut rng = StdRng::seed_from_u64(9);
        let mut h = hist();
        let t0 = Instant::now();
        let mut entries = 0;
        for _ in 0..scans {
            let start = key(rng.random_range(0..a.n));
            let s = Instant::now();
            entries += db.scan(&start[..].., 100).await?.len();
            h.record(s.elapsed().as_nanos() as u64).ok();
        }
        rows.push(Row::new(
            "rustlog",
            "scan 100 (1 task)",
            scans,
            t0.elapsed().as_secs_f64(),
            &h,
            format!("{entries} entries"),
        ));
        rows.last().map(Row::print);
        db.close().await?;
    }
    Ok(())
}

// ------------------------------------------------- runtime blocking probe --

#[derive(Debug)]
pub struct ProbeResult {
    pub device: &'static str,
    pub mode: &'static str,
    pub reads: u64,
    pub secs: f64,
    /// Round trip: OS thread -> task on the runtime -> in-memory get -> reply.
    pub ping: Histogram<u64>,
}

/// The thesis test. A Tokio runtime with 2 workers runs 64 tasks doing
/// random gets that miss every cache (no block cache, OS page cache
/// bypassed), so each one waits on the SSD.
///
/// Meanwhile a plain OS thread (outside the runtime) pings a task on the
/// runtime every 500 us. That task does a get for a key in the memtable
/// (pure RAM, ~1 us) and replies. The round trip is how long a request that
/// needs NO disk waits to be served while the disk-bound load runs.
///
/// No Tokio timers anywhere in the measurement: macOS timer coalescing makes
/// `sleep(1ms)` jitter by milliseconds even on an idle runtime.
///
/// Readers yield after each get, like a request handler awaiting its socket
/// would. Without the yield, inline mode starves the runtime completely and
/// the ping never gets answered at all.
fn probe(dir: &Path, keyspace: u64, readers_n: u64, inline: bool, slow_us: u64, secs: u64) -> Res<ProbeResult> {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
    let opts = Options {
        block_cache_bytes: 0,
        bypass_os_cache: true,
        inline_disk_reads: inline,
        simulated_read_latency: Duration::from_micros(slow_us),
        ..bench_opts()
    };
    let db = rt.block_on(async {
        let db = Db::open(dir, opts).await?;
        db.put("hot-key", "in the memtable").await?;
        Ok::<_, rustlog::Error>(db)
    })?;
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let mut readers = Vec::new();
    for t in 0..readers_n {
        let (db, stop, reads) = (db.clone(), Arc::clone(&stop), Arc::clone(&reads));
        readers.push(rt.spawn(async move {
            let mut rng = StdRng::seed_from_u64(t);
            while !stop.load(Ordering::Relaxed) {
                db.get(&key(rng.random_range(0..keyspace))).await?;
                reads.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
            Ok::<_, rustlog::Error>(())
        }));
    }
    let (ping_tx, mut ping_rx) = tokio::sync::mpsc::channel::<tokio::sync::oneshot::Sender<()>>(1);
    let echo_db = db.clone();
    let echo = rt.spawn(async move {
        while let Some(reply) = ping_rx.recv().await {
            let _ = echo_db.get(b"hot-key").await;
            let _ = reply.send(());
        }
    });

    // Warm up, then measure from a plain thread.
    std::thread::sleep(Duration::from_millis(500));
    let t0 = Instant::now();
    let reads0 = reads.load(Ordering::Relaxed);
    let mut ping = hist();
    while t0.elapsed() < Duration::from_secs(secs) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let s = Instant::now();
        if ping_tx.blocking_send(tx).is_err() || rx.blocking_recv().is_err() {
            return Err("echo task died".into());
        }
        ping.record(s.elapsed().as_nanos().max(1) as u64).ok();
        std::thread::sleep(Duration::from_micros(500));
    }
    let secs_f = t0.elapsed().as_secs_f64();
    let n_reads = reads.load(Ordering::Relaxed) - reads0;
    stop.store(true, Ordering::Relaxed);
    drop(ping_tx);
    rt.block_on(async {
        for r in readers {
            r.await??;
        }
        echo.await?;
        db.close().await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })?;
    Ok(ProbeResult {
        device: if slow_us == 0 { "fast reads (~20 us)" } else { "slow reads (+1 ms, network disk)" },
        mode: match (readers_n, inline) {
            (0, _) => "idle (no readers): baseline",
            (_, false) => "spawn_blocking (RustLog default)",
            (_, true) => "pread on the async worker (anti-pattern)",
        },
        reads: n_reads,
        secs: secs_f,
        ping,
    })
}

const PROBE_HEADER: &str =
    "| device | mode | disk reads/s | pings | RAM get round trip p50 (us) | p99 (us) | p99.9 (us) | max (us) |";
const PROBE_SEP: &str = "|---|---|---:|---:|---:|---:|---:|---:|";

fn probe_line(p: &ProbeResult) -> String {
    let q = |x: f64| p.ping.value_at_quantile(x) as f64 / 1000.0;
    format!(
        "| {} | {} | {:.0} | {} | {:.0} | {:.0} | {:.0} | {:.0} |",
        p.device,
        p.mode,
        p.reads as f64 / p.secs,
        p.ping.len(),
        q(0.5),
        q(0.99),
        q(0.999),
        p.ping.max() as f64 / 1000.0
    )
}

fn main() -> Res<()> {
    let a = parse_args()?;
    std::fs::create_dir_all(&a.dir)?;
    println!(
        "# RustLog benchmark\n\nn = {} keys ({KEY_LEN} B keys, {VALUE_LEN} B values), {} reads, {} tasks/threads, dir {}\n",
        a.n,
        a.reads,
        a.tasks,
        a.dir.display()
    );
    let mut rows = Vec::new();
    header();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(rustlog_suite(&a, &mut rows))?;

    #[cfg(feature = "rocksdb")]
    if a.rocksdb {
        rocks::suite(&a, &mut rows)?;
    }
    #[cfg(not(feature = "rocksdb"))]
    if a.rocksdb {
        return Err("rebuild with --features rocksdb".into());
    }

    let mut probes = Vec::new();
    let fill_dir = a.dir.join("rl-fillrandom");
    if a.only.as_deref().is_none_or(|o| o.contains("probe")) && fill_dir.exists() {
        println!("\n## Is the Tokio runtime blocked? (2 workers, 64 disk-bound readers)\n");
        println!("{PROBE_HEADER}\n{PROBE_SEP}");
        for (readers, inline, slow_us) in
            [(0, false, 0), (64, false, 0), (64, true, 0), (64, false, 1000), (64, true, 1000)]
        {
            let p = probe(&fill_dir, a.n, readers, inline, slow_us, 10)?;
            println!("{}", probe_line(&p));
            probes.push(p);
        }
    }

    if let Some(out) = &a.out {
        write_report(out, &a, &rows, &probes)?;
        println!("\nwrote {}", out.display());
    }
    Ok(())
}

fn write_report(out: &Path, a: &Args, rows: &[Row], probes: &[ProbeResult]) -> Res<()> {
    use std::fmt::Write;
    let mut s = String::new();
    writeln!(s, "# Benchmark results\n")?;
    writeln!(
        s,
        "Command: `cargo run --release -p bench --features rocksdb -- --n {} --reads {} --tasks {}{}`\n",
        a.n,
        a.reads,
        a.tasks,
        if a.rocksdb { " --rocksdb" } else { "" }
    )?;
    writeln!(s, "Machine: {}\n", machine())?;
    writeln!(s, "Keys {KEY_LEN} B, values {VALUE_LEN} B, no compression, writes not fsynced unless noted.\n")?;
    writeln!(s, "| engine | workload | ops/s | p50 (us) | p99 (us) | p99.9 (us) | notes |")?;
    writeln!(s, "|---|---|---:|---:|---:|---:|---|")?;
    for r in rows {
        writeln!(
            s,
            "| {} | {} | {:.0} | {:.1} | {:.1} | {:.1} | {} |",
            r.engine,
            r.workload,
            r.ops as f64 / r.secs,
            r.p50_us,
            r.p99_us,
            r.p999_us,
            r.note
        )?;
    }
    if !probes.is_empty() {
        writeln!(s, "\n## Runtime blocking probe\n")?;
        writeln!(s, "Tokio runtime with 2 worker threads. 64 tasks do random gets with no block cache and `F_NOCACHE` set. On macOS, `F_NOCACHE` doesn't evict pages that are already cached, and the DB was just written, so the \"fast reads\" rows mostly hit the OS cache (inline mode did ~107k reads/s with only 2 in flight, about 19 us each). The \"slow reads\" rows add a simulated 1 ms per block read (a sleep in the reading thread), to model a cloud network disk. A plain OS thread pings a task on the runtime every 500 us; that task reads a key from the memtable (pure RAM) and replies. The round trip is how long a request that needs no disk waits while the disk-bound load runs.\n")?;
        writeln!(s, "{PROBE_HEADER}\n{PROBE_SEP}")?;
        for p in probes {
            writeln!(s, "{}", probe_line(p))?;
        }
    }
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, s)?;
    Ok(())
}

fn machine() -> String {
    let run = |cmd: &str, args: &[&str]| {
        std::process::Command::new(cmd)
            .args(args)
            .output()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|s| !s.is_empty())
    };
    let cpu = run("sysctl", &["-n", "machdep.cpu.brand_string"])
        .or_else(|| run("sh", &["-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"]))
        .unwrap_or_else(|| "unknown CPU".into());
    let os = run("uname", &["-sr"]).unwrap_or_default();
    format!("{cpu}, {} threads, {os}", std::thread::available_parallelism().map_or(0, |n| n.get()))
}
