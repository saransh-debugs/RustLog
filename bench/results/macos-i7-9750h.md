# Benchmark results

Command: `cargo run --release -p bench --features rocksdb -- --n 1000000 --reads 200000 --tasks 16 --rocksdb`

Machine: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz, 12 threads, Darwin 24.6.0

Keys 16 B, values 100 B, no compression, writes not fsynced unless noted.

| engine | workload | ops/s | p50 (us) | p99 (us) | p99.9 (us) | notes |
|---|---|---:|---:|---:|---:|---|
| rustlog | fillseq (1 task) | 7219 | 99.4 | 672.3 | 3672.1 | write amp 3.17, 56 trivial moves, levels 1/5/54/1/0/0/0 |
| rustlog | fillrandom (16 tasks) | 40848 | 160.0 | 3039.2 | 35782.7 | write amp 7.09, space amp 1.07, stalled 0.0s, +1.4s to settle, levels 1/5/49/1/0/0/0 |
| rustlog | fillsync (1 tasks) | 46 | 19841.0 | 91947.0 | 159907.8 | 2000 fsyncs for 2000 writes (1.0 writes per fsync) |
| rustlog | fillsync (64 tasks) | 1619 | 38731.8 | 70844.4 | 149553.2 | 611 fsyncs for 20000 writes (32.7 writes per fsync) |
| rustlog | readrandom (16 tasks) | 70399 | 102.6 | 1503.2 | 10207.2 | found 200000/200000, 7% served without leaving the async task, block cache hit 4% |
| rustlog | readmissing (16 tasks) | 2741167 | 2.1 | 32.4 | 526.8 | found 0, bloom skipped 98.9% of table checks, 0.008 block reads per lookup |
| rustlog | readmissing bloom=10 | 1883198 | 1.4 | 190.0 | 900.1 | 250000 keys, no block cache: 0.02 block reads per lookup |
| rustlog | readmissing bloom=0 | 63599 | 125.2 | 1582.1 | 4878.3 | 250000 keys, no block cache: 2.45 block reads per lookup |
| rustlog | scan 100 (1 task) | 4660 | 159.1 | 1165.3 | 4800.5 | 200000 entries |
| rocksdb | fillseq (1 thread) | 53832 | 13.3 | 73.8 | 196.9 | write amp 2.13, levels 2/0/0/0/0/1/33 |
| rocksdb | fillrandom (16 threads) | 58922 | 214.1 | 803.3 | 10379.3 | write amp 5.75, space amp 1.01, stalled 0.0s, +0.9s to settle, levels 1/0/0/0/0/5/58 |
| rocksdb | fillsync (1 threads) | 5941 | 152.3 | 355.8 | 778.8 | 2000 WAL syncs |
| rocksdb | fillsync (64 threads) | 51302 | 1161.2 | 2177.0 | 3706.9 | 632 WAL syncs |
| rocksdb | readrandom (16 threads) | 251407 | 30.1 | 262.9 | 6086.7 | found 200000/200000 |
| rocksdb | readmissing (16 threads) | 981715 | 5.1 | 28.1 | 425.5 | found 0, bloom useful 510479 |

## Runtime blocking probe

Tokio runtime with 2 worker threads. 64 tasks do random gets with no block cache and `F_NOCACHE` set. On macOS, `F_NOCACHE` doesn't evict pages that are already cached, and the DB was just written, so the "fast reads" rows mostly hit the OS cache (inline mode did ~107k reads/s with only 2 in flight, about 19 us each). The "slow reads" rows add a simulated 1 ms per block read (a sleep in the reading thread), to model a cloud network disk. A plain OS thread pings a task on the runtime every 500 us; that task reads a key from the memtable (pure RAM) and replies. The round trip is how long a request that needs no disk waits while the disk-bound load runs.

| device | mode | disk reads/s | pings | RAM get round trip p50 (us) | p99 (us) | p99.9 (us) | max (us) |
|---|---|---:|---:|---:|---:|---:|---:|
| fast reads (~20 us) | idle (no readers): baseline | 0 | 12522 | 38 | 265 | 1561 | 92144 |
| fast reads (~20 us) | spawn_blocking (RustLog default) | 38307 | 8509 | 450 | 1579 | 8372 | 110690 |
| fast reads (~20 us) | pread on the async worker (anti-pattern) | 107404 | 10655 | 90 | 1148 | 6238 | 107282 |
| slow reads (+1 ms, network disk) | spawn_blocking (RustLog default) | 33758 | 11813 | 80 | 1351 | 8520 | 56001 |
| slow reads (+1 ms, network disk) | pread on the async worker (anti-pattern) | 1086 | 5942 | 598 | 7279 | 29458 | 61506 |
