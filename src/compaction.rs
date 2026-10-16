//! Leveled compaction, on its own thread.
//!
//! Shape of the tree:
//! - L0: one file per flush. Files may overlap, so a read checks all of them.
//! - L1..Ln: each level is sorted and non-overlapping, so a read checks at
//!   most one file per level. Each level may hold `level_multiplier` times
//!   more bytes than the one above.
//!
//! Why leveled and not size-tiered? Size-tiered writes less (lower write
//! amplification) but leaves many overlapping runs per tier, so a read may
//! check many files and space use can temporarily double. Leveled keeps
//! read amplification at ~1 file per level and space overhead ~10%, at the
//! cost of rewriting data more often. That's the right trade for reads.
//!
//! Picking: score each level (L0 by file count, others by bytes / budget),
//! compact the worst one if its score is >= 1.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::engine::{Inner, SuperVersion};
use crate::error::{Error, Result};
use crate::io;
use crate::iterator::{InternalIterator, MergingIterator};
use crate::key::{self, ValueKind};
use crate::manifest::VersionEdit;
use crate::options::Options;
use crate::sstable::{Table, TableBuilder};
use crate::stats;
use crate::version::{FileMeta, Version};

/// One unit of compaction work.
#[derive(Debug)]
pub(crate) struct Compaction {
    pub level: usize,
    /// Files from `level`.
    pub inputs0: Vec<Arc<Table>>,
    /// Overlapping files from `level + 1`.
    pub inputs1: Vec<Arc<Table>>,
    pub score: f64,
}

impl Compaction {
    fn output_level(&self) -> usize {
        self.level + 1
    }

    /// A single file with nothing below it to merge with can just be
    /// relinked one level down in the MANIFEST. No bytes rewritten.
    fn is_trivial_move(&self) -> bool {
        self.inputs0.len() == 1 && self.inputs1.is_empty()
    }
}

fn user_range(files: &[Arc<Table>]) -> Option<(Bytes, Bytes)> {
    let lo = files.iter().map(|t| t.meta.smallest_user()).min()?;
    let hi = files.iter().map(|t| t.meta.largest_user()).max()?;
    Some((Bytes::copy_from_slice(lo), Bytes::copy_from_slice(hi)))
}

/// Picks the most urgent compaction, or `None` if every level is in budget.
pub(crate) fn pick(v: &Version, opts: &Options, compact_pointer: &[Option<Bytes>]) -> Option<Compaction> {
    let last = v.levels.len().checked_sub(1)?;
    let mut best: Option<(usize, f64)> = None;
    for level in 0..last {
        let score = if level == 0 {
            v.levels[0].len() as f64 / opts.l0_compaction_trigger as f64
        } else {
            v.level_bytes(level) as f64 / opts.max_bytes_for_level(level) as f64
        };
        if score >= 1.0 && best.is_none_or(|(_, s)| score > s) {
            best = Some((level, score));
        }
    }
    let (level, score) = best?;

    let inputs0: Vec<Arc<Table>> = if level == 0 {
        // L0 files overlap each other, so take them all. Taking only some
        // could move an older version of a key below a newer one left in L0.
        v.levels[0].clone()
    } else {
        // Round-robin through the key space: the first file after where
        // the last compaction of this level stopped. Without this, a hot
        // key range at the start would be compacted over and over.
        let files = &v.levels[level];
        let start = compact_pointer
            .get(level)
            .and_then(|p| p.as_ref())
            .map_or(0, |p| files.partition_point(|t| key::compare(&t.meta.largest, p).is_le()));
        let f = files.get(start).or_else(|| files.first())?;
        vec![Arc::clone(f)]
    };
    let (lo, hi) = user_range(&inputs0)?;
    let inputs1 = v.overlapping(level + 1, &lo, &hi);
    Some(Compaction { level, inputs0, inputs1, score })
}

pub(crate) fn run(
    inner: Arc<Inner>,
    rx: crossbeam_channel::Receiver<()>,
    deleter: crossbeam_channel::Receiver<std::path::PathBuf>,
) {
    loop {
        crossbeam_channel::select! {
            recv(rx) -> _ => {},
            recv(deleter) -> p => if let Ok(p) = p { delete_file(&p) },
            default(Duration::from_millis(500)) => {},
        }
        while let Ok(p) = deleter.try_recv() {
            delete_file(&p);
        }
        if inner.shutdown.load(Ordering::Acquire) {
            return;
        }
        while inner.bg_error().is_none() && !inner.shutdown.load(Ordering::Acquire) {
            let picked = {
                let vs = inner.vset.lock();
                pick(&inner.state.load().version, &inner.opts, &vs.compact_pointer)
            };
            let Some(c) = picked else { break };
            match run_one(&inner, &c) {
                Ok(()) => {}
                Err(Error::Closed) => return,
                Err(e) => {
                    inner.set_bg_error(&e);
                    break;
                }
            }
            // Files replaced by that compaction may be droppable right now.
            while let Ok(p) = deleter.try_recv() {
                delete_file(&p);
            }
        }
        inner.bg_progress.send_modify(|v| *v += 1);
    }
}

fn delete_file(p: &std::path::Path) {
    match std::fs::remove_file(p) {
        Ok(()) => tracing::debug!("deleted obsolete {}", p.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::warn!("could not delete {}: {e}", p.display()),
    }
}

fn run_one(inner: &Arc<Inner>, c: &Compaction) -> Result<()> {
    let t0 = Instant::now();
    let out_level = c.output_level();

    if c.is_trivial_move() {
        let f = &c.inputs0[0];
        // Reopen under the same number at the new level: a new `Table`
        // handle, same file. The old handle is dropped without deleting.
        let moved = inner.open_table(f.meta.clone())?;
        install(
            inner,
            c,
            vec![(out_level, moved)],
            VersionEdit {
                deleted: vec![(c.level, f.number())],
                added: vec![(out_level, f.meta.clone())],
                ..Default::default()
            },
            false,
        )?;
        stats::add(&inner.stats.trivial_moves, 1);
        tracing::debug!("trivial move {:06}.sst L{} -> L{}", f.number(), c.level, out_level);
        return Ok(());
    }

    let version = Arc::clone(&inner.state.load().version);
    // Newest first: L0 files are already newest-first; level N is newer
    // than level N+1.
    let mut children: Vec<Box<dyn InternalIterator>> = Vec::new();
    for t in c.inputs0.iter().chain(c.inputs1.iter()) {
        // fill_cache = false: compaction touches every block once; caching
        // them would flush out the blocks user reads actually need.
        children.push(Box::new(t.iter(false)));
    }
    let mut it = MergingIterator::new(children);
    it.seek_to_first()?;

    let mut outputs: Vec<FileMeta> = Vec::new();
    let mut builder: Option<(u64, TableBuilder)> = None;
    let mut last_user: Vec<u8> = Vec::new();
    let mut have_last = false;
    let mut n = 0u64;

    let res = (|| -> Result<()> {
        while it.valid() {
            n += 1;
            if n.is_multiple_of(4096) && inner.shutdown.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            let ik = it.key();
            let uk = key::user_key(ik);
            if have_last && uk == last_user.as_slice() {
                // An older version of a key we already emitted. No reader can
                // need it: every reader snapshot is >= the newest version's
                // seq (see `Db::get` for why), so drop it.
                it.next()?;
                continue;
            }
            last_user.clear();
            last_user.extend_from_slice(uk);
            have_last = true;

            let (_, kind) = key::seq_kind(ik).ok_or_else(|| Error::corruption("bad key in compaction input"))?;
            if kind == ValueKind::Delete && version.is_bottommost_for(out_level, uk) {
                // Nothing older is left below to hide: the tombstone has done
                // its job. Dropping it is how deletes actually free space.
                it.next()?;
                continue;
            }

            if builder.is_none() {
                let num = inner.new_file_number();
                let o = &inner.opts;
                builder = Some((
                    num,
                    TableBuilder::create(
                        &io::sst_path(&inner.dir, num),
                        o.block_size,
                        o.bloom_bits_per_key,
                        o.fail_points.clone(),
                    )?,
                ));
            }
            if let Some((_, b)) = &mut builder {
                b.add(ik, it.value())?;
                if b.estimated_size() >= inner.opts.target_file_size {
                    // Cut here. Safe at any key boundary: we keep one version
                    // per user key, so a key never spans two files.
                    if let Some((num, b)) = builder.take() {
                        outputs.push(finish(num, b)?);
                    }
                }
            }
            it.next()?;
        }
        if let Some((num, b)) = builder.take() {
            outputs.push(finish(num, b)?);
        }
        io::sync_dir(&inner.dir)?;
        Ok(())
    })();

    if let Err(e) = res {
        // Abandon: remove what we wrote. Inputs are untouched and still live.
        if let Some((num, _)) = builder.take() {
            let _ = std::fs::remove_file(io::sst_path(&inner.dir, num));
        }
        for m in &outputs {
            let _ = std::fs::remove_file(io::sst_path(&inner.dir, m.number));
        }
        return Err(e);
    }

    let mut tables = Vec::with_capacity(outputs.len());
    for m in &outputs {
        tables.push((out_level, inner.open_table(m.clone())?));
    }
    let deleted: Vec<(usize, u64)> = c
        .inputs0
        .iter()
        .map(|t| (c.level, t.number()))
        .chain(c.inputs1.iter().map(|t| (out_level, t.number())))
        .collect();
    let edit = VersionEdit {
        deleted,
        added: outputs.iter().map(|m| (out_level, m.clone())).collect(),
        next_file: Some(inner.next_file.load(Ordering::SeqCst)),
        ..Default::default()
    };
    install(inner, c, tables, edit, true)?;

    let read: u64 = c.inputs0.iter().chain(c.inputs1.iter()).map(|t| t.meta.size).sum();
    let written: u64 = outputs.iter().map(|m| m.size).sum();
    stats::add(&inner.stats.compaction_bytes_read, read);
    stats::add(&inner.stats.compaction_bytes_written, written);
    stats::add(&inner.stats.compactions, 1);
    tracing::debug!(
        "compacted L{} ({} files) + L{} ({} files) -> {} files, {read} -> {written} bytes in {:?} (score {:.2})",
        c.level,
        c.inputs0.len(),
        out_level,
        c.inputs1.len(),
        outputs.len(),
        t0.elapsed(),
        c.score
    );
    Ok(())
}

fn finish(num: u64, b: TableBuilder) -> Result<FileMeta> {
    let built = b.finish()?;
    Ok(FileMeta { number: num, size: built.size, smallest: built.smallest, largest: built.largest })
}

/// Commits a compaction: MANIFEST first (durable), then the in-memory swap
/// (visible), then mark the inputs for deletion.
fn install(
    inner: &Arc<Inner>,
    c: &Compaction,
    added: Vec<(usize, Arc<Table>)>,
    edit: VersionEdit,
    delete_inputs: bool,
) -> Result<()> {
    let mut vs = inner.vset.lock();
    let new_version = inner.state.load().version.apply(&edit.deleted, &added)?;
    if cfg!(debug_assertions) {
        new_version.check_invariants()?;
    }
    vs.manifest.append(&edit)?;
    let new_version = Arc::new(new_version);
    inner.update_state(|cur| SuperVersion {
        mem: Arc::clone(&cur.mem),
        imms: cur.imms.clone(),
        version: Arc::clone(&new_version),
    });
    if let Some(last) = c.inputs0.last() {
        if let Some(p) = vs.compact_pointer.get_mut(c.level) {
            *p = Some(last.meta.largest.clone());
        }
    }
    drop(vs);
    if delete_inputs {
        // The files go away when the last reader holding an old version
        // drops its `Arc<Table>`, not now. See `Table::drop`.
        for t in c.inputs0.iter().chain(c.inputs1.iter()) {
            t.mark_obsolete();
        }
    }
    inner.wake_all();
    Ok(())
}
