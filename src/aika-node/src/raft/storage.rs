/// Handles reading and writing Raft persistent state to the local filesystem.
///
/// Raft paper: §5.2, paragraph 1 (persistent state); Figure 2, "Persistent state on all servers".
///
/// Each node writes to its own data directory (`/tmp/inf3203_raft_<node_id>/`).
/// This directory must be local, not on distributed fs (can't use distributed fs for communication).
///
/// The log is stored in NDJSON format (one JSON object per line) so that new
/// entries can be appended without rewriting the entire file.  A full rewrite
/// is only needed after a log truncation (rare — only on leader failover with
/// conflicting entries).
use std::fs;
use std::io::{BufRead, Write};
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use super::log::LogEntry;
use super::state::PersistentState;

pub struct RaftStorage {
    data_dir: PathBuf,
}

impl RaftStorage {
    /// Open (or create) a storage directory for this node.
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&data_dir)?;
        Ok(Self { data_dir })
    }

    /// Persist `current_term` and `voted_for` to stable storage.
    ///
    /// **Must be called before responding to any RPC that mutates these fields.**
    /// Uses a write-then-rename pattern to avoid partial writes.
    pub fn save_persistent_state(&self, state: &PersistentState) -> Result<()> {
        let path = self.data_dir.join("persistent_state.json");
        let tmp = self.data_dir.join("persistent_state.json.tmp");

        serde_json::to_writer(fs::File::create(&tmp)?, state)?;
        fs::rename(tmp, path)?;

        Ok(())
    }

    /// Load `PersistentState` from disk, or return `None` on a fresh node.
    pub fn load_persistent_state(&self) -> Result<Option<PersistentState>> {
        let path = self.data_dir.join("persistent_state.json");

        if !path.exists() {
            return Ok(None);
        }

        let state: PersistentState = serde_json::from_reader(fs::File::open(path)?)?;
        Ok(Some(state))
    }

    /// Persist the full log to stable storage (NDJSON: one entry per line).
    ///
    /// Used after log truncation or when starting fresh. Uses write-then-rename
    /// to avoid partial writes.
    pub fn save_log<C: Serialize>(&self, entries: &[LogEntry<C>]) -> Result<()> {
        let path = self.data_dir.join("persistent_log.ndjson");
        let tmp = self.data_dir.join("persistent_log.ndjson.tmp");

        let mut file = std::io::BufWriter::new(fs::File::create(&tmp)?);
        for entry in entries {
            serde_json::to_writer(&mut file, entry)?;
            file.write_all(b"\n")?;
        }
        file.flush()?;
        drop(file);
        fs::rename(tmp, path)?;

        Ok(())
    }

    /// Append new entries to the existing log file (NDJSON).
    ///
    /// Used on the leader propose path (single entry) and on the follower
    /// append path when there is no log conflict.  Much cheaper than a full
    /// rewrite — O(new entries) instead of O(total log).
    pub fn append_log_entries<C: Serialize>(&self, entries: &[LogEntry<C>]) -> Result<()> {
        let path = self.data_dir.join("persistent_log.ndjson");

        let mut file = std::io::BufWriter::new(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?,
        );
        for entry in entries {
            serde_json::to_writer(&mut file, entry)?;
            file.write_all(b"\n")?;
        }
        file.flush()?;

        Ok(())
    }

    /// Load the log from disk, or return an empty vec on a fresh node.
    ///
    /// Handles both the legacy JSON-array format and the current NDJSON format.
    pub fn load_log<C: for<'de> Deserialize<'de>>(&self) -> Result<Vec<LogEntry<C>>> {
        // Try NDJSON first (current format).
        let ndjson_path = self.data_dir.join("persistent_log.ndjson");
        if ndjson_path.exists() {
            let file = fs::File::open(&ndjson_path)?;
            let reader = std::io::BufReader::new(file);
            let mut entries = Vec::new();
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                entries.push(serde_json::from_str(&line)?);
            }
            return Ok(entries);
        }

        // Fall back to legacy JSON-array format.
        let json_path = self.data_dir.join("persistent_log.json");
        if json_path.exists() {
            let entries: Vec<LogEntry<C>> = serde_json::from_reader(fs::File::open(&json_path)?)?;
            return Ok(entries);
        }

        Ok(Vec::new())
    }
}