/// Replicated log — the central data structure of Raft.
///
/// Every command submitted to the cluster is appended here as a `LogEntry`
/// before it is replicated to followers and eventually applied to the state
/// machine.  The log is 1-indexed in the Raft paper; index 0 is used
/// internally as a sentinel "no entry" value (e.g. `matchIndex` starts at 0).
use serde::{Deserialize, Serialize};

use super::state::{LogIndex, Term};

// region LogEntry

/// A single record in the replicated log.
///
/// `C` is the application-defined command type (e.g. a key-value operation).
/// It must be serializable so it can be sent over the network in
/// `AppendEntries` RPCs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry<C> {
    /// The Raft term in which the leader created this entry.
    pub term: Term,

    /// 1-based position in the log.  Stored redundantly for convenience;
    /// must equal the entry's actual position.
    pub index: LogIndex,

    /// The application command to be applied to the state machine once
    /// this entry is committed.
    pub command: C,
}

// endregion

// region RaftLog

/// Manages the ordered sequence of `LogEntry` values for one Raft node.
///
/// Entries are stored in a `Vec` with a 1-based logical index: the entry at
/// logical index `i` lives at `entries[i - 1]`.  The vec is always kept
/// in-order and append-only from the perspective of committed entries.
///
/// ### Thread-safety
/// `RaftLog` itself is **not** `Sync`.  Callers must hold the
/// `tokio::sync::Mutex<RaftState>` (which owns or borrows the log) before
/// accessing it.
#[derive(Debug, Default)]
pub struct RaftLog<C> {
    /// Backing storage.  Index 0 in the vec == log index 1.
    entries: Vec<LogEntry<C>>,
}

impl<C: Clone + std::fmt::Debug> RaftLog<C> {
    /// Create an empty log.
    pub fn new() -> Self {
        RaftLog {
            entries: Vec::new(),
        }
    }

    /// Reconstruct a log from previously persisted entries (recovery path).
    pub fn from_entries(entries: Vec<LogEntry<C>>) -> Self {
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(
                entry.index,
                (i + 1) as LogIndex,
                "log entry at vec position {i} has index {} (expected {})",
                entry.index,
                i + 1,
            );
        }
        RaftLog { entries }
    }

    // region Queries

    /// Return the index of the last entry, or 0 if the log is empty.
    pub fn last_index(&self) -> LogIndex {
        self.entries.len() as LogIndex
    }

    /// Return the term of the last entry, or 0 if the log is empty.
    pub fn last_term(&self) -> Term {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// Look up the entry at `index` (1-based).  Returns `None` when
    /// `index == 0` or `index > last_index()`.
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry<C>> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    /// Return a slice of entries in the half-open range `[from, to)`.
    ///
    /// Used by the leader to build the `entries` field of `AppendEntries`.
    /// Clamps silently if `to` exceeds the last index.
    pub fn slice(&self, from: LogIndex, to: LogIndex) -> &[LogEntry<C>] {
        if from == 0 || from > self.last_index() {
            return &[];
        }
        let start = (from - 1) as usize;
        let end = std::cmp::min(to.saturating_sub(1) as usize, self.entries.len());
        if start >= end {
            return &[];
        }
        &self.entries[start..end]
    }

    /// Return a cloned `Vec` of all entries starting at `from` (inclusive).
    ///
    /// Convenience wrapper around `slice` for sending over the network.
    pub fn entries_from(&self, from: LogIndex) -> Vec<LogEntry<C>> {
        self.slice(from, self.last_index() + 1).to_vec()
    }

    /// Check whether the caller's log is **at least as up-to-date** as this
    /// log, per the Raft election restriction.
    ///
    /// Raft paper: §5.4.1, paragraph 2.
    ///
    /// Returns `true` when:
    ///   - `candidate_last_term > self.last_term()`, OR
    ///   - terms are equal AND `candidate_last_index >= self.last_index()`.
    pub fn is_at_least_as_up_to_date(
        &self,
        candidate_last_index: LogIndex,
        candidate_last_term: Term,
    ) -> bool {
        (candidate_last_term, candidate_last_index) >= (self.last_term(), self.last_index())
    }

    // endregion

    // region Mutations

    /// Append a new entry to the log (leader path).
    ///
    /// The caller must set `entry.index` to `self.last_index() + 1` before
    /// calling, or use `append_command` which does this automatically.
    pub fn append(&mut self, entry: LogEntry<C>) {
        assert_eq!(
            entry.index,
            self.last_index() + 1,
            "append: entry.index {} != expected {}",
            entry.index,
            self.last_index() + 1,
        );
        self.entries.push(entry);
    }

    /// Convenience: build and append an entry from a raw command and the
    /// current term.  Returns the index assigned to the new entry.
    pub fn append_command(&mut self, term: Term, command: C) -> LogIndex {
        let index = self.last_index() + 1;
        let entry = LogEntry {
            term,
            index,
            command,
        };
        self.append(entry);
        index
    }

    /// Truncate the log back to `new_last_index`, discarding all entries
    /// after that position.
    ///
    /// Used by followers when an `AppendEntries` reveals a conflict:
    /// entries after `prev_log_index` that disagree with the leader must be
    /// deleted before appending the leader's entries.
    pub fn truncate_from(&mut self, new_last_index: LogIndex) {
        self.entries.truncate(new_last_index as usize);
    }

    /// Append a batch of entries received from the leader (follower path).
    ///
    /// Raft paper: Figure 2, "AppendEntries RPC", "Receiver implementation" steps 3–4.
    ///
    /// Implements the Raft conflict-detection rule:
    ///   1. For each incoming entry, if an existing entry at the same index
    ///      has a **different term**, truncate the log from that point.
    ///   2. Append any entries that follow the now-truncated (or intact) log.
    ///
    /// Returns `(truncated, new_entries)`:
    ///   - `truncated`: whether a conflicting suffix was removed (requires full rewrite to disk).
    ///   - `new_entries`: the entries that were actually appended (for incremental persistence).
    pub fn append_entries_from_leader(
        &mut self,
        _prev_log_index: LogIndex,
        entries: Vec<LogEntry<C>>,
    ) -> (bool, Vec<LogEntry<C>>) {
        let mut truncated = false;
        let mut new_entries = Vec::new();
        for entry in entries {
            match self.get(entry.index) {
                Some(existing) if existing.term == entry.term => {
                    // Already have this entry with the same term — skip.
                    continue;
                }
                Some(_) => {
                    // Conflict: existing entry at same index has different term.
                    // Truncate from here and fall through to append.
                    self.truncate_from(entry.index - 1);
                    truncated = true;
                }
                None => {
                    // No entry at this index — fall through to append.
                }
            }
            new_entries.push(entry.clone());
            self.entries.push(entry);
        }
        (truncated, new_entries)
    }

    // endregion
}

// endregion
