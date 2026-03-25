/// Log replication — leader-side logic.
///
/// Once elected, a leader must:
///   1. Accept commands from clients, append them to its own log, and reply
///      once the entry is committed (replicated on a majority).
///   2. Continuously replicate its log to all followers via `AppendEntries`
///      RPCs, both for new entries and as periodic heartbeats.
///   3. Advance `commit_index` when a majority of nodes have matched a given
///      index, then apply newly committed entries to the state machine.
///
/// The replication loop for each peer runs as an independent `tokio` task so
/// that a slow or partitioned follower does not block the others.
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, Notify, mpsc};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::{
    RaftError,
    log::RaftLog,
    rpc::{AppendEntriesArgs, AppendEntriesReply, RaftTransport},
    state::{NodeId, RaftState},
    storage::RaftStorage,
};

// region Configuration

/// Tunable parameters for the replication subsystem.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// How often the leader sends heartbeats when there is nothing new to
    /// replicate.  Must be well below the election timeout minimum.
    pub heartbeat_interval: Duration,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        ReplicationConfig {
            heartbeat_interval: Duration::from_millis(50),
        }
    }
}

// endregion

// region Per-peer replication task

/// Spawn one replication task per peer and return a `Notify` that can be
/// used to wake all tasks when new entries are appended.
///
/// Call this immediately after becoming leader.  Each spawned task runs
/// `replicate_to_peer` in a loop until the `shutdown_rx` channel is closed.
///
/// `match_notify` — shared `Notify` that is signaled from `replicate_to_peer`
/// whenever a peer advances its `match_index`, so `advance_commit_index` can
/// be re-evaluated.
pub async fn start_replication_tasks<C, T>(
    state: Arc<Mutex<RaftState>>,
    log: Arc<Mutex<RaftLog<C>>>,
    transport: Arc<T>,
    storage: Arc<RaftStorage>,
    config: ReplicationConfig,
    commit_notify: Arc<Notify>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Arc<Notify>
where
    C: Clone
        + Send
        + Sync
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + std::fmt::Debug
        + 'static,
    T: RaftTransport<C> + Send + Sync + 'static,
{
    // Lock state so that we can clone peer list
    let peers = {
        let state_guard = state.lock().await;
        if state_guard.role != super::state::Role::Leader {
            panic!("start_replication_tasks called when not leader");
        }
        state_guard
            .leader_state
            .as_ref()
            .unwrap()
            .peers
            .keys()
            .cloned()
            .collect::<Vec<_>>()
    };
    // Lock is released here — no locks held for the rest of this function.

    // Create a new Notify (wrapped in Arc for thread safety)
    let new_entry_notify = Arc::new(Notify::new());

    // Spawn one task per peer. Each task owns its own clones of
    // the shared handles so it never contends with sibling tasks on the Arcs
    // themselves, and it never holds a lock while sleeping or doing I/O.
    for peer in peers {
        let state = Arc::clone(&state);
        let log = Arc::clone(&log);
        let transport = Arc::clone(&transport);
        let storage = Arc::clone(&storage);
        let commit_notify = Arc::clone(&commit_notify);
        let notify = Arc::clone(&new_entry_notify);
        let config = config.clone();
        let mut shutdown = shutdown_rx.clone();

        tokio::spawn(async move {
            loop {
                // Wait for a new entry signal OR a heartbeat tick, whichever
                // comes first. We do NOT hold any lock here.
                tokio::select! {
                    _ = notify.notified() => {}
                    _ = sleep(config.heartbeat_interval) => {}
                    _ = shutdown.changed() => {}
                }

                // Check shutdown after waking for any reason.
                if *shutdown.borrow() {
                    debug!("replication task for {peer} shutting down");
                    break;
                }

                if let Err(e) = replicate_to_peer(
                    &peer,
                    Arc::clone(&state),
                    Arc::clone(&log),
                    Arc::clone(&transport),
                    Arc::clone(&storage),
                    Arc::clone(&commit_notify),
                )
                .await
                {
                    warn!("replication to {peer} failed: {e}");
                }
            }
        });
    }

    new_entry_notify
}

// endregion

// region Send AppendEntries

/// Execute one round of `AppendEntries` toward `peer`.
///
/// Raft paper: Figure 2, "Rules for Servers", "Leaders" bullets 2–3.
///
/// This is the core replication routine called repeatedly by the per-peer task.
/// It handles both normal log replication and the case where the follower's log
/// is behind or has conflicts.
///
/// Steps:
///   1. Lock state; verify we are still leader; read `next_index` for `peer`.
///   2. Lock log; build `AppendEntriesArgs`:
///        - `prev_log_index = next_index - 1`
///        - `prev_log_term  = log.get(prev_log_index).map(|e| e.term).unwrap_or(0)`
///        - `entries        = log.entries_from(next_index)` (may be empty)
///        - `leader_commit  = volatile.commit_index`
///   3. Release both locks, call `transport.send_append_entries`.
///   4. On success (`reply.success == true`):
///        - Lock state, update `match_index[peer] = prev_log_index + entries.len()`,
///          update `next_index[peer] = match_index[peer] + 1`.
///        - Signal `commit_notify` so `advance_commit_index` runs.
///   5. On failure (`reply.success == false`):
///        - If `reply.term > current_term`, step down to follower.
///        - Otherwise decrement `next_index[peer]` (or jump to
///          `reply.conflict_index` if present) and return — caller retries.
///   6. On transport error: log a warning and return; caller retries after
///      the heartbeat interval.
pub async fn replicate_to_peer<C, T>(
    peer: &NodeId,
    state: Arc<Mutex<RaftState>>,
    log: Arc<Mutex<RaftLog<C>>>,
    transport: Arc<T>,
    storage: Arc<RaftStorage>,
    commit_notify: Arc<Notify>,
) -> Result<(), RaftError>
where
    C: Clone
        + Send
        + Sync
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + std::fmt::Debug
        + 'static,
    T: RaftTransport<C>,
{
    // 1. Lock state; verify leader; read next_index for this peer.
    let (term, leader_id, next_index, commit_index) = {
        let state_guard = state.lock().await;
        if !state_guard.is_leader() {
            return Err(RaftError::NotLeader);
        }
        let leader_state = state_guard.leader_state.as_ref().unwrap();
        let peer_state = leader_state
            .peers
            .get(peer)
            .ok_or_else(|| RaftError::Internal(format!("unknown peer {peer}")))?;
        (
            state_guard.persistent.current_term,
            state_guard.id.clone(),
            peer_state.next_index,
            state_guard.volatile.commit_index,
        )
    };

    // 2. Lock log; build AppendEntriesArgs.
    let (args, entries_count) = {
        let log_guard = log.lock().await;
        let prev_log_index = next_index - 1;
        let prev_log_term = log_guard.get(prev_log_index).map(|e| e.term).unwrap_or(0);
        let entries = log_guard.entries_from(next_index);
        let count = entries.len() as u64;
        let args = AppendEntriesArgs {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: commit_index,
        };
        (args, count)
    };

    // 3. Send AppendEntries RPC (no locks held).
    let reply = match transport.send_append_entries(peer, args.clone()).await {
        Ok(reply) => reply,
        Err(e) => {
            warn!("transport error replicating to {peer}: {e}");
            return Err(RaftError::Transport(e));
        }
    };

    // 4. On success: update match_index and next_index; notify commit_notify.
    if reply.success {
        let new_match_index = args.prev_log_index + entries_count;
        let mut state_guard = state.lock().await;
        if let Some(ref mut leader_state) = state_guard.leader_state {
            if let Some(peer_state) = leader_state.peers.get_mut(peer) {
                peer_state.match_index = new_match_index;
                peer_state.next_index = new_match_index + 1;
            }
        }
        drop(state_guard);
        commit_notify.notify_one();
        return Ok(());
    }

    // 5. On failure: step down if higher term, else backtrack next_index.
    if reply.term > term {
        let mut state_guard = state.lock().await;
        state_guard.step_down_to_follower(&storage, reply.term);
        info!(
            "stepped down to follower: peer {peer} has higher term {}",
            reply.term
        );
        return Ok(());
    }

    // Fast log backtracking: the follower already computed the right index to
    // jump to (`conflict_index`).  Use it directly instead of decrementing
    // one step at a time.  `conflict_term` was used by the follower to find
    // that index; the leader doesn't need to re-derive it.
    // Falls back to decrement-by-one when no hint is available.
    {
        let mut state_guard = state.lock().await;
        if let Some(ref mut leader_state) = state_guard.leader_state {
            if let Some(peer_state) = leader_state.peers.get_mut(peer) {
                peer_state.next_index = match reply.conflict_index {
                    Some(ci) => ci.max(1),
                    None => peer_state.next_index.saturating_sub(1).max(1),
                };
            }
        }
    }

    Ok(())
}

// endregion

// region Heartbeat

/// Broadcast an empty `AppendEntries` (heartbeat) to every peer concurrently.
///
/// Raft paper: §5.2, paragraph 4.
///
/// Used immediately after becoming leader and then on each heartbeat tick.
/// Spawns one short-lived task per peer; does not wait for all replies.
/// Replies that contain a higher term cause a step-down to Follower.
pub async fn broadcast_heartbeat<C, T>(
    state: Arc<Mutex<RaftState>>,
    log: Arc<Mutex<RaftLog<C>>>,
    transport: Arc<T>,
    storage: Arc<RaftStorage>,
) where
    C: Clone
        + Send
        + Sync
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + std::fmt::Debug
        + 'static,
    T: RaftTransport<C>,
{
    // Snapshot state + log while holding locks.
    let (term, leader_id, peers, commit_index, last_log_index, last_log_term) = {
        let state_guard = state.lock().await;
        let log_guard = log.lock().await;
        let peers: Vec<NodeId> = state_guard.peers.clone();
        (
            state_guard.persistent.current_term,
            state_guard.id.clone(),
            peers,
            state_guard.volatile.commit_index,
            log_guard.last_index(),
            log_guard.last_term(),
        )
    };

    // For each peer, spawn a task to send the heartbeat.
    for peer in peers {
        let transport = Arc::clone(&transport);
        let state = Arc::clone(&state);
        let storage = Arc::clone(&storage);
        let leader_id = leader_id.clone();

        let args = AppendEntriesArgs {
            term,
            leader_id,
            prev_log_index: last_log_index,
            prev_log_term: last_log_term,
            entries: vec![],
            leader_commit: commit_index,
        };

        tokio::spawn(async move {
            match transport.send_append_entries(&peer, args).await {
                Ok(reply) => {
                    if reply.term > term {
                        let mut state_guard = state.lock().await;
                        if reply.term > state_guard.persistent.current_term {
                            state_guard.step_down_to_follower(&storage, reply.term);
                            info!(
                                "stepped down to follower: heartbeat reply from {peer} has higher term {}",
                                reply.term
                            );
                        }
                    }
                }
                Err(e) => {
                    debug!("heartbeat to {peer} failed: {e}");
                }
            }
        });
    }
}

// endregion

// region Advance commit index

/// Advance `commit_index` if a new log index has been replicated on a
/// majority of servers, then trigger application to the state machine.
///
/// Raft paper: Figure 2, "Rules for Servers", "Leaders" bullet 4; §5.3, paragraph 4; §5.4.2.
///
/// Must be called after every successful `AppendEntries` reply that updated
/// a peer's `match_index`.
///
/// Algorithm (Raft §5.3 + §5.4 Leader Completeness):
///   For N from `last_log_index` down to `commit_index + 1`:
///     - Count peers whose `match_index >= N`.
///     - If count + 1 (self) > cluster_size / 2, AND `log[N].term == current_term`:
///         set `commit_index = N` and break.
///
/// The "same term" check prevents the leader from committing entries from
/// *previous* terms by replication alone (the no-op entry ensures they
/// eventually get committed indirectly).
pub async fn advance_commit_index<C>(
    state: Arc<Mutex<RaftState>>,
    log: Arc<Mutex<RaftLog<C>>>,
    apply_tx: mpsc::Sender<super::ApplyMsg<C>>,
) where
    C: Clone + Send + Sync + std::fmt::Debug + 'static,
{
    {
        let mut state_guard = state.lock().await;
        let log_guard = log.lock().await;

        let current_term = state_guard.persistent.current_term;
        let commit_index = state_guard.volatile.commit_index;
        let last_log_index = log_guard.last_index();

        let leader_state = match state_guard.leader_state.as_ref() {
            Some(ls) => ls,
            None => return, // not leader
        };

        // Total cluster size = peers + self
        let cluster_size = leader_state.peers.len() + 1;

        // Iterate N from last_log_index down to commit_index + 1
        for n in (commit_index + 1..=last_log_index).rev() {
            // Only commit entries from the current term (§5.4)
            if let Some(entry) = log_guard.get(n) {
                if entry.term != current_term {
                    continue;
                }
            } else {
                continue;
            }

            // Count peers whose match_index >= N
            let replication_count = leader_state
                .peers
                .values()
                .filter(|ps| ps.match_index >= n)
                .count();

            // +1 for self (leader always has the entry)
            if replication_count + 1 > cluster_size / 2 {
                state_guard.volatile.commit_index = n;
                debug!("advanced commit_index to {n}");
                break;
            }
        }
    }
    // Locks released — now apply any newly committed entries.
    apply_committed_entries(state, log, apply_tx).await;
}

// endregion

// region Apply commited entries

/// Apply all log entries in `(last_applied, commit_index]` to the state
/// machine in order, advancing `last_applied` as we go.
///
/// Raft paper: Figure 2, "Rules for Servers", "All Servers" bullet 1.
///
/// Sends each entry as an `ApplyMsg` on `apply_tx`.  The receiver (the state
/// machine goroutine / task) processes commands in the order they arrive.
///
/// This function is idempotent: calling it when `last_applied == commit_index`
/// is a no-op.
///
/// Should also be called by followers whenever they advance their
/// `commit_index` (via the `leader_commit` field in `AppendEntries`).
pub async fn apply_committed_entries<C>(
    state: Arc<Mutex<RaftState>>,
    log: Arc<Mutex<RaftLog<C>>>,
    apply_tx: mpsc::Sender<super::ApplyMsg<C>>,
) where
    C: Clone + Send + Sync + std::fmt::Debug + 'static,
{
    // Collect entries to apply while holding locks, then release before sending.
    let entries_to_apply = {
        let mut state_guard = state.lock().await;
        let log_guard = log.lock().await;

        let last_applied = state_guard.volatile.last_applied;
        let commit_index = state_guard.volatile.commit_index;

        if last_applied >= commit_index {
            return;
        }

        let mut entries = Vec::new();
        for idx in (last_applied + 1)..=commit_index {
            if let Some(entry) = log_guard.get(idx) {
                entries.push((idx, entry.command.clone()));
            }
        }

        // Advance last_applied
        state_guard.volatile.last_applied = commit_index;
        entries
    };

    // Send each entry on the apply channel (no locks held).
    for (index, command) in entries_to_apply {
        if let Err(e) = apply_tx
            .send(super::ApplyMsg::Command { index, command })
            .await
        {
            warn!("failed to send ApplyMsg for index {index}: {e}");
            break;
        }
    }
}

// endregion

// region Follower

/// Validate and apply an `AppendEntries` RPC on a follower (or candidate).
///
/// Raft paper: Figure 2, "AppendEntries RPC", "Receiver implementation" steps 1–5.
///
/// Returns the `AppendEntriesReply` to send back to the leader.
///
/// Steps (Raft §5.1–5.3):
///   1. If `args.term < current_term` → reject (`success: false`).
///   2. Otherwise reset the election timer (call provided callback).
///   3. If `args.term > current_term` → step down to follower in new term.
///   4. Update `volatile.current_leader = Some(args.leader_id)`.
///   5. Log consistency check: if `prev_log_index > 0` and
///      `log.get(prev_log_index)` is absent or has wrong term → reject with
///      `conflict_index` / `conflict_term` hints.
///   6. Append entries: call `log.append_entries_from_leader`.
///   7. Advance `commit_index` to `min(args.leader_commit, last_new_entry_index)`.
///   8. Trigger `apply_committed_entries` if `commit_index` advanced.
///   9. Return `AppendEntriesReply { term: current_term, success: true, … }`.
pub async fn handle_append_entries<C>(
    args: AppendEntriesArgs<C>,
    state: Arc<Mutex<RaftState>>,
    log: Arc<Mutex<RaftLog<C>>>,
    storage: Arc<RaftStorage>,
    apply_tx: mpsc::Sender<super::ApplyMsg<C>>,
    reset_election_timer: impl Fn() + Send,
) -> AppendEntriesReply
where
    C: Clone
        + Send
        + Sync
        + std::fmt::Debug
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>
        + 'static,
{
    let commit_advanced;
    let current_term;

    {
        let mut state_guard = state.lock().await;

        // 1. Reject if stale term.
        if args.term < state_guard.persistent.current_term {
            return AppendEntriesReply {
                term: state_guard.persistent.current_term,
                success: false,
                conflict_index: None,
                conflict_term: None,
            };
        }

        // 2. Valid term — reset election timer.
        reset_election_timer();

        // 3. If higher term, step down.
        if args.term > state_guard.persistent.current_term {
            state_guard.step_down_to_follower(&storage, args.term);
        }

        // Also step down from candidate if same term (leader exists).
        if state_guard.role == super::state::Role::Candidate {
            state_guard.role = super::state::Role::Follower;
            state_guard.leader_state = None;
        }

        // 4. Record the leader.
        state_guard.volatile.current_leader = Some(args.leader_id.clone());

        current_term = state_guard.persistent.current_term;

        // 5. Log consistency check.
        let mut log_guard = log.lock().await;

        if args.prev_log_index > 0 {
            match log_guard.get(args.prev_log_index) {
                None => {
                    // We don't have an entry at prev_log_index.
                    // Hint: the follower's last index + 1 is where the leader should retry.
                    return AppendEntriesReply {
                        term: current_term,
                        success: false,
                        conflict_index: Some(log_guard.last_index() + 1),
                        conflict_term: None,
                    };
                }
                Some(entry) if entry.term != args.prev_log_term => {
                    // Term mismatch — find first index of the conflicting term.
                    let conflict_term = entry.term;
                    let mut conflict_index = args.prev_log_index;
                    while conflict_index > 1 {
                        if let Some(prev) = log_guard.get(conflict_index - 1) {
                            if prev.term == conflict_term {
                                conflict_index -= 1;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    return AppendEntriesReply {
                        term: current_term,
                        success: false,
                        conflict_index: Some(conflict_index),
                        conflict_term: Some(conflict_term),
                    };
                }
                _ => {
                    // prev_log_index exists and term matches — consistency OK.
                }
            }
        }

        // 6. Append entries from leader.
        let (truncated, new_entries) =
            log_guard.append_entries_from_leader(args.prev_log_index, args.entries);

        // Persist the updated log before advancing commit_index.
        // Raft §8: a follower must durably record entries before acknowledging
        // AppendEntries, so a crash-restart cannot lose entries the leader
        // believes are safely replicated.
        if !new_entries.is_empty() {
            let persist_result = if truncated {
                // Conflict caused truncation — must rewrite the full log.
                let all_entries = log_guard.entries_from(1);
                storage.save_log(&all_entries)
            } else {
                // No conflict — append only the new entries (O(new) not O(total)).
                storage.append_log_entries(&new_entries)
            };
            if let Err(e) = persist_result {
                warn!("follower log persist failed: {e}");
                return AppendEntriesReply {
                    term: current_term,
                    success: false,
                    conflict_index: None,
                    conflict_term: None,
                };
            }
        }

        // 7. Advance commit_index.
        let last_new_entry_index = log_guard.last_index();
        let old_commit = state_guard.volatile.commit_index;
        if args.leader_commit > old_commit {
            state_guard.volatile.commit_index =
                std::cmp::min(args.leader_commit, last_new_entry_index);
        }
        commit_advanced = state_guard.volatile.commit_index > old_commit;
    }

    // 8. Apply committed entries if commit_index moved (locks released).
    if commit_advanced {
        apply_committed_entries(Arc::clone(&state), Arc::clone(&log), apply_tx).await;
    }

    // 9. Return success.
    AppendEntriesReply {
        term: current_term,
        success: true,
        conflict_index: None,
        conflict_term: None,
    }
}

// endregion
