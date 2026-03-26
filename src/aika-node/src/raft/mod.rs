pub mod election;
pub mod http_transport;
pub mod log;
pub mod replication;
pub mod rpc;
pub mod state;
pub mod storage;

#[cfg(test)]
mod tests;

use storage::RaftStorage;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify, mpsc};

use crate::common::Command;
use election::{ElectionConfig, ElectionTimer};
use http_transport::HttpTransport;
use log::RaftLog;
use replication::ReplicationConfig;
use state::{LogIndex, NodeId, RaftState, Term};

/// A committed log entry delivered to the application state machine.
///
/// Produced by the Raft core once an entry reaches commit quorum; consumed
/// by the task that drives `StateMachine::apply`.
pub enum ApplyMsg<C> {
    Command { index: LogIndex, command: C },
}

/// Errors that can occur during Raft operations (e.g. proposing a command).
#[derive(Debug, thiserror::Error)]
pub enum RaftError {
    #[error("not leader")]
    NotLeader,

    #[error("transport error: {0}")]
    Transport(#[from] rpc::TransportError),

    #[error("internal error: {0}")]
    Internal(String),
}

/// A handle to a running Raft node.
///
/// This is the main interface for cluster controller nodes to interact with Raft: proposing
/// commands, checking leadership, and registering commit callbacks.
#[derive(Clone)]
pub struct RaftNode {
    /// This node's id
    node_id: u64,

    /// Raft State, shared across all tasks
    state: Arc<Mutex<RaftState>>,

    /// Replicated log, shared across all tasks.
    log: Arc<Mutex<RaftLog<Command>>>,

    /// Maps each peer's `NodeId` (its address string) to a stable `u64`
    /// so that `leader_info` can return a numeric ID for any leader.
    ///
    /// Peers are assigned IDs equal to their 0-based index in the `peers`
    /// slice passed to `new`.  The local node always uses its own `node_id`.
    peer_id_map: HashMap<NodeId, u64>,

    /// Maps every Raft NodeId (numeric string like "1", "2", …) to its
    /// externally-reachable network address (like "c9-5:60875").
    /// Built from the peers list which is ordered by node_id.
    node_id_to_address: HashMap<NodeId, String>,

    /// Submits propose requests to the main Raft event loop.
    ///
    /// Each message carries the command and a one-shot reply channel that is
    /// fulfilled once the entry is committed (or an error occurs).
    propose_tx: mpsc::Sender<(Command, tokio::sync::oneshot::Sender<anyhow::Result<()>>)>,

    /// Callbacks invoked (in order, inside the apply task) whenever a log
    /// entry is committed.  Uses `std::sync::Mutex` so `on_commit` can be
    /// called from synchronous context during node setup.
    commit_callbacks: Arc<std::sync::Mutex<Vec<Box<dyn Fn(Command) + Send + 'static>>>>,

    /// Persistent storage handle — used to durably save term/vote and log.
    storage: Arc<RaftStorage>,

    /// Sender end of the committed-entry channel.  Used internally by
    /// `handle_append_entries` to route follower-applied entries into the
    /// same state machine pipeline as leader commits.
    apply_tx: mpsc::Sender<ApplyMsg<Command>>,

    /// Election timer — reset on every valid heartbeat and vote grant.
    timer: Arc<ElectionTimer>,
}

impl RaftNode {
    /// Create a new Raft node with the given ID, peer addresses, and local data directory.
    ///
    /// `data_dir` must be a node-local path (e.g. `/tmp/inf3203_raft_<node_id>/`).
    /// It must **not** be on a shared/distributed filesystem.
    pub fn new(
        node_id: u64,
        peers: Vec<String>,
        data_dir: std::path::PathBuf,
        election_config: ElectionConfig,
        replication_config: ReplicationConfig,
    ) -> Self {
        let own_node_id: NodeId = node_id.to_string();

        // Assign each peer a stable u64 equal to its index in the input slice.
        let peer_id_map: HashMap<NodeId, u64> = peers
            .iter()
            .enumerate()
            .map(|(i, addr)| (addr.clone(), i as u64))
            .collect();

        // Map numeric NodeId strings ("1", "2", …) to network addresses.
        // peers[i] has node_id = i+1, so its Raft NodeId is "(i+1)".
        let node_id_to_address: HashMap<NodeId, String> = peers
            .iter()
            .enumerate()
            .map(|(i, addr)| ((i as u64 + 1).to_string(), addr.clone()))
            .collect();

        // Peer NodeIds are the raw address strings; base URLs add the http:// scheme
        // so reqwest can form valid endpoints like "http://host:port/raft/request_vote".
        let peer_urls: HashMap<NodeId, String> = peers
            .iter()
            .map(|addr| (addr.clone(), format!("http://{addr}")))
            .collect();

        let blocked_peers = Arc::new(Mutex::new(HashSet::new()));
        let transport = Arc::new(HttpTransport::new(
            own_node_id.clone(),
            peer_urls,
            blocked_peers,
            std::time::Duration::from_millis(200),
        ));

        let peer_node_ids: Vec<NodeId> = peers.clone();
        let storage = Arc::new(RaftStorage::new(data_dir).expect("failed to open raft storage"));

        // Restore persisted state (term, votedFor) before wrapping in Arc<Mutex> so
        // we never need to call blocking_lock() from within an async runtime.
        let mut initial_state = RaftState::new(own_node_id, peer_node_ids);
        if let Some(ps) = storage
            .load_persistent_state()
            .expect("failed to load persistent state")
        {
            initial_state.persistent = ps;
        }
        let raft_state = Arc::new(Mutex::new(initial_state));

        // Restore the replicated log from disk.
        let entries = storage.load_log::<Command>().expect("failed to load log");

        // If a previous crash left a corrupt last line, load_log stops reading
        // there and returns only the valid prefix. But the corrupt line stays in the
        // file — future append_log_entries calls would append *after* it, causing
        // every subsequent restart to lose the same entries again.
        //
        // Fix: rewrite the file to exactly what we loaded. This removes any corrupt
        // trailing data so appends always go to the right place.
        if let Err(e) = storage.save_log(&entries) {
            tracing::warn!("Could not rewrite log file after load: {}", e);
        }

        let initial_log = if entries.is_empty() {
            RaftLog::new()
        } else {
            RaftLog::from_entries(entries)
        };
        let raft_log: Arc<Mutex<RaftLog<Command>>> = Arc::new(Mutex::new(initial_log));

        // --- Channel setup ---------------------------------------------------
        // propose_rx must be moved into the event loop (Receiver is not Clone).
        let (propose_tx, mut propose_rx) =
            mpsc::channel::<(Command, tokio::sync::oneshot::Sender<anyhow::Result<()>>)>(64);

        // Committed entries flow: replication/follower code → apply_tx → event loop.
        let (apply_tx, mut apply_rx) = mpsc::channel::<ApplyMsg<Command>>(256);

        // Election timer fires on this channel to trigger a new election.
        let (election_timeout_tx, mut election_timeout_rx) = mpsc::channel::<()>(1);

        // Vote replies from concurrent RequestVote RPCs arrive here.
        let (vote_reply_tx, mut vote_reply_rx) =
            mpsc::channel::<(NodeId, Term, rpc::RequestVoteReply)>(32);

        // Replication tasks signal here when a peer's match_index advances.
        let commit_notify = Arc::new(Notify::new());

        // --- Election timer --------------------------------------------------
        let timer = Arc::new(ElectionTimer::start(election_config, election_timeout_tx));

        // --- Commit callbacks (created before event loop so both sides can hold it) ---
        type CbVec = Vec<Box<dyn Fn(Command) + Send + 'static>>;
        let commit_callbacks: Arc<std::sync::Mutex<CbVec>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        // --- Clone arcs for the event loop task ------------------------------
        let el_state = Arc::clone(&raft_state);
        let el_log = Arc::clone(&raft_log);
        let el_transport = Arc::clone(&transport);
        let el_storage = Arc::clone(&storage);
        let el_callbacks = Arc::clone(&commit_callbacks);
        let el_apply_tx = apply_tx.clone();
        let el_commit_notify = Arc::clone(&commit_notify);
        let el_timer = Arc::clone(&timer);

        // --- Raft event loop -------------------------------------------------
        // Single-task serialises: elections, vote counting, proposals, commits.
        // Lock ordering everywhere: state first, then log — never reversed.
        tokio::spawn(async move {
            // How many nodes (including self) are in the cluster?
            let cluster_size = el_state.lock().await.peers.len() + 1;

            // Proposals waiting for their log entry to be committed.
            // Key = log index assigned when the entry was appended.
            let mut pending: HashMap<LogIndex, tokio::sync::oneshot::Sender<anyhow::Result<()>>> =
                HashMap::new();

            // Votes accumulated in the current election (self-vote already counted).
            let mut votes_received: usize = 0;

            // Wakes per-peer replication tasks when a new entry is appended.
            let mut entry_notify: Option<Arc<Notify>> = None;

            // Signals replication tasks to stop (closed when stepping down).
            let mut shutdown_tx: Option<tokio::sync::watch::Sender<bool>> = None;

            loop {
                tokio::select! {
                    // ── Apply a committed entry ───────────────────────────────
                    // Complete any pending proposal at this log index and invoke
                    // commit callbacks so the state machine can apply the command.
                    Some(msg) = apply_rx.recv() => {
                        let ApplyMsg::Command { index, command } = msg;
                        if let Some(reply_tx) = pending.remove(&index) {
                            let _ = reply_tx.send(Ok(()));
                        }
                        // Callbacks are Fn (sync), so we hold the std::sync::Mutex
                        // briefly — no await inside the lock.
                        let cbs = el_callbacks.lock().expect("commit_callbacks poisoned");
                        for cb in cbs.iter() {
                            cb(command.clone());
                        }
                    }

                    // ── Replication task advanced a match_index ───────────────
                    // Re-evaluate which entries can now be committed.
                    _ = el_commit_notify.notified() => {
                        replication::advance_commit_index(
                            Arc::clone(&el_state),
                            Arc::clone(&el_log),
                            el_apply_tx.clone(),
                        ).await;
                    }

                    // ── Election timer fired ──────────────────────────────────
                    Some(()) = election_timeout_rx.recv() => {
                        votes_received = 1; // self-vote is implicit in start_election
                        let _ = election::start_election(
                            Arc::clone(&el_state),
                            Arc::clone(&el_log),
                            Arc::clone(&el_transport),
                            Arc::clone(&el_storage),
                            &*el_timer,
                            vote_reply_tx.clone(),
                        ).await;
                    }

                    // ── Vote reply from a peer ────────────────────────────────
                    Some((peer, election_term, reply)) = vote_reply_rx.recv() => {
                        let won = election::handle_vote_response(
                            Arc::clone(&el_state),
                            Arc::clone(&el_storage),
                            peer,
                            election_term,
                            reply,
                            &mut votes_received,
                            cluster_size,
                        ).await;

                        if won {
                            votes_received = 0;

                            // Stop any still-running replication tasks from the
                            // previous leadership term.
                            if let Some(tx) = shutdown_tx.take() {
                                let _ = tx.send(true);
                            }

                            if election::become_leader(
                                Arc::clone(&el_state),
                                Arc::clone(&el_log),
                                Arc::clone(&el_transport),
                                Arc::clone(&el_storage),
                            ).await.is_ok() {
                                // Fresh shutdown channel for this leader term.
                                let (sd_tx, sd_rx) = tokio::sync::watch::channel(false);
                                shutdown_tx = Some(sd_tx);

                                let notify = replication::start_replication_tasks(
                                    Arc::clone(&el_state),
                                    Arc::clone(&el_log),
                                    Arc::clone(&el_transport),
                                    Arc::clone(&el_storage),
                                    replication_config.clone(),
                                    Arc::clone(&el_commit_notify),
                                    sd_rx,
                                ).await;
                                entry_notify = Some(notify);
                            }
                        }
                    }

                    // ── Client propose request ────────────────────────────────
                    // Append to log, persist, wake replication tasks, register
                    // pending reply.  Does NOT block on commit — the apply arm
                    // completes the oneshot once the entry is committed.
                    Some((cmd, reply_tx)) = propose_rx.recv() => {
                        let is_leader = el_state.lock().await.is_leader();
                        if !is_leader {
                            let _ = reply_tx.send(Err(anyhow::anyhow!("not the Raft leader")));
                            continue;
                        }

                        // Append while holding both locks (consistent ordering).
                        let (entry_index, new_entry) = {
                            let state_guard = el_state.lock().await;
                            let mut log_guard = el_log.lock().await;
                            let term = state_guard.persistent.current_term;
                            let idx = log_guard.append_command(term, cmd);
                            let entry = log_guard.get(idx).unwrap().clone();
                            (idx, entry)
                            // locks released here
                        };

                        // Append only the new entry to disk (O(1) instead of O(n)).
                        if let Err(e) = el_storage.append_log_entries(&[new_entry]) {
                            tracing::error!("failed to persist log on propose: {e}");
                            let _ = reply_tx.send(Err(anyhow::anyhow!("log persistence failed: {e}")));
                            continue;
                        }

                        pending.insert(entry_index, reply_tx);

                        // Wake per-peer replication tasks.
                        if let Some(ref n) = entry_notify {
                            n.notify_waiters();
                        }
                    }

                    // All channel senders dropped — time to exit.
                    else => break,
                }
            }
        });

        RaftNode {
            node_id,
            state: raft_state,
            log: raft_log,
            peer_id_map,
            node_id_to_address,
            propose_tx,
            commit_callbacks,
            storage,
            apply_tx,
            timer,
        }
    }

    /// Propose a new command to be appended to the log.
    /// Returns once the entry is committed (or fails).
    pub async fn propose(&self, command: Command) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.propose_tx
            .send((command, reply_tx))
            .await
            .map_err(|_| anyhow::anyhow!("raft event loop has shut down"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("raft event loop dropped the reply sender"))?
    }

    /// Check if this node is currently the Raft leader.
    pub fn is_leader(&self) -> bool {
        self.state
            .try_lock()
            .map(|s| s.is_leader())
            .unwrap_or(false)
    }

    /// Get the current leader's ID and address, if known.
    pub fn leader_info(&self) -> Option<(u64, String)> {
        let state = self.state.try_lock().ok()?;
        let leader_node_id: &NodeId = state.volatile.current_leader.as_ref()?;

        // Translate the Raft NodeId (a numeric string like "1") to the
        // real network address using the map built from the peers list.
        // Falls back to the raw NodeId if it is already an address.
        let address = self
            .node_id_to_address
            .get(leader_node_id)
            .cloned()
            .unwrap_or_else(|| leader_node_id.clone());

        // If this node is the leader, return its own numeric ID.
        if leader_node_id == &state.id {
            return Some((self.node_id, address));
        }

        // For a peer leader, look up the numeric ID assigned in `new`.
        let numeric_id = self.peer_id_map.get(leader_node_id).copied().unwrap_or(0);
        Some((numeric_id, address))
    }

    /// Handle an incoming `RequestVote` RPC from a candidate.
    ///
    /// Raft paper: Figure 2, "RequestVote RPC", "Receiver implementation".
    ///
    /// Called by the CC HTTP server on `POST /raft/request_vote`.
    /// Rules (Raft §5.2 + §5.4):
    ///   1. If `args.term < current_term` → deny.
    ///   2. If `args.term > current_term` → step down to follower in new term.
    ///   3. Grant vote if `voted_for` is `None` or equals `args.candidate_id`,
    ///      AND the candidate's log is at least as up-to-date as ours.
    ///   4. Persist `voted_for` before replying (call `storage.save_persistent_state`).
    ///   5. If vote is granted, reset the election timer.
    pub async fn handle_request_vote(&self, args: rpc::RequestVoteArgs) -> rpc::RequestVoteReply {
        // Lock state then log — consistent ordering with every other call site.
        let mut state_guard = self.state.lock().await;
        let log_guard = self.log.lock().await;

        let current_term = state_guard.persistent.current_term;

        // 1. Reject if the candidate's term is stale.
        if args.term < current_term {
            return rpc::RequestVoteReply {
                term: current_term,
                vote_granted: false,
            };
        }

        // 2. Step down if the candidate has a higher term.
        if args.term > current_term {
            state_guard.step_down_to_follower(&self.storage, args.term);
        }

        let current_term = state_guard.persistent.current_term;

        // 3. Grant vote if we haven't voted (or already voted for this candidate)
        //    AND the candidate's log is at least as up-to-date as ours.
        let can_vote = match &state_guard.persistent.voted_for {
            None => true,
            Some(id) => id == &args.candidate_id,
        };

        let log_ok = log_guard.is_at_least_as_up_to_date(args.last_log_index, args.last_log_term);

        if can_vote && log_ok {
            // 4. Record vote and persist before replying.
            state_guard.persistent.voted_for = Some(args.candidate_id.clone());
            self.storage
                .save_persistent_state(&state_guard.persistent)
                .expect("failed to persist voted_for");

            // 5. Reset election timer so we don't trigger a spurious election
            //    immediately after granting a vote (Raft §5.2).
            drop(state_guard);
            self.timer.reset();

            return rpc::RequestVoteReply {
                term: current_term,
                vote_granted: true,
            };
        }

        rpc::RequestVoteReply {
            term: current_term,
            vote_granted: false,
        }
    }

    /// Handle an incoming `AppendEntries` RPC from the leader.
    ///
    /// Called by the CC HTTP server on `POST /raft/append_entries`.
    pub async fn handle_append_entries(
        &self,
        args: rpc::AppendEntriesArgs<Command>,
    ) -> rpc::AppendEntriesReply {
        let timer = Arc::clone(&self.timer);
        replication::handle_append_entries(
            args,
            Arc::clone(&self.state),
            Arc::clone(&self.log),
            Arc::clone(&self.storage),
            self.apply_tx.clone(),
            move || timer.reset(),
        )
        .await
    }

    /// Register a callback that is invoked when a log entry is committed.
    /// This is how the StateMachine receives commands to apply.
    pub fn on_commit(&self, callback: impl Fn(Command) + Send + 'static) {
        self.commit_callbacks
            .lock()
            .expect("commit_callbacks lock poisoned")
            .push(Box::new(callback));
    }
}
