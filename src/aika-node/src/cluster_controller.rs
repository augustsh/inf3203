use crate::common::*;
use crate::raft::{RaftNode, rpc};
use axum::{
    Json, Router,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;

// region config

pub struct ClusterControllerConfig {
    pub node_id: u64,
    pub bind: SocketAddr,
    pub peers: Vec<String>,
    pub task_ttl_secs: u64,
    /// Directory containing unlabeled images to ingest.
    pub image_dir: String,
    /// Number of images per task batch.
    pub batch_size: usize,
    /// Seconds without heartbeat before a local controller is considered failed.
    pub lc_heartbeat_timeout_secs: u64,
    /// Node-local directory for Raft persistent state (must NOT be on shared FS).
    pub data_dir: String,
    /// Directory for writing the results NDJSON file (can be on NFS).
    pub results_dir: String,
    /// Raft heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
    /// Minimum Raft election timeout in milliseconds.
    pub election_timeout_min_ms: u64,
    /// Maximum Raft election timeout in milliseconds.
    pub election_timeout_max_ms: u64,
    /// Maximum number of images to process (0 = unlimited).
    pub max_images: u64,
    /// If true, hold task ingestion until POST /start is received.
    pub hold: bool,
    /// Target number of active (agent_count > 0) local controllers to maintain.
    /// The leader will promote idle replicas if active_count falls below this.
    /// 0 = disabled.
    pub target_active_lc_count: usize,
    /// Agents to spawn when rebalancing promotes a replica. 0 = use 1 as fallback.
    pub agents_per_lc: usize,
}

// endregion

// region state machine

/// Holds the replicated state derived from applying Raft log entries.
struct StateMachine {
    tasks: HashMap<u64, TaskBatch>,
    nodes: HashMap<String, NodeInfo>,
    next_batch_id: u64,
    /// O(1) queue of batch IDs that are ready to be assigned.
    /// Stale entries (already assigned/completed) are lazily skipped.
    pending_queue: VecDeque<u64>,
    // -- Telemetry counters --
    ttl_expirations: u64,
    total_assignments: u64,
    total_completions: u64,
    first_completion_at: Option<u64>,
    last_completion_at: Option<u64>,
    started_at: Option<u64>,
    /// agent_id -> batches completed
    per_agent_completions: HashMap<String, u64>,
    /// agent_id -> images completed
    per_agent_images: HashMap<String, u64>,
    /// agent_id -> TTL expirations (how many times an assignment to this agent expired)
    per_agent_ttl_expirations: HashMap<String, u64>,
    /// Configured batch size (for telemetry reporting).
    batch_size: usize,
    /// Max images configured (for telemetry reporting).
    max_images: u64,
}

impl StateMachine {
    fn new(batch_size: usize, max_images: u64) -> Self {
        StateMachine {
            tasks: HashMap::new(),
            nodes: HashMap::new(),
            next_batch_id: 0,
            pending_queue: VecDeque::new(),
            ttl_expirations: 0,
            total_assignments: 0,
            total_completions: 0,
            first_completion_at: None,
            last_completion_at: None,
            started_at: None,
            per_agent_completions: HashMap::new(),
            per_agent_images: HashMap::new(),
            per_agent_ttl_expirations: HashMap::new(),
            batch_size,
            max_images,
        }
    }

    /// Apply a committed Command to the state machine.
    fn apply(&mut self, command: Command) {
        match command {
            Command::NoOp => {}

            Command::AddTaskBatch {
                batch_id,
                image_paths,
            } => {
                if !self.tasks.contains_key(&batch_id) {
                    self.tasks.insert(
                        batch_id,
                        TaskBatch {
                            batch_id,
                            image_paths,
                            status: TaskStatus::Pending,
                            labels: HashMap::new(),
                        },
                    );
                    self.pending_queue.push_back(batch_id);
                }
                if batch_id >= self.next_batch_id {
                    self.next_batch_id = batch_id + 1;
                }
            }

            Command::AssignTask {
                batch_id,
                agent_id,
                assigned_at,
            } => {
                if let Some(batch) = self.tasks.get_mut(&batch_id) {
                    if batch.status == TaskStatus::Pending {
                        batch.status = TaskStatus::Assigned {
                            agent_id,
                            assigned_at,
                        };
                        self.total_assignments += 1;
                        if self.started_at.is_none() {
                            self.started_at = Some(unix_now());
                        }
                    }
                    // Already Assigned or Completed — no-op (idempotent).
                }
            }

            Command::CompleteTask { batch_id, labels } => {
                if let Some(batch) = self.tasks.get_mut(&batch_id) {
                    if batch.status != TaskStatus::Completed {
                        let image_count = labels.len() as u64;
                        // Track which agent completed this batch.
                        if let TaskStatus::Assigned { ref agent_id, .. } = batch.status {
                            *self
                                .per_agent_completions
                                .entry(agent_id.clone())
                                .or_insert(0) += 1;
                            *self.per_agent_images.entry(agent_id.clone()).or_insert(0) +=
                                image_count;
                        }
                        batch.labels = labels.into_iter().collect();
                        batch.status = TaskStatus::Completed;
                        self.total_completions += 1;
                        let now = unix_now();
                        if self.first_completion_at.is_none() {
                            self.first_completion_at = Some(now);
                        }
                        self.last_completion_at = Some(now);
                    }
                    // Already Completed — no-op (idempotent).
                }
            }

            Command::ExpireTask { batch_id } => {
                if let Some(batch) = self.tasks.get_mut(&batch_id) {
                    if let TaskStatus::Assigned { ref agent_id, .. } = batch.status {
                        *self
                            .per_agent_ttl_expirations
                            .entry(agent_id.clone())
                            .or_insert(0) += 1;
                        batch.status = TaskStatus::Pending;
                        self.pending_queue.push_back(batch_id);
                        self.ttl_expirations += 1;
                    }
                }
            }

            Command::RegisterNode { node_id, address } => {
                self.nodes
                    .entry(node_id.clone())
                    .and_modify(|n| n.address = address.clone())
                    .or_insert_with(|| NodeInfo {
                        node_id,
                        address,
                        last_heartbeat: unix_now(),
                        agent_ids: Vec::new(),
                        agent_count: 0,
                        extractor_script: String::new(),
                        image_base_path: String::new(),
                        python: String::new(),
                    });
            }

            Command::DeregisterNode { node_id } => {
                self.nodes.remove(&node_id);
            }
        }
    }

    /// Pop and return the next pending batch, lazily skipping stale entries.
    /// O(1) amortised instead of O(n) linear scan.
    fn next_pending_batch(&mut self) -> Option<TaskBatch> {
        while let Some(&batch_id) = self.pending_queue.front() {
            if let Some(batch) = self.tasks.get(&batch_id) {
                if batch.status == TaskStatus::Pending {
                    return Some(batch.clone());
                }
            }
            self.pending_queue.pop_front();
        }
        None
    }

    /// Collect the `batch_id`s of all batches whose assignment TTL has expired.
    fn expired_batches(&self, now: u64, ttl_secs: u64) -> Vec<u64> {
        self.tasks
            .values()
            .filter_map(|b| {
                if let TaskStatus::Assigned { assigned_at, .. } = b.status {
                    if now.saturating_sub(assigned_at) > ttl_secs {
                        return Some(b.batch_id);
                    }
                }
                None
            })
            .collect()
    }

    /// Build a ClusterStatus snapshot.
    ///
    /// `now` and `lc_timeout_secs` are used to identify stale local controllers
    /// (those that have not sent a heartbeat recently).
    fn status(&self, now: u64, lc_timeout_secs: u64) -> ClusterStatus {
        let mut pending = 0u64;
        let mut assigned = 0u64;
        let mut completed = 0u64;
        let mut total_images = 0u64;
        let mut completed_images = 0u64;

        for b in self.tasks.values() {
            let img_count = b.image_paths.len() as u64;
            total_images += img_count;
            match b.status {
                TaskStatus::Pending => pending += 1,
                TaskStatus::Assigned { .. } => assigned += 1,
                TaskStatus::Completed => {
                    completed += 1;
                    completed_images += img_count;
                }
            }
        }

        let stale_nodes = self
            .nodes
            .values()
            .filter(|n| now.saturating_sub(n.last_heartbeat) > lc_timeout_secs)
            .map(|n| n.node_id.clone())
            .collect();

        // Aggregate per-agent stats into per-node stats.
        // Agent IDs follow the pattern: "<node_id>-agent-<N>"
        let mut per_node_completions: HashMap<String, u64> = HashMap::new();
        let mut per_node_images: HashMap<String, u64> = HashMap::new();
        let mut per_node_ttl_expirations: HashMap<String, u64> = HashMap::new();
        for (agent_id, count) in &self.per_agent_completions {
            let node_id = agent_id_to_node(agent_id);
            *per_node_completions.entry(node_id).or_insert(0) += count;
        }
        for (agent_id, count) in &self.per_agent_images {
            let node_id = agent_id_to_node(agent_id);
            *per_node_images.entry(node_id).or_insert(0) += count;
        }
        for (agent_id, count) in &self.per_agent_ttl_expirations {
            let node_id = agent_id_to_node(agent_id);
            *per_node_ttl_expirations.entry(node_id).or_insert(0) += count;
        }

        let mut pnc: Vec<(String, u64)> = per_node_completions.into_iter().collect();
        pnc.sort_by(|a, b| b.1.cmp(&a.1));
        let mut pni: Vec<(String, u64)> = per_node_images.into_iter().collect();
        pni.sort_by(|a, b| b.1.cmp(&a.1));
        let mut pnt: Vec<(String, u64)> = per_node_ttl_expirations.into_iter().collect();
        pnt.sort_by(|a, b| b.1.cmp(&a.1));

        let telemetry = ClusterTelemetry {
            total_images,
            completed_images,
            ttl_expirations: self.ttl_expirations,
            total_assignments: self.total_assignments,
            total_completions: self.total_completions,
            first_completion_at: self.first_completion_at,
            last_completion_at: self.last_completion_at,
            started_at: self.started_at,
            per_node_completions: pnc,
            per_node_images: pni,
            per_node_ttl_expirations: pnt,
            batch_size: self.batch_size,
            max_images: self.max_images,
        };

        ClusterStatus {
            total_tasks: self.tasks.len() as u64,
            pending_tasks: pending,
            assigned_tasks: assigned,
            completed_tasks: completed,
            registered_nodes: self.nodes.values().cloned().collect(),
            stale_nodes,
            telemetry,
        }
    }
}

// endregion

// region shared app state

#[derive(Clone)]
struct AppState {
    raft: RaftNode,
    sm: Arc<Mutex<StateMachine>>,
    /// Timeout used for LC liveness checks and the `/status` stale-node list.
    lc_timeout_secs: u64,
    /// This node's numeric ID, used to name the results output file.
    node_id: u64,
    /// Directory for writing the results NDJSON file.
    results_dir: String,
    /// When `--hold` is set, this is false until POST /start is called.
    started: Arc<AtomicBool>,
    /// Target active LC count for rebalancing. 0 = disabled.
    target_active_lc_count: usize,
    /// Agents per LC when promoting a replica during rebalancing.
    agents_per_lc: usize,
}

// endregion

// region main entrypoint

pub async fn run(config: ClusterControllerConfig) -> anyhow::Result<()> {
    tracing::info!(
        "Starting cluster controller {} on {}",
        config.node_id,
        config.bind
    );

    let bind = config.bind;
    let task_ttl_secs = config.task_ttl_secs;
    let image_dir = config.image_dir.clone();
    let batch_size = config.batch_size;
    let lc_timeout_secs = config.lc_heartbeat_timeout_secs;
    let node_id = config.node_id;
    let results_dir = config.results_dir.clone();
    let max_images = config.max_images;

    // Ensure the results directory exists (may be on NFS).
    std::fs::create_dir_all(&config.results_dir)?;

    // Node-local Raft storage (must NOT be a shared/distributed filesystem).
    let data_dir = std::path::PathBuf::from(&config.data_dir).join("raft");
    let election_config = crate::raft::election::ElectionConfig {
        timeout_min: Duration::from_millis(config.election_timeout_min_ms),
        timeout_max: Duration::from_millis(config.election_timeout_max_ms),
    };
    let replication_config = crate::raft::replication::ReplicationConfig {
        heartbeat_interval: Duration::from_millis(config.heartbeat_interval_ms),
    };
    let raft = RaftNode::new(
        config.node_id,
        config.peers,
        data_dir,
        election_config,
        replication_config,
    );
    let sm: Arc<Mutex<StateMachine>> =
        Arc::new(Mutex::new(StateMachine::new(batch_size, max_images)));

    // Register the state machine's apply function as a Raft commit callback.
    // This runs synchronously from inside the event loop — no await, no deadlock.
    {
        let sm_cb = Arc::clone(&sm);
        raft.on_commit(move |cmd| {
            sm_cb.lock().expect("sm lock poisoned").apply(cmd);
        });
    }

    let app = AppState {
        raft: raft.clone(),
        sm: Arc::clone(&sm),
        lc_timeout_secs,
        node_id,
        results_dir,
        started: Arc::new(AtomicBool::new(!config.hold)),
        target_active_lc_count: config.target_active_lc_count,
        agents_per_lc: config.agents_per_lc,
    };

    // TTL reaper — only the leader acts, followers idle.
    {
        let reaper_app = app.clone();
        tokio::spawn(async move {
            ttl_reaper_loop(reaper_app, task_ttl_secs).await;
        });
    }

    // Leadership watcher — resumes/completes image ingestion on every election win.
    // Ingestion may have been partial if the previous leader crashed mid-way;
    // `ingest_image_tasks` uses `next_batch_id` to skip already-committed batches.
    // When `--hold` is set, waits for POST /start before beginning ingestion.
    {
        let ingest_app = app.clone();
        tokio::spawn(async move {
            let mut was_leader = false;
            let mut ingestion_complete = false;
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;

                // Wait for the start signal if --hold was set.
                if !ingest_app.started.load(Ordering::Relaxed) {
                    continue;
                }

                let is_leader = ingest_app.raft.is_leader();
                if !is_leader {
                    was_leader = false;
                    ingestion_complete = false;
                    continue;
                }
                if !was_leader || !ingestion_complete {
                    ingestion_complete =
                        ingest_image_tasks(&ingest_app, &image_dir, batch_size, max_images).await;
                    if !ingestion_complete {
                        // Back off before retrying failed ingestion.
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
                was_leader = is_leader;
            }
        });
    }

    // LC liveness monitor — logs warnings when a local controller goes silent.
    {
        let monitor_app = app.clone();
        tokio::spawn(async move { lc_monitor_loop(monitor_app).await });
    }

    // Results persistence — periodically flushes completed labels to disk.
    {
        let flush_app = app.clone();
        tokio::spawn(async move { results_flush_loop(flush_app).await });
    }

    start_http_server(app, bind).await
}

// endregion

// region http server

async fn start_http_server(app: AppState, bind: SocketAddr) -> anyhow::Result<()> {
    let router = Router::new()
        // Task endpoints (agent/LC-facing)
        .route("/task/request", post(handle_task_request))
        .route("/task/complete", post(handle_task_complete))
        .route("/heartbeat", post(handle_heartbeat))
        .route("/leader", get(handle_leader_query))
        .route("/status", get(handle_status))
        // Cluster lifecycle
        .route("/start", post(handle_start))
        // Raft internal endpoints (CC-to-CC only)
        .route("/raft/request_vote", post(handle_raft_request_vote))
        .route("/raft/append_entries", post(handle_raft_append_entries))
        .with_state(app);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!("Cluster controller HTTP server listening on {}", bind);
    axum::serve(listener, router).await?;
    Ok(())
}

// endregion

// region task handlers

/// POST /task/request — assign the next pending batch to the requesting agent.
///
/// Only the leader can assign tasks. Non-leaders redirect to the leader.
/// Retries up to 5 times to handle simultaneous-assignment collisions.
async fn handle_task_request(
    State(app): State<AppState>,
    Json(request): Json<TaskRequest>,
) -> Result<Json<TaskAssignment>, ApiError> {
    redirect_if_not_leader(&app)?;

    for _ in 0..5u32 {
        // Find a pending batch (brief lock, no I/O).
        let batch = {
            let mut sm = app.sm.lock().expect("sm lock");
            sm.next_pending_batch()
        };
        let Some(batch) = batch else {
            return Err(ApiError::NoWorkAvailable);
        };

        // Propose the assignment through Raft (blocks until committed).
        app.raft
            .propose(Command::AssignTask {
                batch_id: batch.batch_id,
                agent_id: request.agent_id.clone(),
                assigned_at: unix_now(),
            })
            .await
            .map_err(|e| ApiError::Internal(e.to_string()))?;

        // Verify we actually got the batch (another agent may have raced us).
        let got_it = {
            let sm = app.sm.lock().expect("sm lock");
            sm.tasks.get(&batch.batch_id).is_some_and(|b| {
                matches!(&b.status, TaskStatus::Assigned { agent_id, .. } if agent_id == &request.agent_id)
            })
        };
        if got_it {
            return Ok(Json(TaskAssignment {
                batch_id: batch.batch_id,
                image_paths: batch.image_paths,
            }));
        }
        // Collision — try the next pending batch.
    }

    Err(ApiError::NoWorkAvailable)
}

/// POST /task/complete — record the labels for a completed batch. Idempotent.
async fn handle_task_complete(
    State(app): State<AppState>,
    Json(completion): Json<TaskCompletion>,
) -> Result<Json<TaskCompletionResponse>, ApiError> {
    redirect_if_not_leader(&app)?;

    // Already completed → accept without a Raft round-trip.
    let already_done = {
        let sm = app.sm.lock().expect("sm lock");
        sm.tasks
            .get(&completion.batch_id)
            .is_some_and(|b| b.status == TaskStatus::Completed)
    };
    if already_done {
        return Ok(Json(TaskCompletionResponse {
            accepted: true,
            message: "already completed (idempotent)".into(),
        }));
    }

    app.raft
        .propose(Command::CompleteTask {
            batch_id: completion.batch_id,
            labels: completion.labels,
        })
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(Json(TaskCompletionResponse {
        accepted: true,
        message: "accepted".into(),
    }))
}

/// POST /heartbeat — record liveness of a local controller.
///
/// Heartbeat state is ephemeral (not replicated) — it is rebuilt from
/// incoming heartbeats after every leader election.
async fn handle_heartbeat(
    State(app): State<AppState>,
    Json(request): Json<HeartbeatRequest>,
) -> Result<Json<HeartbeatResponse>, ApiError> {
    redirect_if_not_leader(&app)?;

    {
        let mut sm = app.sm.lock().expect("sm lock");
        let entry = sm
            .nodes
            .entry(request.node_id.clone())
            .or_insert_with(|| NodeInfo {
                node_id: request.node_id.clone(),
                address: request.address.clone(),
                last_heartbeat: unix_now(),
                agent_ids: Vec::new(),
                agent_count: 0,
                extractor_script: String::new(),
                image_base_path: String::new(),
                python: String::new(),
            });
        entry.last_heartbeat = unix_now();
        entry.agent_ids = request.agent_ids;
        entry.address = request.address;
        entry.agent_count = request.agent_count;
        entry.extractor_script = request.extractor_script;
        entry.image_base_path = request.image_base_path;
        entry.python = request.python;
    }

    Ok(Json(HeartbeatResponse { acknowledged: true }))
}

/// POST /start — release the hold on task ingestion.
/// Safe to call multiple times; idempotent.
async fn handle_start(State(app): State<AppState>) -> StatusCode {
    let was_started = app.started.swap(true, Ordering::Relaxed);
    if !was_started {
        tracing::info!("Cluster start signal received — beginning task ingestion");
    }
    StatusCode::OK
}

/// GET /leader — return the current Raft leader's address.
async fn handle_leader_query(State(app): State<AppState>) -> Json<LeaderResponse> {
    let (leader_id, leader_address) = match app.raft.leader_info() {
        Some((id, addr)) => (Some(id), Some(addr)),
        None => (None, None),
    };
    Json(LeaderResponse {
        leader_id,
        leader_address,
    })
}

/// GET /status — return a summary of task progress and node health.
async fn handle_status(State(app): State<AppState>) -> Json<ClusterStatus> {
    let now = unix_now();
    let status = app
        .sm
        .lock()
        .expect("sm lock")
        .status(now, app.lc_timeout_secs);
    Json(status)
}

// endregion

// region raft internal handlers (CC only)

/// POST /raft/request_vote
async fn handle_raft_request_vote(
    State(app): State<AppState>,
    Json(args): Json<rpc::RequestVoteArgs>,
) -> Json<rpc::RequestVoteReply> {
    Json(app.raft.handle_request_vote(args).await)
}

/// POST /raft/append_entries
async fn handle_raft_append_entries(
    State(app): State<AppState>,
    Json(args): Json<rpc::AppendEntriesArgs<Command>>,
) -> Json<rpc::AppendEntriesReply> {
    let reply = app.raft.handle_append_entries(args).await;
    Json(reply)
}

// endregion

// region background tasks

/// Periodically scans for expired task assignments and proposes `ExpireTask`
/// to return them to Pending so they can be reassigned.
///
/// Only the leader acts; followers sleep and check again next cycle.
async fn ttl_reaper_loop(app: AppState, ttl_secs: u64) {
    let interval = Duration::from_secs((ttl_secs / 2).max(5));
    loop {
        tokio::time::sleep(interval).await;

        if !app.raft.is_leader() {
            continue;
        }

        let now = unix_now();
        // Brief lock to collect expired IDs — no I/O while holding it.
        let expired = {
            app.sm
                .lock()
                .expect("sm lock")
                .expired_batches(now, ttl_secs)
        };

        for batch_id in expired {
            tracing::info!("TTL expired for batch {}, returning to Pending", batch_id);
            if let Err(e) = app.raft.propose(Command::ExpireTask { batch_id }).await {
                tracing::warn!("Failed to propose ExpireTask for batch {}: {}", batch_id, e);
            }
        }
    }
}

/// Read image filenames from `image_dir`, chunk them into batches, and
/// propose `AddTaskBatch` commands so every CC replica learns about them.
///
/// Resumable: reads `next_batch_id` from the state machine and skips the
/// corresponding prefix of the sorted image list, so a new leader after a
/// mid-ingestion crash picks up exactly where the previous leader left off.
/// Returns `true` if all batches were successfully ingested, `false` if
/// ingestion was partial and should be retried.
///
/// Uses gap detection (checks which batch IDs exist in the state machine)
/// so it correctly fills holes left by a previous partial ingestion, and
/// pipelines up to `MAX_IN_FLIGHT` Raft proposals for throughput.
async fn ingest_image_tasks(
    app: &AppState,
    image_dir: &str,
    batch_size: usize,
    max_images: u64,
) -> bool {
    tracing::info!(
        "Ingesting image tasks from {} (batch_size={}, max_images={})",
        image_dir,
        batch_size,
        if max_images == 0 {
            "unlimited".to_string()
        } else {
            max_images.to_string()
        }
    );

    // Collect image file names.
    let mut names: Vec<String> = match tokio::fs::read_dir(image_dir).await {
        Ok(mut dir) => {
            let mut v = Vec::new();
            loop {
                match dir.next_entry().await {
                    Ok(Some(entry)) => match entry.file_type().await {
                        Ok(ft) if ft.is_file() => {
                            v.push(entry.file_name().to_string_lossy().into_owned());
                        }
                        _ => {}
                    },
                    Ok(None) => break,
                    Err(e) => {
                        tracing::error!("Error reading image dir: {}", e);
                        break;
                    }
                }
            }
            v
        }
        Err(e) => {
            tracing::error!("Cannot open image dir {}: {}", image_dir, e);
            return false;
        }
    };

    if names.is_empty() {
        tracing::warn!("No image files found in {}", image_dir);
        return false;
    }

    names.sort_unstable();

    // Limit the number of images if max_images is set.
    if max_images > 0 && names.len() > max_images as usize {
        tracing::info!(
            "Limiting ingestion to {} of {} images (--max-images)",
            max_images,
            names.len()
        );
        names.truncate(max_images as usize);
    }

    let total_batches = names.len().div_ceil(batch_size);

    // Find which batch IDs are missing from the state machine (handles gaps
    // from partial previous ingestion as well as fresh start).
    let existing_ids: HashSet<u64> = {
        let sm = app.sm.lock().expect("sm lock");
        sm.tasks.keys().copied().collect()
    };

    let missing: Vec<(u64, Vec<String>)> = names
        .chunks(batch_size)
        .enumerate()
        .filter(|(i, _)| !existing_ids.contains(&(*i as u64)))
        .map(|(i, chunk)| (i as u64, chunk.to_vec()))
        .collect();

    if missing.is_empty() {
        tracing::info!(
            "All {} batches already ingested — nothing to do",
            total_batches
        );
        return true;
    }

    tracing::info!(
        "Found {} images ({} batches total), {} to ingest",
        names.len(),
        total_batches,
        missing.len(),
    );

    // Pipeline proposals: keep up to MAX_IN_FLIGHT concurrent Raft commits.
    const MAX_IN_FLIGHT: usize = 16;
    let mut in_flight: VecDeque<(u64, tokio::task::JoinHandle<anyhow::Result<()>>)> =
        VecDeque::new();
    let mut failed = false;

    for (batch_id, image_paths) in missing {
        // Drain oldest if at capacity.
        while in_flight.len() >= MAX_IN_FLIGHT {
            let (id, handle) = in_flight.pop_front().unwrap();
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::error!("Failed to propose AddTaskBatch {}: {}", id, e);
                    failed = true;
                    break;
                }
                Err(e) => {
                    tracing::error!("Ingestion task {} panicked: {}", id, e);
                    failed = true;
                    break;
                }
            }
        }
        if failed {
            break;
        }

        let raft = app.raft.clone();
        in_flight.push_back((
            batch_id,
            tokio::spawn(async move {
                raft.propose(Command::AddTaskBatch {
                    batch_id,
                    image_paths,
                })
                .await
            }),
        ));
    }

    // Drain remaining in-flight proposals.
    for (id, handle) in in_flight {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!("Failed to propose AddTaskBatch {}: {}", id, e);
                failed = true;
            }
            Err(e) => {
                tracing::error!("Ingestion task {} panicked: {}", id, e);
                failed = true;
            }
        }
    }

    if failed {
        tracing::warn!("Ingestion incomplete — will retry");
        return false;
    }

    tracing::info!("Ingestion complete: {} batches", total_batches);
    true
}

/// Monitor local controller liveness with two-stage failover.
///
/// Stage 1 (first `lc_timeout_secs` of silence): mark LC as "suspected", log warning.
/// Stage 2 (another `lc_timeout_secs` of silence): LC is confirmed failed — find a
/// standby replica (agent_count == 0) and send it POST /activate to take over.
///
/// State is cleared when this node loses leadership so the next leader starts fresh.
async fn lc_monitor_loop(app: AppState) {
    let interval = Duration::from_secs(3);
    let client = reqwest::Client::new();
    // node_id -> unix timestamp when it first exceeded the timeout (stage 1)
    let mut suspected_since: HashMap<String, u64> = HashMap::new();
    // node_ids that have already been delegated to a replica (avoid re-triggering)
    let mut already_delegated: HashSet<String> = HashSet::new();

    loop {
        tokio::time::sleep(interval).await;

        if !app.raft.is_leader() {
            suspected_since.clear();
            already_delegated.clear();
            continue;
        }

        let now = unix_now();
        let timeout = app.lc_timeout_secs;
        let nodes: Vec<NodeInfo> = app
            .sm
            .lock()
            .expect("sm lock")
            .nodes
            .values()
            .cloned()
            .collect();

        for node in &nodes {
            let age = now.saturating_sub(node.last_heartbeat);

            if age < timeout {
                // Node is healthy — clear suspicion so it can be detected again later.
                suspected_since.remove(&node.node_id);
                already_delegated.remove(&node.node_id);
                continue;
            }

            if !suspected_since.contains_key(&node.node_id) {
                // Stage 1: first time we notice the timeout exceeded.
                suspected_since.insert(node.node_id.clone(), now);
                tracing::warn!(
                    "LC {} suspected failed ({}s without heartbeat) — waiting before delegating",
                    node.node_id,
                    age
                );
                continue;
            }

            // Stage 2: node has been suspected for at least another full timeout.
            let suspected_age = now.saturating_sub(suspected_since[&node.node_id]);
            if suspected_age < timeout || already_delegated.contains(&node.node_id) {
                continue;
            }

            tracing::warn!(
                "LC {} confirmed failed ({}s total silence) — activating replica",
                node.node_id,
                age
            );
            already_delegated.insert(node.node_id.clone());

            // Find a healthy standby replica (agent_count == 0, not itself, not failed).
            let replica = nodes.iter().find(|n| {
                n.node_id != node.node_id
                    && n.agent_count == 0
                    && now.saturating_sub(n.last_heartbeat) < timeout
                    && !already_delegated.contains(&n.node_id)
            });

            if let Some(replica) = replica {
                let req = ActivateRequest {
                    failed_node_id: node.node_id.clone(),
                    agent_count: node.agent_count.max(1),
                    extractor_script: node.extractor_script.clone(),
                    image_base_path: node.image_base_path.clone(),
                    python: node.python.clone(),
                };
                let url = format!("http://{}/activate", replica.address);
                match client.post(&url).json(&req).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        tracing::info!(
                            "Replica {} activated to replace failed LC {}",
                            replica.node_id,
                            node.node_id
                        );
                    }
                    Ok(resp) => tracing::warn!(
                        "Replica {} activation returned unexpected status {}",
                        replica.node_id,
                        resp.status()
                    ),
                    Err(e) => tracing::warn!(
                        "Failed to activate replica {} for LC {}: {}",
                        replica.node_id,
                        node.node_id,
                        e
                    ),
                }
            } else {
                tracing::warn!(
                    "No healthy standby replica found to replace failed LC {}",
                    node.node_id
                );
            }
        }

        // Rebalancing: if target_active_lc_count is configured and we have fewer
        // active LCs than desired (e.g. after mass crash/restart), promote an idle
        // replica to restore throughput.
        if app.target_active_lc_count > 0 {
            let active_count = nodes
                .iter()
                .filter(|n| n.agent_count > 0 && now.saturating_sub(n.last_heartbeat) < timeout)
                .count();

            if active_count < app.target_active_lc_count {
                let promote_agents = app.agents_per_lc.max(1);
                // Find a healthy idle replica not already delegated.
                let replica = nodes.iter().find(|n| {
                    n.agent_count == 0
                        && now.saturating_sub(n.last_heartbeat) < timeout
                        && !already_delegated.contains(&n.node_id)
                });
                if let Some(replica) = replica {
                    tracing::info!(
                        "Rebalancing: active={} < target={} — promoting replica {} ({} agents)",
                        active_count, app.target_active_lc_count, replica.node_id, promote_agents
                    );
                    already_delegated.insert(replica.node_id.clone());
                    let req = ActivateRequest {
                        failed_node_id: "rebalance".to_string(),
                        agent_count: promote_agents,
                        extractor_script: replica.extractor_script.clone(),
                        image_base_path: replica.image_base_path.clone(),
                        python: replica.python.clone(),
                    };
                    let url = format!("http://{}/activate", replica.address);
                    match client.post(&url).json(&req).send().await {
                        Ok(resp) if resp.status().is_success() => {
                            tracing::info!("Replica {} promoted for rebalancing", replica.node_id);
                        }
                        Ok(resp) => tracing::warn!(
                            "Rebalance activation of {} returned status {}",
                            replica.node_id, resp.status()
                        ),
                        Err(e) => tracing::warn!(
                            "Rebalance activation of {} failed: {}",
                            replica.node_id, e
                        ),
                    }
                }
            }
        }
    }
}

/// Periodically flush completed task labels to a node-local NDJSON file.
///
/// Each line is a JSON object: `{"batch_id": N, "labels": [[path, label], ...]}`.
/// Only newly completed batches since the last flush are appended, so the file
/// grows incrementally and a crash only loses the current flush interval's data.
///
/// Once every task is completed, writes a final `labeled_data.json` that groups
/// images by label.
///
/// Runs only on the leader (the node with the authoritative state machine).
/// File path: `<results_dir>/results_<node_id>.ndjson`
async fn results_flush_loop(app: AppState) {
    let path = format!("{}/results_{}.ndjson", app.results_dir, app.node_id);
    let mut flushed: HashSet<u64> = HashSet::new();
    let mut final_written = false;

    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;

        if !app.raft.is_leader() {
            final_written = false;
            continue;
        }

        // Collect newly completed batches — brief lock, no I/O.
        let (new_completed, all_done, total) = {
            let sm = app.sm.lock().expect("sm lock");
            let new: Vec<(u64, Vec<(String, String)>)> = sm
                .tasks
                .values()
                .filter(|b| b.status == TaskStatus::Completed && !flushed.contains(&b.batch_id))
                .map(|b| {
                    let labels: Vec<(String, String)> = b
                        .labels
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    (b.batch_id, labels)
                })
                .collect();
            let total = sm.tasks.len();
            let completed = sm
                .tasks
                .values()
                .filter(|b| b.status == TaskStatus::Completed)
                .count();
            (new, total > 0 && completed == total, total)
        };

        if !new_completed.is_empty() {
            // Append new records to the output file (no locks held).
            match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await
            {
                Ok(mut file) => {
                    let mut written = 0usize;
                    for (batch_id, labels) in &new_completed {
                        let record = serde_json::json!({
                            "batch_id": batch_id,
                            "labels": labels,
                        });
                        let line = format!("{}\n", record);
                        match file.write_all(line.as_bytes()).await {
                            Ok(()) => {
                                flushed.insert(*batch_id);
                                written += 1;
                            }
                            Err(e) => {
                                tracing::error!("Failed to write results to {}: {}", path, e);
                                break;
                            }
                        }
                    }
                    if written > 0 {
                        tracing::info!(
                            "Flushed {} completed batches to {} ({} total flushed)",
                            written,
                            path,
                            flushed.len()
                        );
                    }
                }
                Err(e) => tracing::error!("Cannot open results file {}: {}", path, e),
            }
        }

        // Once every task is done, write the final labeled_data.json.
        if all_done && !final_written {
            tracing::info!("All {} tasks completed — writing labeled_data.json", total);
            write_labeled_data_json(&app).await;
            final_written = true;
        }
    }
}

/// Build and write `labeled_data.json`
///
/// Groups all images by their predicted label:
/// ```json
/// {Mo
///   "Yellow parrots": ["img001.jpg", "img042.jpg"],
///   "Greyhound(dog)": ["img003.jpg", "img017.jpg"]
/// }
/// ```
async fn write_labeled_data_json(app: &AppState) {
    // Collect all labels from the state machine (brief lock, no I/O).
    let label_map: HashMap<String, Vec<String>> = {
        let sm = app.sm.lock().expect("sm lock");
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for batch in sm.tasks.values() {
            for (image_path, label) in &batch.labels {
                map.entry(label.clone())
                    .or_default()
                    .push(image_path.clone());
            }
        }
        map
    };

    // Sort images within each label for deterministic output.
    let mut sorted_map: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for (label, mut images) in label_map {
        images.sort_unstable();
        sorted_map.insert(label, images);
    }

    let output_path = format!("{}/labeled_data.json", app.results_dir);
    let tmp_path = format!("{}/labeled_data.json.tmp", app.results_dir);

    match tokio::fs::File::create(&tmp_path).await {
        Ok(mut file) => {
            let json = match serde_json::to_string_pretty(&sorted_map) {
                Ok(j) => j,
                Err(e) => {
                    tracing::error!("Failed to serialize labeled_data.json: {}", e);
                    return;
                }
            };
            if let Err(e) = file.write_all(json.as_bytes()).await {
                tracing::error!("Failed to write labeled_data.json: {}", e);
                return;
            }
        }
        Err(e) => {
            tracing::error!("Cannot create {}: {}", tmp_path, e);
            return;
        }
    }

    if let Err(e) = tokio::fs::rename(&tmp_path, &output_path).await {
        tracing::error!("Failed to rename {} -> {}: {}", tmp_path, output_path, e);
        return;
    }

    let label_count = sorted_map.len();
    let image_count: usize = sorted_map.values().map(|v| v.len()).sum();
    tracing::info!(
        "Wrote {} ({} labels, {} images)",
        output_path,
        label_count,
        image_count
    );
}

// endregion

// region error types

#[derive(Debug)]
enum ApiError {
    NotLeader { leader_address: Option<String> },
    NoWorkAvailable,
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::NotLeader {
                leader_address: Some(addr),
            } => (
                StatusCode::TEMPORARY_REDIRECT,
                [(header::LOCATION, format!("http://{}", addr))],
                "not the leader",
            )
                .into_response(),
            ApiError::NotLeader {
                leader_address: None,
            } => (
                StatusCode::SERVICE_UNAVAILABLE,
                "leader unknown — election in progress",
            )
                .into_response(),
            ApiError::NoWorkAvailable => {
                (StatusCode::NO_CONTENT, "no work available").into_response()
            }
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response(),
        }
    }
}

// endregion

// region Helpers

/// If this node is not the Raft leader, return an `ApiError::NotLeader`
/// redirect so the client can find and contact the leader directly.
fn redirect_if_not_leader(app: &AppState) -> Result<(), ApiError> {
    if app.raft.is_leader() {
        return Ok(());
    }
    let leader_address = app.raft.leader_info().map(|(_, addr)| addr);
    Err(ApiError::NotLeader { leader_address })
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Extract the node portion from an agent ID.
/// Agent IDs follow `<node_id>-agent-<N>`, e.g. `lc-c6-0-agent-2` → `lc-c6-0`.
fn agent_id_to_node(agent_id: &str) -> String {
    if let Some(pos) = agent_id.rfind("-agent-") {
        agent_id[..pos].to_string()
    } else {
        agent_id.to_string()
    }
}

// endregion
