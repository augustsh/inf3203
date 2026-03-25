use crate::common::*;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

pub struct LocalControllerConfig {
    pub node_id: String,
    pub bind: SocketAddr,
    pub cc_addrs: Vec<String>,
    pub agent_count: usize,
    pub health_check_interval: u64,
    pub extractor_script: String,
    pub image_base_path: String,
    pub python: String,
    /// Number of OpenMP threads per agent. 0 = auto (total_cores / agent_count).
    pub omp_threads: usize,
}

/// Tracks the state of a locally managed agent process.
struct ManagedAgent {
    /// Handle to the spawned child process; used to check liveness and kill on shutdown.
    process: std::process::Child,
    /// Unix timestamp of the last heartbeat received from this agent.
    last_heartbeat: u64,
    /// The batch this agent is currently processing, if any.
    current_batch_id: Option<u64>,
}

/// The local controller's runtime state — owned by a single Mutex.
struct LocalControllerState {
    config: LocalControllerConfig,
    agents: HashMap<String, ManagedAgent>,
    /// Cached address of the current Raft leader (updated on redirects).
    current_leader: Option<String>,
    /// True when this LC has agents running (startup with agent_count > 0, or after /activate).
    is_active: bool,
}

impl LocalControllerState {
    fn new(config: LocalControllerConfig) -> Self {
        let is_active = config.agent_count > 0;
        Self {
            config,
            agents: HashMap::new(),
            current_leader: None,
            is_active,
        }
    }
}

/// All shared handles needed by background tasks and HTTP handlers.
/// `Clone` is cheap — Arc and reqwest::Client are both reference-counted.
#[derive(Clone)]
struct AppState {
    state: Arc<Mutex<LocalControllerState>>,
    client: reqwest::Client,
}

// region entry point

/// Main entry point for the local controller.
pub async fn run(config: LocalControllerConfig) -> anyhow::Result<()> {
    tracing::info!(
        "Starting local controller {} on {} with {} agents",
        config.node_id,
        config.bind,
        config.agent_count
    );

    let agent_count = config.agent_count;
    let health_interval = config.health_check_interval;
    let bind = config.bind;

    let state = Arc::new(Mutex::new(LocalControllerState::new(config)));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("failed to build HTTP client");
    let app = AppState {
        state: Arc::clone(&state),
        client,
    };

    // Spawn initial agent processes.
    // No other tasks are running yet so holding the lock here is fine.
    if agent_count > 0 {
        // Extract config data without holding the lock during the spawn syscalls.
        let (node_id, lc_port, cc_addrs, extractor_script, image_base_path, python, omp_threads) = {
            let g = state.lock().await;
            let omp = effective_omp_threads(g.config.omp_threads, g.config.agent_count);
            (
                g.config.node_id.clone(),
                g.config.bind.port(),
                g.config.cc_addrs.clone(),
                g.config.extractor_script.clone(),
                g.config.image_base_path.clone(),
                g.config.python.clone(),
                omp,
            )
        };
        let lc_addr = format!("127.0.0.1:{}", lc_port);

        tracing::info!(
            "OMP_NUM_THREADS per agent: {}",
            if omp_threads == 0 {
                "default (all cores)".to_string()
            } else {
                omp_threads.to_string()
            }
        );

        let mut spawned = Vec::with_capacity(agent_count);
        for i in 0..agent_count {
            let agent_id = format!("{}-agent-{}", node_id, i);
            match spawn_agent(
                &agent_id,
                &lc_addr,
                &cc_addrs,
                &extractor_script,
                &image_base_path,
                &python,
                omp_threads,
            ) {
                Ok(agent) => {
                    tracing::info!("Spawned agent {}", agent_id);
                    spawned.push((agent_id, agent));
                }
                Err(e) => tracing::error!("Failed to spawn agent {}: {}", agent_id, e),
            }
        }

        let mut g = state.lock().await;
        for (id, agent) in spawned {
            g.agents.insert(id, agent);
        }
    } else {
        tracing::info!("Running in replica mode (no agents) — standing by for failover");
    }

    // Start background tasks (both take a cheap clone of AppState).
    let hb_app = app.clone();
    tokio::spawn(async move { heartbeat_loop(hb_app).await });

    let health_app = app.clone();
    tokio::spawn(async move { agent_health_loop(health_app, health_interval).await });

    // Start the HTTP server — this blocks until shutdown.
    start_http_server(app, bind).await
}

// endregion

// region CC leader discovery

/// Return the cached leader address, or discover it by querying GET /leader
/// on each known CC address.
///
/// Does NOT hold the mutex during the HTTP calls.
async fn find_leader(app: &AppState) -> anyhow::Result<String> {
    // Check the cache first.
    {
        let g = app.state.lock().await;
        if let Some(addr) = &g.current_leader {
            return Ok(addr.clone());
        }
    }

    // Cache miss — query each CC.
    let cc_addrs = {
        let g = app.state.lock().await;
        g.config.cc_addrs.clone()
    };

    for cc_addr in &cc_addrs {
        let url = format!("http://{}/leader", cc_addr);
        match app.client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(body) = resp.json::<LeaderResponse>().await {
                    if let Some(addr) = body.leader_address {
                        app.state.lock().await.current_leader = Some(addr.clone());
                        return Ok(addr);
                    }
                }
            }
            Ok(resp) => tracing::debug!("CC {} /leader returned {}", cc_addr, resp.status()),
            Err(e) => tracing::debug!("CC {} unreachable: {}", cc_addr, e),
        }
    }

    anyhow::bail!("No Raft leader found among configured CCs")
}

/// Forward a POST request to the CC leader, updating the leader cache from the
/// response URL (reqwest auto-follows 3xx redirects from non-leader CCs).
///
/// Retries up to 3 times after clearing the cache if an attempt fails.
async fn proxy_to_leader<B, R>(app: &AppState, path: &str, body: &B) -> anyhow::Result<R>
where
    B: serde::Serialize,
    R: for<'de> serde::Deserialize<'de>,
{
    const MAX_ATTEMPTS: u32 = 3;
    for attempt in 0..MAX_ATTEMPTS {
        let leader = match find_leader(app).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(
                    "Proxy attempt {}/{} to {}: leader discovery failed: {}",
                    attempt + 1,
                    MAX_ATTEMPTS,
                    path,
                    e
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let url = format!("http://{}{}", leader, path);

        match app.client.post(&url).json(body).send().await {
            Ok(resp) => {
                // Cache the actual endpoint reached (may differ from `leader` if
                // reqwest followed a redirect to the true leader).
                if let (Some(host), Some(port)) =
                    (resp.url().host_str(), resp.url().port_or_known_default())
                {
                    let actual = format!("{}:{}", host, port);
                    if actual != leader {
                        tracing::debug!("Leader updated: {} -> {}", leader, actual);
                        app.state.lock().await.current_leader = Some(actual);
                    }
                }

                let status = resp.status();

                if status == reqwest::StatusCode::OK {
                    return Ok(resp.json::<R>().await?);
                }

                // 204 No Content → no work available; not a leader problem.
                if status == reqwest::StatusCode::NO_CONTENT {
                    anyhow::bail!("no work available");
                }

                tracing::warn!(
                    "Proxy attempt {}/{} to {} returned {}",
                    attempt + 1,
                    MAX_ATTEMPTS,
                    path,
                    status
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Proxy attempt {}/{} to {} failed: {}",
                    attempt + 1,
                    MAX_ATTEMPTS,
                    path,
                    e
                );
            }
        }

        // Clear cache so the next attempt re-discovers the leader.
        app.state.lock().await.current_leader = None;
    }

    anyhow::bail!(
        "Failed to proxy {} to CC leader after {} attempts",
        path,
        MAX_ATTEMPTS
    )
}

// endregion

// region agent process lifecycle

/// Spawn a new agent as a child process using the same binary as the current process.
fn spawn_agent(
    agent_id: &str,
    lc_addr: &str,
    cc_addrs: &[String],
    extractor_script: &str,
    image_base_path: &str,
    python: &str,
    omp_threads: usize,
) -> anyhow::Result<ManagedAgent> {
    let exe = std::env::current_exe()
        .map_err(|e| anyhow::anyhow!("Cannot determine current executable: {}", e))?;

    let child = std::process::Command::new(&exe)
        .arg("agent")
        .arg("--agent-id")
        .arg(agent_id)
        .arg("--lc-addr")
        .arg(lc_addr)
        .arg("--cc-addrs")
        .arg(cc_addrs.join(","))
        .arg("--extractor-script")
        .arg(extractor_script)
        .arg("--image-base-path")
        .arg(image_base_path)
        .arg("--python")
        .arg(python)
        .arg("--omp-threads")
        .arg(omp_threads.to_string())
        .spawn()
        .map_err(|e| anyhow::anyhow!("Failed to spawn agent process: {}", e))?;

    Ok(ManagedAgent {
        process: child,
        last_heartbeat: unix_now(),
        current_batch_id: None,
    })
}

// endregion

// region background loops

/// Periodically checks that all managed agents are alive and restarts crashed ones.
///
/// Lock is held briefly per cycle (only for `try_wait` + possible respawn).
async fn agent_health_loop(app: AppState, interval_secs: u64) {
    loop {
        tokio::time::sleep(Duration::from_secs(interval_secs)).await;

        // Collect IDs of dead agents and config data needed for respawning —
        // all under a single short-lived lock.
        let (dead_ids, lc_addr, cc_addrs, extractor_script, image_base_path, python, omp_threads) = {
            let mut g = app.state.lock().await;
            let lc_port = g.config.bind.port();
            let lc_addr = format!("127.0.0.1:{}", lc_port);
            let cc_addrs = g.config.cc_addrs.clone();
            let extractor_script = g.config.extractor_script.clone();
            let image_base_path = g.config.image_base_path.clone();
            let python = g.config.python.clone();
            let omp = effective_omp_threads(g.config.omp_threads, g.config.agent_count);

            let mut dead_ids = Vec::new();
            for (id, agent) in g.agents.iter_mut() {
                match agent.process.try_wait() {
                    Ok(Some(status)) => {
                        tracing::warn!("Agent {} exited ({}), will respawn", id, status);
                        dead_ids.push(id.clone());
                    }
                    Ok(None) => {} // still running
                    Err(e) => tracing::error!("try_wait failed for agent {}: {}", id, e),
                }
            }
            // Remove dead entries so we can re-insert fresh ones below.
            for id in &dead_ids {
                g.agents.remove(id);
            }

            (
                dead_ids,
                lc_addr,
                cc_addrs,
                extractor_script,
                image_base_path,
                python,
                omp,
            )
        }; // Lock released before spawning.

        // Spawn replacements outside the lock (process creation can be slow).
        let mut respawned = Vec::new();
        for agent_id in &dead_ids {
            match spawn_agent(
                agent_id,
                &lc_addr,
                &cc_addrs,
                &extractor_script,
                &image_base_path,
                &python,
                omp_threads,
            ) {
                Ok(agent) => {
                    tracing::info!("Respawned agent {}", agent_id);
                    respawned.push((agent_id.clone(), agent));
                }
                Err(e) => tracing::error!("Failed to respawn agent {}: {}", agent_id, e),
            }
        }

        if !respawned.is_empty() {
            let mut g = app.state.lock().await;
            for (id, agent) in respawned {
                g.agents.insert(id, agent);
            }
        }
    }
}

/// Periodically sends heartbeats to the cluster controller.
/// Errors are logged and ignored — transient CC unavailability is expected.
async fn heartbeat_loop(app: AppState) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Collect the data we need from state (brief lock, no I/O).
        let (
            node_id,
            routable_addr,
            agent_ids,
            load,
            agent_count,
            extractor_script,
            image_base_path,
            python,
        ) = {
            let g = app.state.lock().await;
            let agent_ids: Vec<String> = g.agents.keys().cloned().collect();
            let in_flight = g
                .agents
                .values()
                .filter(|a| a.current_batch_id.is_some())
                .count() as f64;
            let load = if agent_ids.is_empty() {
                0.0
            } else {
                in_flight / agent_ids.len() as f64
            };
            // Use the hostname from node_id (strip "lc-" prefix) so the CC
            // gets a routable address instead of "0.0.0.0".
            let hostname = g
                .config
                .node_id
                .strip_prefix("lc-")
                .unwrap_or(&g.config.node_id);
            let addr = format!("{}:{}", hostname, g.config.bind.port());
            (
                g.config.node_id.clone(),
                addr,
                agent_ids,
                load,
                g.config.agent_count,
                g.config.extractor_script.clone(),
                g.config.image_base_path.clone(),
                g.config.python.clone(),
            )
        };

        let req = HeartbeatRequest {
            node_id,
            address: routable_addr,
            agent_ids,
            load,
            agent_count,
            extractor_script,
            image_base_path,
            python,
        };

        // find_leader + HTTP POST happen without holding the mutex.
        match find_leader(&app).await {
            Ok(leader) => {
                let url = format!("http://{}/heartbeat", leader);
                if let Err(e) = app.client.post(&url).json(&req).send().await {
                    tracing::warn!("Heartbeat to CC failed: {}", e);
                    // Clear stale leader cache so the next call re-discovers.
                    app.state.lock().await.current_leader = None;
                }
            }
            Err(e) => tracing::warn!("Heartbeat skipped — no leader found: {}", e),
        }
    }
}

// endregion

// region HTTP server

async fn start_http_server(app: AppState, bind: SocketAddr) -> anyhow::Result<()> {
    let router = Router::new()
        .route("/agent/heartbeat", post(handle_agent_heartbeat))
        .route("/agent/request_task", post(handle_agent_task_request))
        .route("/agent/complete", post(handle_agent_task_complete))
        .route("/activate", post(handle_activate))
        .with_state(app);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!("Local controller HTTP server listening on {}", bind);
    axum::serve(listener, router).await?;
    Ok(())
}

/// POST /activate — CC instructs this replica LC to spawn agents and take over for a failed node.
///
/// Idempotent: if already active, returns `activated: false` without spawning more agents.
async fn handle_activate(
    State(app): State<AppState>,
    Json(req): Json<ActivateRequest>,
) -> Json<ActivateResponse> {
    let mut g = app.state.lock().await;

    if g.is_active {
        return Json(ActivateResponse {
            activated: false,
            message: "already active".into(),
        });
    }

    tracing::info!(
        "Activating as replacement for failed LC {} — spawning {} agents",
        req.failed_node_id,
        req.agent_count
    );

    // Mark active *before* releasing the lock to prevent a concurrent
    // /activate request from passing the is_active check and double-spawning.
    g.is_active = true;

    // Update config so heartbeats and health checks reflect the new parameters.
    g.config.agent_count = req.agent_count;
    g.config.extractor_script = req.extractor_script.clone();
    g.config.image_base_path = req.image_base_path.clone();
    g.config.python = req.python.clone();

    let lc_addr = format!("127.0.0.1:{}", g.config.bind.port());
    let cc_addrs = g.config.cc_addrs.clone();
    let node_id = g.config.node_id.clone();
    let omp_threads = effective_omp_threads(g.config.omp_threads, req.agent_count);

    // Spawn agents outside the lock (process creation can be slow).
    drop(g);

    let mut spawned = Vec::with_capacity(req.agent_count);
    for i in 0..req.agent_count {
        let agent_id = format!("{}-agent-{}", node_id, i);
        match spawn_agent(
            &agent_id,
            &lc_addr,
            &cc_addrs,
            &req.extractor_script,
            &req.image_base_path,
            &req.python,
            omp_threads,
        ) {
            Ok(agent) => {
                tracing::info!("Spawned agent {}", agent_id);
                spawned.push((agent_id, agent));
            }
            Err(e) => tracing::error!("Failed to spawn agent {}: {}", agent_id, e),
        }
    }

    let count = spawned.len();
    let mut g = app.state.lock().await;
    for (id, agent) in spawned {
        g.agents.insert(id, agent);
    }

    Json(ActivateResponse {
        activated: true,
        message: format!("spawned {} agents", count),
    })
}

/// POST /agent/heartbeat — agent reports it is alive.
async fn handle_agent_heartbeat(
    State(app): State<AppState>,
    Json(heartbeat): Json<AgentHeartbeat>,
) -> StatusCode {
    let mut g = app.state.lock().await;
    if let Some(agent) = g.agents.get_mut(&heartbeat.agent_id) {
        agent.last_heartbeat = unix_now();
        agent.current_batch_id = heartbeat.current_batch_id;
    } else {
        tracing::debug!("Heartbeat from unknown agent {}", heartbeat.agent_id);
    }
    StatusCode::OK
}

/// POST /agent/request_task — agent requests a work batch; proxied to CC leader.
async fn handle_agent_task_request(
    State(app): State<AppState>,
    Json(request): Json<TaskRequest>,
) -> Result<Json<TaskAssignment>, StatusCode> {
    proxy_to_leader::<_, TaskAssignment>(&app, "/task/request", &request)
        .await
        .map(Json)
        .map_err(|e| {
            tracing::error!("Task request proxy failed: {}", e);
            StatusCode::SERVICE_UNAVAILABLE
        })
}

/// POST /agent/complete — agent reports batch completion; proxied to CC leader.
async fn handle_agent_task_complete(
    State(app): State<AppState>,
    Json(completion): Json<TaskCompletion>,
) -> Result<Json<TaskCompletionResponse>, StatusCode> {
    proxy_to_leader::<_, TaskCompletionResponse>(&app, "/task/complete", &completion)
        .await
        .map(Json)
        .map_err(|e| {
            tracing::error!("Task completion proxy failed: {}", e);
            StatusCode::SERVICE_UNAVAILABLE
        })
}

// endregion

// region utilities

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Compute the effective OMP_NUM_THREADS value for each agent.
///
/// - If `configured` > 0, use it as-is (explicit override).
/// - If `configured` == 0 and `agent_count` <= 1, return 0 (let PyTorch use all cores).
/// - If `configured` == 0 and `agent_count` > 1, auto-partition: total_cores / agent_count.
fn effective_omp_threads(configured: usize, agent_count: usize) -> usize {
    if configured > 0 {
        return configured;
    }
    if agent_count <= 1 {
        return 0; // single agent — let it use all cores
    }
    // Auto-partition cores among agents to prevent OpenMP oversubscription.
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let per_agent = (cpus / agent_count).max(1);
    tracing::debug!(
        "Auto OMP threads: {} cores / {} agents = {} threads each",
        cpus,
        agent_count,
        per_agent
    );
    per_agent
}

// endregion
