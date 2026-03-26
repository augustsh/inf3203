/// Áika cluster deploy orchestrator.
///
/// Downloads the `aika-node` binary once to the current working directory
/// (which must be on the NFS shared filesystem so all cluster nodes can reach
/// it), then SSH-starts each role using that shared path.
/// No per-node binary download is needed.
///
/// Role allocation for N requested nodes (N ≥ 3):
///   Nodes 1–3  →  Cluster Controller  (3-node Raft quorum)
///   Node  4    →  Local Controller replica  (--agents 0, standby)
///   Nodes 5–N  →  Local Controller active   (--agents 4)
mod dashboard;

use rand::Rng;
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use dashboard::{DeployInfo, LogBuffer};

// region Constants

const VERSION: &str = "v0.0.1"; // replaced by CI at release

const NUM_CC: usize = 3;
const AGENTS_PER_LC: usize = 2;

const IMAGE_DIR: &str = "/share/inf3203/unlabeled_images";

// can't use nodes with "decent GPU capabilities"
const GPU_NODE_PATTERNS: &[&str] = &["RTX", "Quadro", "GTX"];

// endregion

// region Helpers

/// Priority order for node selection: c11 (0) > c9 (1) > c8 (2) > c7 (3) > other (4).
fn node_priority(hostname: &str) -> u8 {
    if hostname.starts_with("c11-") { 0 }
    else if hostname.starts_with("c9-") { 1 }
    else if hostname.starts_with("c8-") { 2 }
    else if hostname.starts_with("c7-") { 3 }
    else { 4 }
}

/// Sort candidates by priority group (c11 first), shuffling within each group.
/// Also filters out the local deploy node to avoid deploying to ourselves.
fn prioritize_nodes(mut candidates: Vec<String>, rng: &mut impl rand::Rng) -> Vec<String> {
    // Determine and filter out the local hostname.
    let local = Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    if !local.is_empty() {
        candidates.retain(|n| n != &local);
    }

    // Group by priority, shuffle each group, then concatenate.
    let mut groups: [Vec<String>; 5] = Default::default();
    for node in candidates {
        groups[node_priority(&node) as usize].push(node);
    }
    let mut result = Vec::new();
    for group in &mut groups {
        group.shuffle(rng);
        result.extend(group.drain(..));
    }
    result
}

/// Parse `/share/ifi/list-cluster-static.sh` and return a list of nodes with GPUs
fn gpu_node_hostnames() -> std::collections::HashSet<String> {
    let output = Command::new("/share/ifi/list-cluster-static.sh")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output();

    let output = match output {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Warning: could not run list-cluster-static.sh: {}", e);
            return std::collections::HashSet::new();
        }
    };

    let mut gpu_nodes = std::collections::HashSet::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let trimmed = line.trim();
        // Node lines start with a hostname (e.g. "c6-0, ...")
        // Skip header/separator/description lines.
        if trimmed.is_empty() || trimmed.starts_with('-') || trimmed.starts_with('N') {
            continue;
        }
        let fields: Vec<&str> = trimmed.splitn(10, ',').collect();
        if fields.len() < 9 {
            continue;
        }
        let hostname = fields[0].trim();
        let graphics = fields[8].trim().to_lowercase();
        if GPU_NODE_PATTERNS
            .iter()
            .any(|pat| graphics.contains(&pat.to_lowercase()))
        {
            gpu_nodes.insert(hostname.to_string());
        }
    }
    gpu_nodes
}

/// Check if the selected Port is in use
fn port_in_use(node: &str, port: u16) -> bool {
    Command::new("ssh")
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            "-o",
            "StrictHostKeyChecking=no",
            node,
        ])
        .arg(format!("ss -ltnp 2>/dev/null | grep -q ':{} '", port))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Finds a random free port on the node
fn find_free_port(node: &str, max_attempts: u32) -> Option<u16> {
    let mut rng = rand::rng();
    for _ in 0..max_attempts {
        let port: u16 = rng.random_range(49152..=65000);
        if !port_in_use(node, port) {
            return Some(port);
        }
        thread::sleep(Duration::from_millis(100));
    }
    None
}

/// SSH to `node` and run a shell command.
fn ssh_run(node: &str, cmd: &str) -> bool {
    Command::new("ssh")
        .args([
            "-n", // do not read stdin; prevents SSH from keeping the connection
            // open waiting for the deploy process's terminal
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "StrictHostKeyChecking=no",
            node,
        ])
        .arg(cmd)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or_else(|e| {
            eprintln!("ssh {} failed: {}", node, e);
            false
        })
}

/// SSH to `node` and start an Aika node as a detached background process.
/// stdout and stderr are appended to `log_file` (must be an NFS path visible from the remote node).
/// Waits 2s then verifies the process is actually running.
fn ssh_start(node: &str, binary: &str, args: &str, log_file: &str) -> bool {
    let cmd = format!(
        "nohup {} {} </dev/null >>{} 2>&1 &",
        binary, args, log_file
    );
    if !ssh_run(node, &cmd) {
        return false;
    }
    // Give the process a moment to start (or crash immediately).
    thread::sleep(Duration::from_secs(2));
    ssh_run(node, "pgrep -f '[i]nf3203_aika' > /dev/null 2>&1")
}

/// Ensure a Python virtual environment with the required packages exists in
/// `install_dir/inf3203_venv`. Returns the path to the venv's `python` binary.
fn ensure_venv(install_dir: &Path, classify_script: &Path, log_buf: &LogBuffer) -> PathBuf {
    let venv_dir = install_dir.join("inf3203_venv");
    let python = venv_dir.join("bin/python");

    if python.exists() {
        log_buf.push(format!("venv already present: {}", venv_dir.display()));
        return python;
    }

    log_buf.push(format!("Creating Python venv at {}…", venv_dir.display()));
    let status = Command::new("python3")
        .args(["-m", "venv"])
        .arg(&venv_dir)
        .status()
        .expect("python3 not found — cannot create venv");
    if !status.success() {
        log_buf.push(format!("ERROR: Failed to create venv"));
        std::process::exit(1);
    }

    // Derive requirements.txt path: it lives next to classify.py
    let requirements = classify_script.parent().unwrap_or(install_dir).join("requirements.txt");
    if requirements.exists() {
        log_buf.push(format!("Installing packages from {}…", requirements.display()));
        let pip = venv_dir.join("bin/pip");
        let status = Command::new(&pip)
            .args(["install", "-r"])
            .arg(&requirements)
            .status()
            .expect("pip not found in venv");
        if !status.success() {
            log_buf.push(format!("ERROR: pip install failed"));
            std::process::exit(1);
        }
    } else {
        log_buf.push(format!(
            "Warning: requirements.txt not found at {}",
            requirements.display()
        ));
    }

    log_buf.push(format!("venv ready: {}", python.display()));
    python
}

/// Ensure `classify.py` is present in `install_dir`. If not download from GitHub.
fn ensure_classify_script(install_dir: &Path, log_buf: &LogBuffer) -> PathBuf {
    let dest = install_dir.join("classify.py");
    if dest.exists() {
        log_buf.push(format!("classify.py already present: {}", dest.display()));
        return dest;
    }

    let url = format!(
        "https://github.com/augustsh/inf3203/releases/download/{}/batch_classify.py",
        VERSION
    );
    log_buf.push(format!("Downloading classify.py {}…", VERSION));

    let status = Command::new("curl")
        .args(["-fsS", "--no-progress-meter", "-L", "-o"])
        .arg(&dest)
        .arg(&url)
        .status()
        .expect("curl not found");

    if !status.success() {
        log_buf.push(format!("ERROR: Download failed: {}", url));
        std::process::exit(1);
    }

    log_buf.push(format!("Installed: {}", dest.display()));
    dest
}

/// Ensure the `aika-node` binary and `inf3203_aika` symlink are present in `install_dir`.
/// Download binary from GitHub if absent.
fn ensure_binary(install_dir: &Path, log_buf: &LogBuffer) -> PathBuf {
    let symlink = install_dir.join("inf3203_aika");
    if symlink.exists() {
        log_buf.push(format!("Binary already present: {}", symlink.display()));
        return symlink;
    }

    let url = format!(
        "https://github.com/augustsh/inf3203/releases/download/{}/aika-node-x86_64-unknown-linux-musl.tar.gz",
        VERSION
    );
    log_buf.push(format!("Downloading aika-node {}…", VERSION));

    let tarball = install_dir.join("aika-node.tar.gz");
    let status = Command::new("curl")
        .args(["-fsS", "--no-progress-meter", "-L", "-o"])
        .arg(&tarball)
        .arg(&url)
        .status()
        .expect("curl not found");

    if !status.success() {
        log_buf.push(format!("ERROR: Download failed: {}", url));
        std::process::exit(1);
    }

    let status = Command::new("tar")
        .args(["-xzf"])
        .arg(&tarball)
        .arg("-C")
        .arg(install_dir)
        .status()
        .expect("tar not found");

    if !status.success() {
        log_buf.push(format!("ERROR: Failed to extract tarball"));
        std::process::exit(1);
    }

    fs::remove_file(&tarball).ok();

    let binary = install_dir.join("aika-node");
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).expect("chmod failed");

    // Symlink: the running process will appear as inf3203_aika in ps/pgrep,
    // satisfying the cluster's process-naming requirement.
    std::os::unix::fs::symlink(&binary, &symlink).expect("symlink failed");

    log_buf.push(format!("Installed: {}", symlink.display()));
    symlink
}

// endregion

// region Main

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() >= 2 && args[1] == "cleanup" {
        let cwd: PathBuf = env::current_dir().expect("cannot determine current directory");
        run_cleanup(&cwd);
        return;
    }

    if args.len() >= 2 && args[1] == "merge" {
        let cwd: PathBuf = env::current_dir().expect("cannot determine current directory");
        merge_results(&cwd);
        return;
    }

    let cwd: PathBuf = env::current_dir().expect("cannot determine current directory");

    let num_nodes: usize;
    let max_images: u64;
    let num_cc: usize;
    let num_replicas: usize;
    let agents_per_lc: usize;
    let task_ttl_secs: u64;
    let batch_size: usize;

    // --- Discover available nodes ---
    let gpu_nodes = gpu_node_hostnames();
    let raw = Command::new("/share/ifi/available-nodes.sh")
        .stdout(Stdio::piped())
        .output()
        .expect("failed to run /share/ifi/available-nodes.sh");

    let candidates: Vec<String> = String::from_utf8_lossy(&raw.stdout)
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && !gpu_nodes.contains(s.as_str()))
        .collect();

    if candidates.len() < 3 {
        eprintln!(
            "Not enough non-GPU nodes available (found {}, need at least 3).",
            candidates.len(),
        );
        std::process::exit(1);
    }

    // --- Configuration: startup TUI or CLI args ---
    if args.len() >= 2 {
        // Legacy CLI mode: deploy <num_nodes> [max_images]
        num_nodes = args[1].parse().expect("num_nodes must be a positive integer");
        max_images = if args.len() > 2 { args[2].parse().unwrap_or(0) } else { 0 };
        num_cc = NUM_CC;
        num_replicas = 1;
        agents_per_lc = AGENTS_PER_LC;
        task_ttl_secs = 60;
        batch_size = 50;
    } else {
        // Interactive startup TUI
        match dashboard::run_startup_tui(candidates.len()) {
            Ok(Some(cfg)) => {
                num_nodes = cfg.num_nodes;
                max_images = cfg.max_images;
                num_cc = cfg.num_cc;
                num_replicas = cfg.num_replicas;
                agents_per_lc = cfg.agents_per_lc;
                task_ttl_secs = cfg.task_ttl_secs;
                batch_size = cfg.batch_size;
            }
            Ok(None) => {
                // User pressed q
                return;
            }
            Err(e) => {
                eprintln!("TUI error: {}", e);
                std::process::exit(1);
            }
        }
    }

    if num_nodes < num_cc {
        eprintln!(
            "Need at least {} nodes for {} CCs (requested {}).",
            num_cc, num_cc, num_nodes
        );
        std::process::exit(1);
    }

    // --- Set up shared state for TUI + deploy thread ---
    let log_buf = LogBuffer::new(500);
    let deploy_info: Arc<Mutex<Option<DeployInfo>>> = Arc::new(Mutex::new(None));
    let dashboard_state = Arc::new(Mutex::new(DashboardState {
        lc_crashes: 0,
        lc_restarts: 0,
        cc_crashes: 0,
        cc_restarts: 0,
        lc_replacements: 0,
        throughput_history: Vec::new(),
        per_node_history: HashMap::new(),
        ttl_history: Vec::new(),
        fault_events: Vec::new(),
        config_snapshot: None,
        start_time_unix: None,
        last_status: None,
        start_time: None,
        completed_at: None,
        cc_statuses: Vec::new(),
        telemetry_file: String::new(),
        telemetry_saved: false,
    }));

    // Spawn the deploy work in a background thread so the TUI is live immediately.
    {
        let log_buf = log_buf.clone();
        let deploy_info = Arc::clone(&deploy_info);
        let dashboard_state = Arc::clone(&dashboard_state);
        let cwd = cwd.clone();
        thread::spawn(move || {
            run_deploy(
                cwd, num_nodes, num_cc, num_replicas, agents_per_lc,
                task_ttl_secs, batch_size, max_images, candidates,
                log_buf, deploy_info, dashboard_state,
            );
        });
    }

    if let Err(e) = dashboard::run_tui(dashboard_state, deploy_info, log_buf) {
        eprintln!("TUI error: {}", e);
    }
    // Kill all cluster processes when the dashboard exits.
    run_teardown(&cwd);
}

/// Runs the deploy sequence in a background thread: downloads binaries,
/// SSH-starts CCs and LCs, then spawns watchdog threads.
fn run_deploy(
    cwd: PathBuf,
    num_nodes: usize,
    num_cc: usize,
    num_replicas: usize,
    agents_per_lc: usize,
    task_ttl_secs: u64,
    batch_size: usize,
    max_images: u64,
    candidates: Vec<String>,
    log_buf: LogBuffer,
    deploy_info: Arc<Mutex<Option<DeployInfo>>>,
    dashboard_state: Arc<Mutex<DashboardState>>,
) {
    let args: Vec<String> = env::args().collect();

    if max_images > 0 {
        log_buf.push(format!("Image limit: {} images", max_images));
    }

    // ensure that the aika binary is in the user directory (in the distributed filesystem)
    let username = env::var("USER")
        .or_else(|_| env::var("LOGNAME"))
        .unwrap_or_else(|_| "unknown".into());

    let binary = ensure_binary(&cwd, &log_buf);
    let binary_str = binary.to_str().expect("non-UTF8 path");

    let classify_script = ensure_classify_script(&cwd, &log_buf);
    let classify_script_str = classify_script.to_str().expect("non-UTF8 path");

    let venv_python = ensure_venv(&cwd, &classify_script, &log_buf);
    let venv_python_str = venv_python.to_str().expect("non-UTF8 path");

    // Create the results directory on the shared filesystem so all CC nodes can
    // write their NDJSON output to a single accessible location.
    let results_dir = cwd.join("inf3203_data");
    fs::create_dir_all(&results_dir).expect("failed to create inf3203_data dir");
    let results_dir_str = results_dir.to_str().expect("non-UTF8 path");

    let log_dir = results_dir.join("logs");
    fs::create_dir_all(&log_dir).expect("failed to create logs dir");
    let log_dir_str = log_dir.to_str().expect("non-UTF8 path").to_string();

    // Clear stale logs and result files from previous runs (keep telemetry_*.json).
    let mut cleared = 0usize;
    if let Ok(entries) = fs::read_dir(&log_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("log") {
                let _ = fs::remove_file(&path);
                cleared += 1;
            }
        }
    }
    if let Ok(entries) = fs::read_dir(&results_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("results_") && name.ends_with(".ndjson") {
                    let _ = fs::remove_file(&path);
                    cleared += 1;
                }
            }
        }
    }
    if cleared > 0 {
        log_buf.push(format!("[deploy] Cleared {} stale log/result files from previous run.", cleared));
    }

    let mut rng = rand::rng();
    let candidates = prioritize_nodes(candidates, &mut rng);

    let nodes: Vec<String> = candidates.into_iter().take(num_nodes).collect();
    log_buf.push(format!(
        "Deploying Áika cluster: {} nodes ({} CC, {} LC)",
        nodes.len(),
        num_cc,
        nodes.len().saturating_sub(num_cc)
    ));

    // assign ports to CC nodes and build the peer list
    let cc_nodes = &nodes[..num_cc];
    let lc_nodes = &nodes[num_cc..];

    log_buf.push("[1/3] Assigning ports to cluster controllers…".into());
    let mut cc_assignments: Vec<(String, u16)> = Vec::new();
    for node in cc_nodes {
        match find_free_port(node, 30) {
            Some(port) => cc_assignments.push((node.clone(), port)),
            None => {
                log_buf.push(format!("ERROR: Could not find a free port on CC node {}.", node));
                return;
            }
        }
    }

    let peer_list: String = cc_assignments
        .iter()
        .map(|(n, p)| format!("{}:{}", n, p))
        .collect::<Vec<_>>()
        .join(",");

    // start Cluster Controllers
    log_buf.push(format!("[2/3] Starting {} cluster controllers…", num_cc));
    let mut cc_started: Vec<String> = Vec::new();
    // (node, binary, args) — used by the watchdog to restart crashed CCs.
    let mut cc_watchlist: Vec<(String, String, String)> = Vec::new();

    for (i, (node, port)) in cc_assignments.iter().enumerate() {
        let node_id = i + 1;
        let data_dir = format!("/tmp/inf3203_{}_{}", username, node_id);
        // Restart args do NOT include --hold: a restarted CC should rejoin immediately
        // without waiting for POST /start (which was already sent during initial deploy).
        let target_active_lc_count = lc_nodes.len().saturating_sub(num_replicas);
        let node_args = format!(
            "cluster-controller --node-id {} --bind 0.0.0.0:{} --peers {} --image-dir {} \
             --data-dir {} --results-dir {} \
             --batch-size {} --task-ttl-secs {} \
             --heartbeat-interval-ms 200 --election-timeout-min-ms 1000 --election-timeout-max-ms 3000 \
             --lc-heartbeat-timeout-secs 6 \
             --max-images {} \
             --target-active-lc-count {} --agents-per-lc {}",
            node_id, port, peer_list, IMAGE_DIR, data_dir, results_dir_str,
            batch_size, task_ttl_secs, max_images,
            target_active_lc_count, agents_per_lc
        );
        let log_file = format!("{}/cc-{}.log", log_dir_str, node_id);
        // Wipe old Raft state on the target node (start fresh every time we deploy new cluster).
        // Initial start uses --hold; restarts (in watchdog) do not.
        let cmd = format!(
            "rm -rf {} ; nohup {} {} --hold </dev/null >>{} 2>&1 &",
            data_dir, binary_str, node_args, log_file
        );
        if ssh_run(node, &cmd) {
            log_buf.push(format!("  CC{} @ {}:{} … ok", node_id, node, port));
            cc_started.push(format!("{}:{}", node, port));
            cc_watchlist.push((node.clone(), binary_str.to_string(), node_args));
        } else {
            log_buf.push(format!("  CC{} @ {}:{} … FAILED", node_id, node, port));
        }
    }

    if cc_started.len() < num_cc {
        log_buf.push(format!(
            "ERROR: Only {}/{} CCs started — quorum requires all {}. Aborting.",
            cc_started.len(),
            num_cc,
            num_cc
        ));
        return;
    }

    // (node, binary, args) — used by the watchdog to restart crashed LCs.
    let mut lc_watchlist: Vec<(String, String, String)> = Vec::new();

    // start Local Controllers
    if lc_nodes.is_empty() {
        log_buf.push(format!("[3/3] No LC nodes — CC-only deployment."));
    } else {
        let num_active = lc_nodes.len().saturating_sub(num_replicas);
        log_buf.push(format!(
            "[3/3] Starting {} local controllers ({} standby replica, {} active)…",
            lc_nodes.len(),
            num_replicas,
            num_active
        ));

        let mut lc_started = 0usize;

        for (i, node) in lc_nodes.iter().enumerate() {
            let is_replica = i < num_replicas;
            let agents = if is_replica { 0 } else { agents_per_lc };
            let label = if is_replica { "replica" } else { "active " };

            let port = match find_free_port(node, 30) {
                Some(p) => p,
                None => {
                    log_buf.push(format!("  No free port on {} — skipping.", node));
                    continue;
                }
            };

            let node_id = format!("lc-{}", node);
            let lc_args = format!(
                "local-controller --node-id {} --bind 0.0.0.0:{} --cc-addrs {} \
                 --agents {} --extractor-script {} --image-base-path {} --python {}",
                node_id, port, peer_list, agents, classify_script_str, IMAGE_DIR, venv_python_str
            );
            let restart_args_template = format!(
                "local-controller --node-id {} --bind 0.0.0.0:{{PORT}} --cc-addrs {} \
                 --agents 0 --extractor-script {} --image-base-path {} --python {}",
                node_id, peer_list, classify_script_str, IMAGE_DIR, venv_python_str
            );

            let log_file = format!("{}/lc-{}.log", log_dir_str, node);
            let cmd = format!(
                "nohup {} {} </dev/null >>{} 2>&1 &",
                binary_str, lc_args, log_file
            );
            if ssh_run(node, &cmd) {
                log_buf.push(format!("  LC [{}] {} @ {}:{} … ok", label, node_id, node, port));
                lc_started += 1;
                lc_watchlist.push((node.clone(), node_id, restart_args_template));
            } else {
                log_buf.push(format!("  LC [{}] {} @ {}:{} … FAILED", label, node_id, node, port));
            }
        }

        log_buf.push(format!(
            "  {}/{} local controllers started.",
            lc_started,
            lc_nodes.len()
        ));
    }

    // save node list and print summary
    let nodes_file = cwd.join(".inf3203_nodes");
    if let Err(e) = fs::write(&nodes_file, nodes.join("\n")) {
        log_buf.push(format!("Warning: could not save node list: {}", e));
    }

    log_buf.push(format!("══════════════════════════════════════════"));
    log_buf.push(format!("  Áika cluster deployed successfully"));
    log_buf.push(format!("══════════════════════════════════════════"));
    log_buf.push(format!("  Binary:   {}", binary_str));
    log_buf.push(format!("  Results:  {}", results_dir_str));
    log_buf.push(format!("  Logs:     {}", log_dir_str));
    log_buf.push(format!("  CCs:      {}", cc_started.join("  ")));
    log_buf.push(format!("  Status:   curl http://{}/status", cc_started[0]));
    log_buf.push(format!("  Leader:   curl http://{}/leader", cc_started[0]));
    log_buf.push(format!("  Log tail: tail -f {}/*.log", log_dir_str));
    log_buf.push(format!(
        "  Teardown: xargs -a {} -I{{}} ssh {{}} \"pkill -f inf3203_aika; true\"",
        nodes_file.display()
    ));
    log_buf.push(format!("  Cleanup:  {} cleanup", args[0]));
    log_buf.push(format!("══════════════════════════════════════════"));

    if !lc_watchlist.is_empty() {
        log_buf.push(format!("Watchdog active."));
        let ctx = WatchdogContext {
            binary: binary_str.to_string(),
            classify_script: classify_script_str.to_string(),
            peer_list: peer_list.clone(),
            log_dir: log_dir_str.clone(),
            python: venv_python_str.to_string(),
        };

        let info = DeployInfo {
            binary: binary_str.to_string(),
            results_dir: results_dir_str.to_string(),
            log_dir: log_dir_str,
            cc_addrs: cc_started.clone(),
            num_nodes: nodes.len(),
            num_cc,
            num_lc: nodes.len().saturating_sub(num_cc),
            nodes_file: nodes_file.display().to_string(),
            cc_nodes: cc_nodes.to_vec(),
            lc_nodes: lc_nodes.to_vec(),
        };

        // Populate shared state so the TUI can see deploy results.
        let initial_cc_statuses: Vec<CcNodeStatus> = cc_started
            .iter()
            .map(|addr| CcNodeStatus {
                addr: addr.clone(),
                alive: true,
                is_leader: false,
            })
            .collect();

        {
            let num_active = lc_nodes.len().saturating_sub(num_replicas);
            let mut d = dashboard_state.lock().unwrap();
            d.cc_statuses = initial_cc_statuses;
            d.telemetry_file = format!("{}/telemetry_{}.json", results_dir_str, timestamp_str());
            d.config_snapshot = Some(ConfigSnapshot {
                num_cc,
                num_lc: lc_nodes.len(),
                active_lc: num_active,
                replica_lc: num_replicas,
                agents_per_lc,
                batch_size,
                task_ttl_secs,
            });
        }
        *deploy_info.lock().unwrap() = Some(info);

        // Spawn watchdog threads (they run until the process exits).
        {
            let state = Arc::clone(&dashboard_state);
            let log = log_buf.clone();
            let addrs = cc_started.clone();
            thread::spawn(move || {
                telemetry_poller(addrs, state, log);
            });
        }
        {
            let state = Arc::clone(&dashboard_state);
            let log = log_buf.clone();
            let log_dir = ctx.log_dir.clone();
            thread::spawn(move || {
                cc_monitor(cc_watchlist, log_dir, state, log);
            });
        }
        {
            let state = Arc::clone(&dashboard_state);
            let log = log_buf.clone();
            thread::spawn(move || {
                lc_monitor(lc_watchlist, ctx, state, log);
            });
        }
    } else {
        // CC-only deployment — still populate deploy_info for the TUI header.
        let info = DeployInfo {
            binary: binary_str.to_string(),
            results_dir: results_dir_str.to_string(),
            log_dir: log_dir_str,
            cc_addrs: cc_started.clone(),
            num_nodes: nodes.len(),
            num_cc,
            num_lc: 0,
            nodes_file: nodes_file.display().to_string(),
            cc_nodes: cc_nodes.to_vec(),
            lc_nodes: vec![],
        };
        *deploy_info.lock().unwrap() = Some(info);
    }
}

// Watchdog (set up using Claude Code Sonnet 4.6 in Agent Mode)
// Created in three steps
// First prompt asked to create a watchdog that can restart crashed LC (Local Controller) nodes,
// since the current solution can only handle one crash before we run out of replicas, so we need
// to replenish the replica so that we can handle further failures.

// Second prompt asked to also handle cases where the LC nodes goes completely down, in those cases
// we should find a new available node and start a new LC there. After the first prompt the node
// was always restarted with agents, meaning that we could get one more working node and lose our
// replica. Instead, we wanted the standby replica to activate, and restart the crashed node as a
// new standby replica. To accomplish this we need the new node to be restarted on a delay, so that
// the CC has time to detect the failure, promote the replica to active, and only then we restart
// the crashed node. Without the delay we risk that the replica isn't activated, and we're left with
// two standby replicas and one less working node.

// Third prompt asked to also track the CC (Cluster Controller) nodes, and restart them on failure.
// Since our basic Raft implementation does not support dynamic joining/leaving, we should not
// handle completely downed nodes, just restarting on the same node. We should also refrain from
// clearing the logs since we need the persistent logs for the node to rejoin the quorum.

/// Context needed by the watchdog to reschedule LCs onto replacement nodes.
struct WatchdogContext {
    binary: String,
    classify_script: String,
    peer_list: String,
    log_dir: String,
    python: String,
}

/// One entry in the watchdog's tracking list.
struct WatchdogEntry {
    node: String,
    node_id: String,
    /// Always `--agents 0` — restarts rejoin as standby replica.
    /// Contains a `{PORT}` placeholder that gets replaced with a fresh port.
    restart_args_template: String,
    /// How many consecutive restart attempts have failed for this entry.
    failed_attempts: u32,
    /// When we first detected this node as down in the current failure episode.
    down_since: Option<std::time::Instant>,
}

/// Telemetry snapshot received from the CC /status endpoint.
#[derive(Default, Clone, serde::Deserialize)]
struct StatusResponse {
    #[serde(default)]
    total_tasks: u64,
    #[serde(default)]
    pending_tasks: u64,
    #[serde(default)]
    assigned_tasks: u64,
    #[serde(default)]
    completed_tasks: u64,
    #[serde(default)]
    registered_nodes: Vec<NodeInfoResp>,
    #[serde(default)]
    stale_nodes: Vec<String>,
    #[serde(default)]
    telemetry: TelemetryResp,
}

#[derive(Default, Clone, serde::Deserialize)]
struct NodeInfoResp {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    agent_count: usize,
}

#[derive(Default, Clone, serde::Deserialize)]
struct TelemetryResp {
    #[serde(default)]
    total_images: u64,
    #[serde(default)]
    completed_images: u64,
    #[serde(default)]
    ttl_expirations: u64,
    #[serde(default)]
    total_assignments: u64,
    #[serde(default)]
    total_completions: u64,
    #[serde(default)]
    first_completion_at: Option<u64>,
    #[serde(default)]
    last_completion_at: Option<u64>,
    #[serde(default)]
    started_at: Option<u64>,
    #[serde(default)]
    per_node_completions: Vec<(String, u64)>,
    #[serde(default)]
    per_node_images: Vec<(String, u64)>,
    #[serde(default)]
    per_node_ttl_expirations: Vec<(String, u64)>,
    #[serde(default)]
    batch_size: usize,
    #[serde(default)]
    max_images: u64,
}

/// Per-CC status tracked by the telemetry poller.
#[derive(Clone)]
struct CcNodeStatus {
    addr: String,
    alive: bool,
    is_leader: bool,
}

/// A timestamped fault event (node crash, restart, or replacement).
struct FaultEvent {
    ts: u64,
    kind: &'static str, // "lc_crash" | "lc_restart" | "lc_replacement" | "cc_crash" | "cc_restart"
    node: String,
}

/// Snapshot of the deployment configuration for telemetry output.
struct ConfigSnapshot {
    num_cc: usize,
    num_lc: usize,
    active_lc: usize,
    replica_lc: usize,
    agents_per_lc: usize,
    batch_size: usize,
    task_ttl_secs: u64,
}

/// Dashboard state accumulated across polling cycles.
struct DashboardState {
    lc_crashes: u32,
    lc_restarts: u32,
    cc_crashes: u32,
    cc_restarts: u32,
    lc_replacements: u32,
    /// History of (unix_timestamp, completed_batches) for throughput calculation.
    throughput_history: Vec<(u64, u64)>,
    /// Per-node image history: node_id -> Vec<(unix_timestamp, images_total)>.
    per_node_history: HashMap<String, Vec<(u64, u64)>>,
    /// History of (unix_timestamp, cumulative_ttl_expirations).
    ttl_history: Vec<(u64, u64)>,
    /// Timestamped fault events (crashes, restarts, replacements).
    fault_events: Vec<FaultEvent>,
    /// Deployment config snapshot (set after deploy completes).
    config_snapshot: Option<ConfigSnapshot>,
    /// Unix timestamp when the cluster was started (user pressed 's').
    start_time_unix: Option<u64>,
    last_status: Option<StatusResponse>,
    /// Set when the user presses 's' (start). `None` means timer not started yet.
    start_time: Option<std::time::Instant>,
    /// Frozen when all tasks are completed so the timer stops ticking.
    completed_at: Option<std::time::Instant>,
    /// Per-CC liveness and leader status (updated by telemetry_poller).
    cc_statuses: Vec<CcNodeStatus>,
    /// Path to write telemetry JSON on completion.
    telemetry_file: String,
    /// Whether we already persisted telemetry.
    telemetry_saved: bool,
}

/// Persist telemetry snapshot to JSON for later analysis/figures.
fn save_telemetry(state: &DashboardState, path: &str) {
    use std::io::Write;

    let elapsed = state
        .completed_at
        .unwrap_or_else(std::time::Instant::now)
        .duration_since(state.start_time.unwrap_or_else(std::time::Instant::now))
        .as_secs_f64();

    let total_images = state.last_status.as_ref().map_or(0, |s| s.telemetry.completed_images);

    // Global throughput: avg and max from throughput_history consecutive pairs.
    let global_avg = if elapsed > 0.0 { total_images as f64 / elapsed } else { 0.0 };
    let global_max = state.throughput_history
        .windows(2)
        .filter_map(|w| {
            let dt = w[1].0.saturating_sub(w[0].0);
            let dc = w[1].1.saturating_sub(w[0].1);
            if dt == 0 { return None; }
            // completed_tasks → multiply by batch_size for image rate
            let batch_size = state.last_status.as_ref().map_or(1, |s| s.telemetry.batch_size.max(1));
            Some(dc as f64 * batch_size as f64 / dt as f64)
        })
        .fold(0.0f64, f64::max);

    // Per-node throughput stats from per_node_history.
    let per_node_stats: HashMap<String, serde_json::Value> = state.per_node_history.iter()
        .map(|(node, hist)| {
            let rates: Vec<f64> = hist.windows(2)
                .filter_map(|w| {
                    let dt = w[1].0.saturating_sub(w[0].0);
                    let dc = w[1].1.saturating_sub(w[0].1);
                    if dt == 0 { return None; }
                    Some(dc as f64 / dt as f64)
                })
                .collect();
            let avg = if rates.is_empty() { 0.0 }
                else { rates.iter().sum::<f64>() / rates.len() as f64 };
            let min = rates.iter().cloned().fold(f64::MAX, f64::min);
            let max = rates.iter().cloned().fold(0.0f64, f64::max);
            let stats = serde_json::json!({
                "avg_imgs_per_sec": avg,
                "min_imgs_per_sec": if rates.is_empty() { 0.0 } else { min },
                "max_imgs_per_sec": max,
            });
            (node.clone(), stats)
        })
        .collect();

    let data = serde_json::json!({
        "config": state.config_snapshot.as_ref().map(|c| serde_json::json!({
            "num_cc": c.num_cc,
            "num_lc": c.num_lc,
            "active_lc": c.active_lc,
            "replica_lc": c.replica_lc,
            "agents_per_lc": c.agents_per_lc,
            "batch_size": c.batch_size,
            "task_ttl_secs": c.task_ttl_secs,
        })),
        "run": {
            "start_time_unix": state.start_time_unix,
            "elapsed_secs": elapsed,
            "total_images": state.last_status.as_ref().map_or(0, |s| s.telemetry.total_images),
            "completed_images": total_images,
        },
        "throughput": {
            "global_avg_imgs_per_sec": global_avg,
            "global_max_imgs_per_sec": global_max,
            "history": state.throughput_history.iter()
                .map(|(t, c)| {
                    let batch_size = state.last_status.as_ref()
                        .map_or(1, |s| s.telemetry.batch_size.max(1));
                    serde_json::json!({"ts": t, "completed_images": c * batch_size as u64})
                })
                .collect::<Vec<_>>(),
        },
        "per_node": per_node_stats,
        "ttl_expirations": {
            "total": state.last_status.as_ref().map_or(0, |s| s.telemetry.ttl_expirations),
            "history": state.ttl_history.iter()
                .map(|(t, v)| serde_json::json!({"ts": t, "total": v}))
                .collect::<Vec<_>>(),
        },
        "fault_events": state.fault_events.iter()
            .map(|e| serde_json::json!({"ts": e.ts, "type": e.kind, "node": e.node}))
            .collect::<Vec<_>>(),
        "fault_summary": {
            "lc_crashes": state.lc_crashes,
            "lc_restarts": state.lc_restarts,
            "cc_crashes": state.cc_crashes,
            "cc_restarts": state.cc_restarts,
            "lc_replacements": state.lc_replacements,
        },
    });

    if let Ok(mut f) = std::fs::File::create(path) {
        let _ = f.write_all(serde_json::to_string_pretty(&data).unwrap_or_default().as_bytes());
    }
}

/// Poll one of the CC addresses for the /status endpoint.
fn poll_status(cc_addrs: &[String]) -> Option<StatusResponse> {
    // Try to find the leader first — only the leader has heartbeat data
    // (registered_nodes, per-node stats). Polling a follower gives stale node info.
    let mut leader_addr: Option<String> = None;
    for addr in cc_addrs {
        let output = Command::new("curl")
            .args(["-s", "--connect-timeout", "2", "--max-time", "3"])
            .arg(format!("http://{}/leader", addr))
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output();
        if let Ok(out) = output {
            if out.status.success() {
                if let Ok(resp) = serde_json::from_slice::<LeaderResponse>(&out.stdout) {
                    if let Some(la) = resp.leader_address {
                        leader_addr = Some(la);
                        break;
                    }
                }
            }
        }
    }

    // Build the list of addresses to try: leader first, then all others as fallback.
    let mut addrs_to_try: Vec<&str> = Vec::new();
    if let Some(ref la) = leader_addr {
        addrs_to_try.push(la.as_str());
    }
    for addr in cc_addrs {
        if leader_addr.as_deref() != Some(addr.as_str()) {
            addrs_to_try.push(addr.as_str());
        }
    }

    for addr in addrs_to_try {
        let url = format!("http://{}/status", addr);

        let output = Command::new("curl")
            .args(["-s", "--connect-timeout", "3", "--max-time", "5"])
            .arg(&url)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output();

        if let Ok(out) = output {
            if out.status.success() {
                if let Ok(status) = serde_json::from_slice::<StatusResponse>(&out.stdout) {
                    return Some(status);
                }
            }
        }
    }
    None
}

/// Format a duration into human-readable form.
fn fmt_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{}h{:02}m{:02}s", h, m, s)
    } else if m > 0 {
        format!("{}m{:02}s", m, s)
    } else {
        format!("{}s", s)
    }
}

/// Format a large number with commas.
fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Format current UTC time as "YYYYMMDD_HHMMSS" from a unix timestamp.
fn timestamp_str() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Manual UTC breakdown (no DST, no external crate needed).
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    // Days since epoch → approximate year/month/day.
    let days = secs / 86400;
    let mut year = 1970u32;
    let mut rem = days;
    loop {
        let dy = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 366 } else { 365 };
        if rem < dy { break; }
        rem -= dy;
        year += 1;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [31u32, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut month = 1u32;
    for &md in &month_days {
        if rem < md as u64 { break; }
        rem -= md as u64;
        month += 1;
    }
    let day = rem + 1;
    format!("{:04}{:02}{:02}_{:02}{:02}{:02}", year, month, day, h, m, s)
}

/// Unix timestamp in seconds (used by watchdog fault event logging).
fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// SSH to every previously-deployed node and kill all inf3203 processes.
/// Runs after the dashboard exits to clean up the cluster.
fn run_teardown(cwd: &Path) {
    let nodes_file = cwd.join(".inf3203_nodes");
    let content = match fs::read_to_string(&nodes_file) {
        Ok(c) => c,
        Err(_) => {
            eprintln!("[teardown] No node list found ({}) — skipping teardown.", nodes_file.display());
            return;
        }
    };
    let nodes: Vec<String> = content.lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    if nodes.is_empty() {
        return;
    }
    eprintln!("[teardown] Killing inf3203_aika on {} nodes…", nodes.len());
    let handles: Vec<_> = nodes.into_iter().map(|node| {
        thread::spawn(move || {
            ssh_run(&node, "pkill -f '[i]nf3203_aika'; true");
        })
    }).collect();
    for h in handles { let _ = h.join(); }
    eprintln!("[teardown] Done.");
}

/// Response from GET /leader on a CC node.
#[derive(serde::Deserialize)]
struct LeaderResponse {
    leader_address: Option<String>,
}

/// Probe a single CC address: returns (alive, is_leader).
fn probe_cc(addr: &str) -> (bool, bool) {
    let output = Command::new("curl")
        .args(["-s", "--connect-timeout", "2", "--max-time", "3"])
        .arg(format!("http://{}/leader", addr))
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output();
    match output {
        Ok(out) if out.status.success() => {
            if let Ok(resp) = serde_json::from_slice::<LeaderResponse>(&out.stdout) {
                let is_leader = resp
                    .leader_address
                    .as_deref()
                    .map_or(false, |la| la == addr);
                (true, is_leader)
            } else {
                (true, false) // responded but couldn't parse — still alive
            }
        }
        _ => (false, false),
    }
}

/// Polls CC /status every few seconds and updates the shared dashboard state.
/// Also probes each CC individually for alive/leader status.
fn telemetry_poller(
    cc_addrs: Vec<String>,
    dashboard: Arc<Mutex<DashboardState>>,
    log_buf: LogBuffer,
) {
    const POLL_INTERVAL_SECS: u64 = 3;
    const MAX_HISTORY: usize = 1200; // ~1 hour at 3s interval

    loop {
        thread::sleep(Duration::from_secs(POLL_INTERVAL_SECS));

        // Probe each CC for alive/leader status.
        let statuses: Vec<CcNodeStatus> = cc_addrs
            .iter()
            .map(|addr| {
                let (alive, is_leader) = probe_cc(addr);
                CcNodeStatus {
                    addr: addr.clone(),
                    alive,
                    is_leader,
                }
            })
            .collect();

        if let Some(status) = poll_status(&cc_addrs) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let mut d = dashboard.lock().unwrap();
            d.cc_statuses = statuses;
            d.throughput_history.push((now, status.completed_tasks));
            if d.throughput_history.len() > MAX_HISTORY {
                d.throughput_history.remove(0);
            }
            // Record per-node image history for rate calculation.
            for (node_id, img_count) in &status.telemetry.per_node_images {
                let history = d.per_node_history.entry(node_id.clone()).or_default();
                history.push((now, *img_count));
                if history.len() > MAX_HISTORY {
                    history.remove(0);
                }
            }
            // Record TTL expiration history.
            d.ttl_history.push((now, status.telemetry.ttl_expirations));
            if d.ttl_history.len() > MAX_HISTORY {
                d.ttl_history.remove(0);
            }
            // Freeze the timer when all tasks are completed.
            if d.completed_at.is_none()
                && status.total_tasks > 0
                && status.completed_tasks == status.total_tasks
            {
                d.completed_at = Some(std::time::Instant::now());
                let telemetry_file = d.telemetry_file.clone();
                let saved = d.telemetry_saved;
                d.last_status = Some(status);
                if !saved {
                    d.telemetry_saved = true;
                    save_telemetry(&d, &telemetry_file);
                }
                drop(d);
                log_buf.push(format!("✅ All tasks completed!"));
            } else {
                d.last_status = Some(status);
            }
        } else {
            // No status available but still update CC statuses.
            let mut d = dashboard.lock().unwrap();
            d.cc_statuses = statuses;
        }
    }
}

/// Monitors CC processes via SSH and restarts them on the same node if crashed.
/// Runs every 10s. No replacement logic (Raft doesn't support dynamic membership).
fn cc_monitor(
    cc_entries: Vec<(String, String, String)>,
    log_dir: String,
    dashboard: Arc<Mutex<DashboardState>>,
    log_buf: LogBuffer,
) {
    const INTERVAL_SECS: u64 = 5;
    let mut was_down: Vec<bool> = vec![false; cc_entries.len()];

    log_buf.push(format!(
        "[watchdog] CC monitor started — watching {} nodes",
        cc_entries.len()
    ));

    loop {
        thread::sleep(Duration::from_secs(INTERVAL_SECS));

        for (i, (node, binary, args)) in cc_entries.iter().enumerate() {
            let alive = ssh_run(node, "pgrep -f '[i]nf3203_aika' > /dev/null 2>&1");
            if alive {
                was_down[i] = false;
                continue;
            }
            // Only count the crash once per episode.
            if !was_down[i] {
                was_down[i] = true;
                let mut d = dashboard.lock().unwrap();
                d.cc_crashes += 1;
                d.fault_events.push(FaultEvent { ts: unix_now_secs(), kind: "cc_crash", node: node.clone() });
            }
            log_buf.push(format!("[watchdog] CC on {} not running — restarting…", node));
            let log_file = format!("{}/cc-{}.log", log_dir, node);
            if ssh_start(node, binary, args, &log_file) {
                let mut d = dashboard.lock().unwrap();
                d.cc_restarts += 1;
                d.fault_events.push(FaultEvent { ts: unix_now_secs(), kind: "cc_restart", node: node.clone() });
                was_down[i] = false;
                log_buf.push(format!("[watchdog] CC on {} restarted ok", node));
            } else {
                log_buf.push(format!(
                    "[watchdog] CC on {} restart FAILED (process not running after start)",
                    node
                ));
            }
        }
    }
}

/// Monitors LC processes via SSH. Restarts with delay, replaces permanently dead
/// nodes with fresh available ones.
fn lc_monitor(
    lc_watchlist: Vec<(String, String, String)>,
    ctx: WatchdogContext,
    dashboard: Arc<Mutex<DashboardState>>,
    log_buf: LogBuffer,
) {
    const INTERVAL_SECS: u64 = 3;
    // Must be > 2 × lc_heartbeat_timeout_secs (now 6s).
    const RESTART_DELAY_SECS: u64 = 15;
    // Consecutive SSH failures before we give up and pick a replacement node.
    const MAX_RESTART_ATTEMPTS: u32 = 3;

    log_buf.push(format!(
        "[watchdog] LC monitor started — watching {} nodes",
        lc_watchlist.len()
    ));

    let mut lc_entries: Vec<WatchdogEntry> = lc_watchlist
        .into_iter()
        .map(|(node, node_id, restart_args_template)| WatchdogEntry {
            node,
            node_id,
            restart_args_template,
            failed_attempts: 0,
            down_since: None,
        })
        .collect();

    loop {
        thread::sleep(Duration::from_secs(INTERVAL_SECS));

        // Collect nodes currently tracked so we can exclude them when picking replacements.
        let known_nodes: std::collections::HashSet<String> =
            lc_entries.iter().map(|e| e.node.clone()).collect();

        for entry in lc_entries.iter_mut() {
            let alive = ssh_run(&entry.node, "pgrep -f '[i]nf3203_aika' > /dev/null 2>&1");
            if alive {
                if entry.down_since.take().is_some() {
                    log_buf.push(format!("[watchdog] LC on {} recovered", entry.node));
                    entry.failed_attempts = 0;
                }
                continue;
            }

            // Node is down — record when we first noticed.
            if entry.down_since.is_none() {
                let mut d = dashboard.lock().unwrap();
                d.lc_crashes += 1;
                d.fault_events.push(FaultEvent { ts: unix_now_secs(), kind: "lc_crash", node: entry.node.clone() });
            }
            let first_down = entry.down_since.get_or_insert_with(std::time::Instant::now);
            let down_secs = first_down.elapsed().as_secs();

            if down_secs < RESTART_DELAY_SECS {
                log_buf.push(format!(
                    "[watchdog] LC on {} down {}s — waiting {}s before restart",
                    entry.node, down_secs, RESTART_DELAY_SECS
                ));
                continue;
            }

            // Delay elapsed — attempt restart with a fresh port.
            let port = match find_free_port(&entry.node, 30) {
                Some(p) => p,
                None => {
                    log_buf.push(format!(
                        "[watchdog] LC on {} — no free port, will retry next cycle",
                        entry.node
                    ));
                    continue;
                }
            };
            let restart_args = entry.restart_args_template.replace("{PORT}", &port.to_string());
            log_buf.push(format!(
                "[watchdog] LC on {} down {}s — restarting as replica on port {} (attempt {}/{})…",
                entry.node,
                down_secs,
                port,
                entry.failed_attempts + 1,
                MAX_RESTART_ATTEMPTS
            ));
            let log_file = format!("{}/lc-{}.log", ctx.log_dir, entry.node);
            if ssh_start(&entry.node, &ctx.binary, &restart_args, &log_file) {
                log_buf.push(format!("[watchdog] LC on {} restarted ok", entry.node));
                let mut d = dashboard.lock().unwrap();
                d.lc_restarts += 1;
                d.fault_events.push(FaultEvent { ts: unix_now_secs(), kind: "lc_restart", node: entry.node.clone() });
                drop(d);
                entry.down_since = None;
                entry.failed_attempts = 0;
                continue;
            }

            entry.failed_attempts += 1;
            if entry.failed_attempts < MAX_RESTART_ATTEMPTS {
                log_buf.push(format!(
                    "[watchdog] LC on {} restart failed ({}/{})",
                    entry.node, entry.failed_attempts, MAX_RESTART_ATTEMPTS
                ));
                continue;
            }

            // Node is confirmed dead — find a replacement.
            log_buf.push(format!(
                "[watchdog] LC on {} permanently unreachable — seeking replacement node",
                entry.node
            ));
            let gpu_nodes = gpu_node_hostnames();
            let raw = Command::new("/share/ifi/available-nodes.sh")
                .stdout(Stdio::piped())
                .output();
            let candidates: Vec<String> = match raw {
                Ok(o) => {
                    let mut rng = rand::rng();
                    let raw_list: Vec<String> = String::from_utf8_lossy(&o.stdout)
                        .lines()
                        .map(|s| s.trim().to_string())
                        .filter(|s| {
                            !s.is_empty()
                                && !gpu_nodes.contains(s.as_str())
                                && !known_nodes.contains(s.as_str())
                        })
                        .collect();
                    prioritize_nodes(raw_list, &mut rng)
                },
                Err(e) => {
                    log_buf.push(format!("[watchdog] Could not query available nodes: {}", e));
                    continue;
                }
            };

            let Some(new_node) = candidates.into_iter().next() else {
                log_buf.push(format!(
                    "[watchdog] No replacement node available for {}",
                    entry.node
                ));
                continue;
            };

            let Some(port) = find_free_port(&new_node, 30) else {
                log_buf.push(format!("[watchdog] No free port on replacement node {}", new_node));
                continue;
            };

            let node_id = format!("lc-{}", new_node);
            let new_restart_args = format!(
                "local-controller --node-id {} --bind 0.0.0.0:{} --cc-addrs {} \
                 --agents 0 --extractor-script {} --image-base-path {} --python {}",
                node_id, port, ctx.peer_list, ctx.classify_script, IMAGE_DIR, ctx.python
            );
            let new_restart_template = format!(
                "local-controller --node-id {} --bind 0.0.0.0:{{PORT}} --cc-addrs {} \
                 --agents 0 --extractor-script {} --image-base-path {} --python {}",
                node_id, ctx.peer_list, ctx.classify_script, IMAGE_DIR, ctx.python
            );

            log_buf.push(format!(
                "[watchdog] Starting replacement LC on {} (was {})…",
                new_node, entry.node
            ));
            let log_file = format!("{}/lc-{}.log", ctx.log_dir, new_node);
            if ssh_start(&new_node, &ctx.binary, &new_restart_args, &log_file) {
                log_buf.push(format!("[watchdog] Replacement LC on {} started ok", new_node));
                let mut d = dashboard.lock().unwrap();
                d.lc_replacements += 1;
                d.fault_events.push(FaultEvent { ts: unix_now_secs(), kind: "lc_replacement", node: new_node.clone() });
                drop(d);
                entry.node = new_node;
                entry.node_id = node_id;
                entry.restart_args_template = new_restart_template;
                entry.down_since = None;
                entry.failed_attempts = 0;
            } else {
                log_buf.push(format!("[watchdog] Replacement LC on {} failed to start", new_node));
                // Reset attempts so we try another replacement next cycle.
                entry.failed_attempts = 0;
            }
        }
    }
}

// Merge subcommand (set up using Claude Code Sonnet 4.6 in Agent Mode)
// Prompt asked to create a script for deduplicating and merging the multiple result files that
// will be generated on a leader change during the processing.

/// Merge all `results_*.ndjson` files in `<cwd>/inf3203_data/` into a single
/// deduplicated `results_final.ndjson` in the same directory.
///
/// Multiple result files can exist when leadership changed mid-run — each leader
/// writes to its own file. Lines are deduplicated by `batch_id`; the first
/// occurrence of each batch_id wins (all copies carry identical labels).
fn merge_results(cwd: &Path) {
    let results_dir = cwd.join("inf3203_data");

    // Collect all results_*.ndjson files (excluding the final output itself).
    let mut input_files: Vec<PathBuf> = match fs::read_dir(&results_dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| {
                        n.starts_with("results_")
                            && n.ends_with(".ndjson")
                            && n != "results_final.ndjson"
                    })
                    .unwrap_or(false)
            })
            .collect(),
        Err(e) => {
            eprintln!("Cannot read {}: {}", results_dir.display(), e);
            std::process::exit(1);
        }
    };
    input_files.sort();

    if input_files.is_empty() {
        eprintln!(
            "No results_*.ndjson files found in {}",
            results_dir.display()
        );
        std::process::exit(1);
    }

    println!("Merging {} file(s)…", input_files.len());

    // Read all lines, deduplicate by batch_id (first occurrence wins).
    let mut seen: std::collections::HashMap<u64, String> = std::collections::HashMap::new();
    let mut total_lines = 0usize;

    for path in &input_files {
        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("  Warning: cannot read {}: {}", path.display(), e);
                continue;
            }
        };
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            total_lines += 1;
            // Extract batch_id from the JSON line without a full parse.
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                if let Some(batch_id) = v["batch_id"].as_u64() {
                    seen.entry(batch_id).or_insert_with(|| line.to_string());
                }
            }
        }
    }

    let duplicates = total_lines.saturating_sub(seen.len());
    println!(
        "  {} total lines, {} unique batches, {} duplicates removed",
        total_lines,
        seen.len(),
        duplicates
    );

    // Write deduplicated output sorted by batch_id.
    let mut entries: Vec<(u64, &String)> = seen.iter().map(|(k, v)| (*k, v)).collect();
    entries.sort_by_key(|(id, _)| *id);

    let out_path = results_dir.join("results_final.ndjson");
    let mut out = match fs::File::create(&out_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Cannot create {}: {}", out_path.display(), e);
            std::process::exit(1);
        }
    };

    use std::io::Write;
    for (_, line) in &entries {
        writeln!(out, "{}", line).expect("write failed");
    }

    println!("  Written: {}", out_path.display());
}

// Cleanup subcommand (setup using Claude Code Sonnet 4.6 in Agent Mode)
// Prompt asked to create folders during deploy for the Raft logs (in /tmp/ to avoid using the
// distributed filesystem), and to create a cleanup script to avoid stale state interfering with
// new deployments and to prevent taking up unnecessary disk space.
/// SSH to every previously-deployed node and remove the Raft state directory.
fn run_cleanup(cwd: &Path) {
    let username = env::var("USER")
        .or_else(|_| env::var("LOGNAME"))
        .unwrap_or_else(|_| "unknown".into());

    let nodes_file = cwd.join(".inf3203_nodes");
    let content = match fs::read_to_string(&nodes_file) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Cannot read {}: {}", nodes_file.display(), e);
            eprintln!("Run deploy first to record the node list.");
            std::process::exit(1);
        }
    };

    println!("Cleaning Raft state dirs on deployed nodes…");
    let mut ok = 0usize;
    let mut failed = 0usize;
    for node in content.lines().filter(|l| !l.trim().is_empty()) {
        let pattern = format!("/tmp/inf3203_{}_*", username);
        let cmd = format!("rm -rf {}", pattern);
        print!("  {} … ", node);
        let _ = std::io::stdout().flush();
        if ssh_run(node, &cmd) {
            println!("ok");
            ok += 1;
        } else {
            println!("FAILED");
            failed += 1;
        }
    }
    println!("\nDone: {ok} ok, {failed} failed.");
}

// endregion
