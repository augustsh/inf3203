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
use rand::Rng;
use rand::seq::SliceRandom;
use std::env;
use std::fs;
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

// region Constants

const VERSION: &str = "v0.0.1"; // replaced by CI at release

const NUM_CC: usize = 3;
const AGENTS_PER_LC: usize = 4;

const IMAGE_DIR: &str = "/share/inf3203/unlabeled_images";

// can't use nodes with "decent GPU capabilities"
const GPU_NODE_PATTERNS: &[&str] = &["RTX", "Quadro", "GTX"];

// endregion

// region Helpers

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
fn ssh_start(node: &str, binary: &str, args: &str) -> bool {
    let cmd = format!("nohup {} {} </dev/null >/dev/null 2>&1 &", binary, args);
    ssh_run(node, &cmd)
}

/// Ensure `classify.py` is present in `install_dir`. If not download from GitHub.
fn ensure_classify_script(install_dir: &Path) -> PathBuf {
    let dest = install_dir.join("classify.py");
    if dest.exists() {
        println!("classify.py already present: {}", dest.display());
        return dest;
    }

    let url = format!(
        "https://github.com/augustsh/inf3203/releases/download/{}/classify.py",
        VERSION
    );
    println!("Downloading classify.py {}…", VERSION);

    let status = Command::new("curl")
        .args(["-fsS", "--no-progress-meter", "-L", "-o"])
        .arg(&dest)
        .arg(&url)
        .status()
        .expect("curl not found");

    if !status.success() {
        eprintln!("Download failed: {}", url);
        std::process::exit(1);
    }

    println!("Installed: {}", dest.display());
    dest
}

/// Ensure the `aika-node` binary and `inf3203_aika` symlink are present in `install_dir`.
/// Download binary from GitHub if absent.
fn ensure_binary(install_dir: &Path) -> PathBuf {
    let symlink = install_dir.join("inf3203_aika");
    if symlink.exists() {
        println!("Binary already present: {}", symlink.display());
        return symlink;
    }

    let url = format!(
        "https://github.com/augustsh/inf3203/releases/download/{}/aika-node-x86_64-unknown-linux-musl.tar.gz",
        VERSION
    );
    println!("Downloading aika-node {}…", VERSION);

    let tarball = install_dir.join("aika-node.tar.gz");
    let status = Command::new("curl")
        .args(["-fsS", "--no-progress-meter", "-L", "-o"])
        .arg(&tarball)
        .arg(&url)
        .status()
        .expect("curl not found");

    if !status.success() {
        eprintln!("Download failed: {}", url);
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
        eprintln!("Failed to extract tarball");
        std::process::exit(1);
    }

    fs::remove_file(&tarball).ok();

    let binary = install_dir.join("aika-node");
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).expect("chmod failed");

    // Symlink: the running process will appear as inf3203_aika in ps/pgrep,
    // satisfying the cluster's process-naming requirement.
    std::os::unix::fs::symlink(&binary, &symlink).expect("symlink failed");

    println!("Installed: {}", symlink.display());
    symlink
}

// endregion

// region Main

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <num_nodes>", args[0]);
        eprintln!("       {} cleanup", args[0]);
        eprintln!("       {} merge", args[0]);
        eprintln!();
        eprintln!("  Deploys an Áika cluster. Run from a shared-filesystem directory");
        eprintln!("  (e.g. your NFS cwd) so all nodes can reach the binary.");
        eprintln!(
            "  Minimum: {} nodes (one Raft quorum). Recommended: 10+.",
            NUM_CC
        );
        eprintln!();
        eprintln!("  cleanup  Remove Raft state dirs on all previously-deployed nodes.");
        eprintln!("  merge    Deduplicate results_*.ndjson into results_final.ndjson.");
        std::process::exit(1);
    }

    let cwd: PathBuf = env::current_dir().expect("cannot determine current directory");

    if args[1] == "cleanup" {
        run_cleanup(&cwd);
        return;
    }

    if args[1] == "merge" {
        merge_results(&cwd);
        return;
    }

    let num_nodes: usize = args[1]
        .parse()
        .expect("num_nodes must be a positive integer");

    if num_nodes < NUM_CC {
        eprintln!(
            "Need at least {} nodes for a Raft quorum (requested {}).",
            NUM_CC, num_nodes
        );
        std::process::exit(1);
    }

    // ensure that the aika binary is in the user directory (in the distributed filesystem)
    let username = env::var("USER")
        .or_else(|_| env::var("LOGNAME"))
        .unwrap_or_else(|_| "unknown".into());

    let binary = ensure_binary(&cwd);
    let binary_str = binary.to_str().expect("non-UTF8 path");

    let classify_script = ensure_classify_script(&cwd);
    let classify_script_str = classify_script.to_str().expect("non-UTF8 path");

    // Create the results directory on the shared filesystem so all CC nodes can
    // write their NDJSON output to a single accessible location.
    let results_dir = cwd.join("inf3203_data");
    fs::create_dir_all(&results_dir).expect("failed to create inf3203_data dir");
    let results_dir_str = results_dir.to_str().expect("non-UTF8 path");

    let gpu_nodes = gpu_node_hostnames();

    // Get currently available non-gpu nodes
    let raw = Command::new("/share/ifi/available-nodes.sh")
        .stdout(Stdio::piped())
        .output()
        .expect("failed to run /share/ifi/available-nodes.sh");

    let mut candidates: Vec<String> = String::from_utf8_lossy(&raw.stdout)
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && !gpu_nodes.contains(s.as_str()))
        .collect();

    if candidates.len() < NUM_CC {
        eprintln!(
            "Not enough non-GPU nodes available (found {}, need {}).",
            candidates.len(),
            NUM_CC
        );
        std::process::exit(1);
    }

    let mut rng = rand::rng();
    candidates.shuffle(&mut rng);

    let nodes: Vec<String> = candidates.into_iter().take(num_nodes).collect();
    println!(
        "\nDeploying Áika cluster: {} nodes ({} CC, {} LC)\n",
        nodes.len(),
        NUM_CC,
        nodes.len().saturating_sub(NUM_CC)
    );

    // assign ports to CC nodes and build the peer list
    let cc_nodes = &nodes[..NUM_CC];
    let lc_nodes = &nodes[NUM_CC..];

    println!("[1/3] Assigning ports to cluster controllers…");
    let mut cc_assignments: Vec<(String, u16)> = Vec::new();
    for node in cc_nodes {
        match find_free_port(node, 30) {
            Some(port) => cc_assignments.push((node.clone(), port)),
            None => {
                eprintln!("Could not find a free port on CC node {}.", node);
                std::process::exit(1);
            }
        }
    }

    let peer_list: String = cc_assignments
        .iter()
        .map(|(n, p)| format!("{}:{}", n, p))
        .collect::<Vec<_>>()
        .join(",");

    // start Cluster Controllers
    println!("\n[2/3] Starting {} cluster controllers…", NUM_CC);
    let mut cc_started: Vec<String> = Vec::new();
    // (node, binary, args) — used by the watchdog to restart crashed CCs.
    let mut cc_watchlist: Vec<(String, String, String)> = Vec::new();

    for (i, (node, port)) in cc_assignments.iter().enumerate() {
        let node_id = i + 1;
        let data_dir = format!("/tmp/inf3203_{}_{}", username, node_id);
        let node_args = format!(
            "cluster-controller --node-id {} --bind 0.0.0.0:{} --peers {} --image-dir {} \
             --data-dir {} --results-dir {}",
            node_id, port, peer_list, IMAGE_DIR, data_dir, results_dir_str
        );
        // Wipe old Raft state on the target node (start fresh every time we deploy new cluster)
        let cmd = format!(
            "rm -rf {} && nohup {} {} </dev/null >/dev/null 2>&1 &",
            data_dir, binary_str, node_args
        );
        print!("  CC{} @ {}:{}  … ", node_id, node, port);
        let _ = std::io::stdout().flush();
        if ssh_run(node, &cmd) {
            println!("ok");
            cc_started.push(format!("{}:{}", node, port));
            cc_watchlist.push((node.clone(), binary_str.to_string(), node_args));
        } else {
            println!("FAILED");
        }
    }

    if cc_started.len() < NUM_CC {
        eprintln!(
            "\nOnly {}/{} CCs started — quorum requires all {}. Aborting.",
            cc_started.len(),
            NUM_CC,
            NUM_CC
        );
        std::process::exit(1);
    }

    // (node, binary, args) — used by the watchdog to restart crashed LCs.
    let mut lc_watchlist: Vec<(String, String, String)> = Vec::new();

    // start Local Controllers
    if lc_nodes.is_empty() {
        println!("\n[3/3] No LC nodes — CC-only deployment.");
    } else {
        let num_replica = 1.min(lc_nodes.len());
        let num_active = lc_nodes.len().saturating_sub(num_replica);
        println!(
            "\n[3/3] Starting {} local controllers ({} standby replica, {} active)…",
            lc_nodes.len(),
            num_replica,
            num_active
        );

        let mut lc_started = 0usize;

        for (i, node) in lc_nodes.iter().enumerate() {
            // The first LC is a standby replica with 0 agents; it will be
            // activated by the CC via POST /activate if another LC fails.
            let is_replica = i == 0;
            let agents = if is_replica { 0 } else { AGENTS_PER_LC };
            let label = if is_replica { "replica" } else { "active " };

            let port = match find_free_port(node, 30) {
                Some(p) => p,
                None => {
                    eprintln!("  No free port on {} — skipping.", node);
                    continue;
                }
            };

            let node_id = format!("lc-{}", node);
            let lc_args = format!(
                "local-controller --node-id {} --bind 0.0.0.0:{} --cc-addrs {} \
                 --agents {} --extractor-script {} --image-base-path {}",
                node_id, port, peer_list, agents, classify_script_str, IMAGE_DIR
            );
            // Watchdog always restarts as a standby replica (--agents 0)
            // to replace the old replica that CC promoted
            let restart_args = format!(
                "local-controller --node-id {} --bind 0.0.0.0:{} --cc-addrs {} \
                 --agents 0 --extractor-script {} --image-base-path {}",
                node_id, port, peer_list, classify_script_str, IMAGE_DIR
            );

            print!("  LC [{}] {} @ {}:{}  … ", label, node_id, node, port);
            let _ = std::io::stdout().flush();
            if ssh_start(node, binary_str, &lc_args) {
                println!("ok");
                lc_started += 1;
                lc_watchlist.push((node.clone(), binary_str.to_string(), restart_args));
            } else {
                println!("FAILED");
            }
        }

        println!(
            "  {}/{} local controllers started.",
            lc_started,
            lc_nodes.len()
        );
    }

    // save node list and print summary
    let nodes_file = cwd.join(".inf3203_nodes");
    if let Err(e) = fs::write(&nodes_file, nodes.join("\n")) {
        eprintln!("Warning: could not save node list: {}", e);
    }

    println!("\n══════════════════════════════════════════");
    println!("  Áika cluster deployed successfully");
    println!("══════════════════════════════════════════");
    println!("  Binary:   {}", binary_str);
    println!("  Results:  {}", results_dir_str);
    println!("  CCs:      {}", cc_started.join("  "));
    println!();
    println!("  Status:   curl http://{}/status", cc_started[0]);
    println!("  Leader:   curl http://{}/leader", cc_started[0]);
    println!();
    println!(
        "  Teardown: xargs -a {} -I{{}} ssh {{}} \"pkill -f inf3203_aika; true\"",
        nodes_file.display()
    );
    println!("  Cleanup:  {} cleanup", args[0]);
    println!("══════════════════════════════════════════");

    if !lc_watchlist.is_empty() {
        println!("\nWatchdog running — press Ctrl+C to stop.\n");
        let ctx = WatchdogContext {
            binary: binary_str.to_string(),
            classify_script: classify_script_str.to_string(),
            peer_list: peer_list.clone(),
        };
        lc_watchdog(lc_watchlist, cc_watchlist, ctx);
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
}

/// One entry in the watchdog's tracking list.
struct WatchdogEntry {
    node: String,
    /// Always `--agents 0` — restarts rejoin as standby replica.
    restart_args: String,
    /// How many consecutive restart attempts have failed for this entry.
    failed_attempts: u32,
    /// When we first detected this node as down in the current failure episode.
    down_since: Option<std::time::Instant>,
}

/// Periodically checks that every LC process is still running.
///
/// On first detecting a node as down the watchdog waits `RESTART_DELAY_SECS`
/// so the CC can complete its two-stage failover before the node comes back
/// (prevents the CC clearing suspicion prematurely and skipping replica activation).
///
/// After `MAX_RESTART_ATTEMPTS` consecutive SSH failures the node is considered
/// permanently dead. The watchdog then picks a fresh available node, starts an
/// LC (`--agents 0`) on it, and replaces the dead entry in the watchlist.
fn lc_watchdog(
    lc_watchlist: Vec<(String, String, String)>,
    cc_watchlist: Vec<(String, String, String)>,
    ctx: WatchdogContext,
) {
    // How often to poll each LC node.
    const INTERVAL_SECS: u64 = 30;
    // Must be > 2 × lc_heartbeat_timeout_secs (default 60s).
    const RESTART_DELAY_SECS: u64 = 150;
    // Consecutive SSH failures before we give up and pick a replacement node.
    const MAX_RESTART_ATTEMPTS: u32 = 3;

    let mut lc_entries: Vec<WatchdogEntry> = lc_watchlist
        .into_iter()
        .map(|(node, _binary, restart_args)| WatchdogEntry {
            node,
            restart_args,
            failed_attempts: 0,
            down_since: None,
        })
        .collect();

    // CC entries: (node, binary, args) — stored separately, no replacement logic.
    let cc_entries: Vec<(String, String, String)> = cc_watchlist;

    loop {
        thread::sleep(Duration::from_secs(INTERVAL_SECS));

        // --- CC monitoring: restart on same node, no replacement ---
        for (node, binary, args) in cc_entries.iter() {
            let alive = ssh_run(node, "pgrep -f inf3203_aika > /dev/null 2>&1");
            if !alive {
                eprintln!("[watchdog] CC on {} not running — restarting…", node);
                if ssh_start(node, binary, args) {
                    eprintln!("[watchdog] CC on {} restarted ok", node);
                } else {
                    eprintln!(
                        "[watchdog] CC on {} restart FAILED — will retry next cycle",
                        node
                    );
                }
            }
        }

        // --- LC monitoring: restart with delay, replace if permanently dead ---

        // Collect nodes currently tracked so we can exclude them when picking replacements.
        let known_nodes: std::collections::HashSet<String> =
            lc_entries.iter().map(|e| e.node.clone()).collect();

        for entry in lc_entries.iter_mut() {
            let alive = ssh_run(&entry.node, "pgrep -f inf3203_aika > /dev/null 2>&1");
            if alive {
                if entry.down_since.take().is_some() {
                    eprintln!("[watchdog] LC on {} recovered", entry.node);
                    entry.failed_attempts = 0;
                }
                continue;
            }

            // Node is down — record when we first noticed.
            let first_down = entry.down_since.get_or_insert_with(std::time::Instant::now);
            let down_secs = first_down.elapsed().as_secs();

            if down_secs < RESTART_DELAY_SECS {
                eprintln!(
                    "[watchdog] LC on {} down {}s — waiting {}s before restart",
                    entry.node, down_secs, RESTART_DELAY_SECS
                );
                continue;
            }

            // Delay elapsed — attempt restart.
            eprintln!(
                "[watchdog] LC on {} down {}s — restarting as replica (attempt {}/{})…",
                entry.node,
                down_secs,
                entry.failed_attempts + 1,
                MAX_RESTART_ATTEMPTS
            );
            if ssh_start(&entry.node, &ctx.binary, &entry.restart_args) {
                eprintln!("[watchdog] LC on {} restarted ok", entry.node);
                entry.down_since = None;
                entry.failed_attempts = 0;
                continue;
            }

            entry.failed_attempts += 1;
            if entry.failed_attempts < MAX_RESTART_ATTEMPTS {
                eprintln!(
                    "[watchdog] LC on {} restart failed ({}/{})",
                    entry.node, entry.failed_attempts, MAX_RESTART_ATTEMPTS
                );
                continue;
            }

            // Node is confirmed dead — find a replacement.
            eprintln!(
                "[watchdog] LC on {} permanently unreachable — seeking replacement node",
                entry.node
            );
            let gpu_nodes = gpu_node_hostnames();
            let raw = Command::new("/share/ifi/available-nodes.sh")
                .stdout(Stdio::piped())
                .output();
            let candidates: Vec<String> = match raw {
                Ok(o) => String::from_utf8_lossy(&o.stdout)
                    .lines()
                    .map(|s| s.trim().to_string())
                    .filter(|s| {
                        !s.is_empty()
                            && !gpu_nodes.contains(s.as_str())
                            && !known_nodes.contains(s.as_str())
                    })
                    .collect(),
                Err(e) => {
                    eprintln!("[watchdog] Could not query available nodes: {}", e);
                    continue;
                }
            };

            let Some(new_node) = candidates.into_iter().next() else {
                eprintln!(
                    "[watchdog] No replacement node available for {}",
                    entry.node
                );
                continue;
            };

            let Some(port) = find_free_port(&new_node, 30) else {
                eprintln!("[watchdog] No free port on replacement node {}", new_node);
                continue;
            };

            let node_id = format!("lc-{}", new_node);
            let new_restart_args = format!(
                "local-controller --node-id {} --bind 0.0.0.0:{} --cc-addrs {} \
                 --agents 0 --extractor-script {} --image-base-path {}",
                node_id, port, ctx.peer_list, ctx.classify_script, IMAGE_DIR
            );

            eprintln!(
                "[watchdog] Starting replacement LC on {} (was {})…",
                new_node, entry.node
            );
            if ssh_start(&new_node, &ctx.binary, &new_restart_args) {
                eprintln!("[watchdog] Replacement LC on {} started ok", new_node);
                entry.node = new_node;
                entry.restart_args = new_restart_args;
                entry.down_since = None;
                entry.failed_attempts = 0;
            } else {
                eprintln!("[watchdog] Replacement LC on {} failed to start", new_node);
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
