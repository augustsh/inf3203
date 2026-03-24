use clap::{Parser, Subcommand};
use std::net::SocketAddr;

mod agent;
mod cluster_controller;
mod common;
mod local_controller;
mod raft;

#[derive(Parser)]
#[command(name = "inf3203_aika")]
#[command(about = "Distributed edge system for AI inference")]
struct Cli {
    #[command(subcommand)]
    command: Role,
}

#[derive(Subcommand)]
enum Role {
    /// Run as a cluster controller (participates in Raft consensus)
    ClusterController {
        /// This node's unique ID within the Raft cluster
        #[arg(long)]
        node_id: u64,

        /// Address to bind the HTTP + Raft server to
        #[arg(long, default_value = "0.0.0.0:8000")]
        bind: SocketAddr,

        /// Comma-separated list of peer cluster controller addresses (e.g. node1:8000,node2:8000)
        #[arg(long, value_delimiter = ',')]
        peers: Vec<String>,

        /// TTL in seconds for assigned tasks before they are reclaimed
        #[arg(long, default_value = "120")]
        task_ttl_secs: u64,

        /// Directory containing unlabeled images to ingest on first leader election
        #[arg(long, default_value = "/share/inf3203/unlabeled_images")]
        image_dir: String,

        /// Number of images per task batch
        #[arg(long, default_value = "100")]
        batch_size: usize,

        /// Seconds without heartbeat before a local controller is flagged as failed
        #[arg(long, default_value = "60")]
        lc_heartbeat_timeout_secs: u64,

        /// Node-local directory for Raft persistent state (must NOT be on shared FS).
        /// Defaults to /tmp/inf3203_<USER>_<node_id>
        #[arg(long)]
        data_dir: Option<String>,

        /// Directory for writing the results NDJSON file (can be on NFS).
        /// Defaults to $HOME/inf3203_data
        #[arg(long)]
        results_dir: Option<String>,

        /// Raft heartbeat interval in milliseconds (leader → followers).
        /// Must be well below the election timeout minimum.
        #[arg(long, default_value = "50")]
        heartbeat_interval_ms: u64,

        /// Minimum Raft election timeout in milliseconds.
        /// Increase for clusters with higher network latency.
        #[arg(long, default_value = "150")]
        election_timeout_min_ms: u64,

        /// Maximum Raft election timeout in milliseconds.
        #[arg(long, default_value = "300")]
        election_timeout_max_ms: u64,
    },

    /// Run as a local controller (monitors agents on this physical node)
    LocalController {
        /// Unique identifier for this node
        #[arg(long)]
        node_id: String,

        /// Address to bind the local controller HTTP server to
        #[arg(long, default_value = "0.0.0.0:9000")]
        bind: SocketAddr,

        /// Comma-separated cluster controller addresses for task requests
        #[arg(long, value_delimiter = ',')]
        cc_addrs: Vec<String>,

        /// Number of agent workers to spawn (0 = replica mode)
        #[arg(long, default_value = "4")]
        agents: usize,

        /// Seconds between health checks on local agents
        #[arg(long, default_value = "5")]
        health_check_interval: u64,

        /// Path to the feature extraction Python script (passed to spawned agents)
        #[arg(long, default_value = "./feature_extractor.py")]
        extractor_script: String,

        /// Base path where the unlabeled images reside (passed to spawned agents)
        #[arg(long, default_value = "/share/inf3203/unlabeled_images")]
        image_base_path: String,

        /// Path to the Python interpreter to use for feature extraction (e.g. venv python)
        #[arg(long, default_value = "python3")]
        python: String,
    },

    /// Run as a worker agent (usually spawned by a local controller)
    Agent {
        /// Unique identifier for this agent
        #[arg(long)]
        agent_id: String,

        /// Address of the local controller managing this agent
        #[arg(long)]
        lc_addr: String,

        /// Comma-separated cluster controller addresses for task requests
        #[arg(long, value_delimiter = ',')]
        cc_addrs: Vec<String>,

        /// Path to the feature extraction Python script
        #[arg(long, default_value = "./feature_extractor.py")]
        extractor_script: String,

        /// Base path where the unlabeled images reside
        #[arg(long, default_value = "/share/inf3203/unlabeled_images")]
        image_base_path: String,

        /// Path to the Python interpreter to use for feature extraction (e.g. venv python)
        #[arg(long, default_value = "python3")]
        python: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command-line arguments to determine what role this node has
    let cli = Cli::parse();

    // Match role to build the needed config struct and start the node
    match cli.command {
        Role::ClusterController {
            node_id,
            bind,
            peers,
            task_ttl_secs,
            image_dir,
            batch_size,
            lc_heartbeat_timeout_secs,
            data_dir,
            results_dir,
            heartbeat_interval_ms,
            election_timeout_min_ms,
            election_timeout_max_ms,
        } => {
            let username = std::env::var("USER").unwrap_or_else(|_| "unknown".into());
            let data_dir =
                data_dir.unwrap_or_else(|| format!("/tmp/inf3203_{}_{}", username, node_id));
            let results_dir = results_dir.unwrap_or_else(|| {
                std::env::var("HOME")
                    .map(|h| format!("{}/inf3203_data", h))
                    .unwrap_or_else(|_| "/tmp/inf3203_data".into())
            });
            let config = cluster_controller::ClusterControllerConfig {
                node_id,
                bind,
                peers,
                task_ttl_secs,
                image_dir,
                batch_size,
                lc_heartbeat_timeout_secs,
                data_dir,
                results_dir,
                heartbeat_interval_ms,
                election_timeout_min_ms,
                election_timeout_max_ms,
            };
            cluster_controller::run(config).await
        }

        Role::LocalController {
            node_id,
            bind,
            cc_addrs,
            agents,
            health_check_interval,
            extractor_script,
            image_base_path,
            python,
        } => {
            let config = local_controller::LocalControllerConfig {
                node_id,
                bind,
                cc_addrs,
                agent_count: agents,
                health_check_interval,
                extractor_script,
                image_base_path,
                python,
            };
            local_controller::run(config).await
        }

        Role::Agent {
            agent_id,
            lc_addr,
            cc_addrs,
            extractor_script,
            image_base_path,
            python,
        } => {
            let config = agent::AgentConfig {
                agent_id,
                lc_addr,
                cc_addrs,
                extractor_script,
                image_base_path,
                python,
            };
            agent::run(config).await
        }
    }
}
