use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// region state machine commands

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum Command {
    /// No-op entry appended by a newly elected leader so it can commit
    /// entries from previous terms (Raft Leader Completeness, §5.4).
    /// This is the `Default` variant so `become_leader` can use `C: Default`.
    #[default]
    NoOp,

    /// Register a batch of image paths as pending tasks
    AddTaskBatch {
        batch_id: u64,
        image_paths: Vec<String>,
    },

    /// Mark a batch as assigned to a specific agent with a TTL timestamp
    AssignTask {
        batch_id: u64,
        agent_id: String,
        /// needs to be a unix timestamp
        assigned_at: u64,
    },

    /// Mark a batch as completed with label results
    CompleteTask {
        batch_id: u64,
        /// Maps image_path -> label
        labels: Vec<(String, String)>,
    },

    /// TTL expired — return batch to pending
    ExpireTask { batch_id: u64 },

    /// Register a node (local controller) joining the cluster
    RegisterNode { node_id: String, address: String },

    /// Remove a node from the cluster
    DeregisterNode { node_id: String },
}

// endregion

// region task and node state

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Assigned { agent_id: String, assigned_at: u64 },
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskBatch {
    pub batch_id: u64,
    pub image_paths: Vec<String>,
    pub status: TaskStatus,
    /// Populated once completed: image_path -> label
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub address: String,
    pub last_heartbeat: u64,
    pub agent_ids: Vec<String>,
    /// Number of agent workers this LC is running (0 = standby replica).
    pub agent_count: usize,
    pub extractor_script: String,
    pub image_base_path: String,
    pub python: String,
}

// endregion

// region HTTP request/response types

/// Local controller -> Cluster controller: request a batch of work
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskRequest {
    pub agent_id: String,
}

/// Cluster controller -> Agent: assign a batch of tasks to an agent
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub batch_id: u64,
    pub image_paths: Vec<String>,
}

/// Agent -> Cluster controller: report completion of a batch with labels
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskCompletion {
    pub batch_id: u64,
    pub agent_id: String,
    /// (image_path, label)
    pub labels: Vec<(String, String)>,
}

/// Cluster controller responds to completion requests
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskCompletionResponse {
    pub accepted: bool,
    pub message: String,
}

/// Local controller -> Cluster controller: periodic heartbeat
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub node_id: String,
    pub address: String,
    pub agent_ids: Vec<String>,
    pub load: f64, // e.g. CPU usage or tasks in flight
    /// Number of agent workers this LC is running (0 = standby replica).
    pub agent_count: usize,
    pub extractor_script: String,
    pub image_base_path: String,
    pub python: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub acknowledged: bool,
}

/// Response to GET /leader — helps clients find the current Raft leader
#[derive(Debug, Serialize, Deserialize)]
pub struct LeaderResponse {
    pub leader_id: Option<u64>,
    pub leader_address: Option<String>,
}

/// Response to GET /status — cluster overview
#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub total_tasks: u64,
    pub pending_tasks: u64,
    pub assigned_tasks: u64,
    pub completed_tasks: u64,
    pub registered_nodes: Vec<NodeInfo>,
    /// Node IDs of local controllers that have not sent a heartbeat recently.
    pub stale_nodes: Vec<String>,
}

/// Agent -> Local controller: periodic heartbeat with current status
#[derive(Debug, Serialize, Deserialize)]
pub struct AgentHeartbeat {
    pub agent_id: String,
    pub current_batch_id: Option<u64>,
}

/// Cluster controller -> Replica LC: instruct it to activate and take over for a failed LC.
#[derive(Debug, Serialize, Deserialize)]
pub struct ActivateRequest {
    /// The node_id of the LC that has failed (informational).
    pub failed_node_id: String,
    /// Number of agent workers to spawn.
    pub agent_count: usize,
    pub extractor_script: String,
    pub image_base_path: String,
    pub python: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActivateResponse {
    pub activated: bool,
    pub message: String,
}

// endregion
