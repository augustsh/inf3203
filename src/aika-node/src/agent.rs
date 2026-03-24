use crate::common::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct AgentConfig {
    pub agent_id: String,
    pub lc_addr: String,
    pub cc_addrs: Vec<String>,
    pub extractor_script: String,
    pub image_base_path: String,
    pub python: String,
}

/// Main entry point for the agent worker.
pub async fn run(config: AgentConfig) -> anyhow::Result<()> {
    tracing::info!(
        "Starting agent {} (local controller: {})",
        config.agent_id,
        config.lc_addr
    );

    let config = Arc::new(config);
    let client = reqwest::Client::new();

    // Shared slot: the work loop writes the current batch ID here so the
    // heartbeat loop can report it to the local controller.
    let current_batch: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));

    // Start heartbeat background task to local controller.
    let hb_config = Arc::clone(&config);
    let hb_batch = Arc::clone(&current_batch);
    let hb_client = client.clone();
    tokio::spawn(async move {
        heartbeat_loop(hb_config, hb_batch, hb_client).await;
    });

    // Enter the main work loop (runs until error or shutdown).
    work_loop(&config, &current_batch, &client).await
}

// region main work loop

/// Core loop: request work, process it, report results, repeat.
async fn work_loop(
    config: &AgentConfig,
    current_batch: &Arc<Mutex<Option<u64>>>,
    client: &reqwest::Client,
) -> anyhow::Result<()> {
    loop {
        // 1. Request a task batch
        let assignment = match request_task(config, client).await {
            Ok(assignment) => assignment,
            Err(e) => {
                tracing::warn!("No work available or error requesting task: {}", e);
                // Back off before retrying
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        tracing::info!(
            "Agent {} received batch {} with {} images",
            config.agent_id,
            assignment.batch_id,
            assignment.image_paths.len()
        );

        // Advertise the current batch to the heartbeat loop.
        *current_batch.lock().unwrap() = Some(assignment.batch_id);

        // 2. Process each image in the batch
        let labels = process_batch(config, &assignment).await?;

        // Clear batch slot before reporting — the batch is no longer in-flight.
        *current_batch.lock().unwrap() = None;

        // 3. Report completion
        let completion = TaskCompletion {
            batch_id: assignment.batch_id,
            agent_id: config.agent_id.clone(),
            labels,
        };

        match report_completion(config, client, completion).await {
            Ok(response) => {
                tracing::info!(
                    "Batch {} completion {}: {}",
                    assignment.batch_id,
                    if response.accepted {
                        "accepted"
                    } else {
                        "rejected"
                    },
                    response.message
                );
            }
            Err(e) => {
                // Not fatal — the batch will either be accepted on retry
                // or expire and be reassigned.
                tracing::error!(
                    "Failed to report completion for batch {}: {}",
                    assignment.batch_id,
                    e
                );
            }
        }
    }
}

// endregion

// region local controller communication

/// Request a task batch from the local controller (which proxies to CC).
/// Falls back to contacting a CC directly if the LC is unreachable.
async fn request_task(
    config: &AgentConfig,
    client: &reqwest::Client,
) -> anyhow::Result<TaskAssignment> {
    let body = TaskRequest {
        agent_id: config.agent_id.clone(),
    };

    // Try the local controller first.
    let lc_url = format!("http://{}/agent/request_task", config.lc_addr);
    match client.post(&lc_url).json(&body).send().await {
        Ok(resp) if resp.status().is_success() => {
            return Ok(resp.json::<TaskAssignment>().await?);
        }
        Ok(resp) => {
            tracing::warn!(
                "LC returned non-success status {}, falling back to CC",
                resp.status()
            );
        }
        Err(e) => {
            tracing::warn!(
                "LC unreachable at {}: {}, trying CC directly",
                config.lc_addr,
                e
            );
        }
    }

    // Fallback: try each CC address. Non-leaders will redirect to the leader
    // (reqwest follows redirects automatically by default).
    for cc_addr in &config.cc_addrs {
        let url = format!("http://{}/task/request", cc_addr);
        match client.post(&url).json(&body).send().await {
            Ok(resp) if resp.status().is_success() => {
                return Ok(resp.json::<TaskAssignment>().await?);
            }
            Ok(resp) => {
                tracing::debug!("CC {} returned status {}", cc_addr, resp.status());
            }
            Err(e) => {
                tracing::debug!("CC {} unreachable: {}", cc_addr, e);
            }
        }
    }

    anyhow::bail!("No task available from LC or any CC")
}

/// Report task completion to the local controller (which proxies to CC).
/// Retries up to 3 times with exponential backoff on transient failures.
async fn report_completion(
    config: &AgentConfig,
    client: &reqwest::Client,
    completion: TaskCompletion,
) -> anyhow::Result<TaskCompletionResponse> {
    let url = format!("http://{}/agent/complete", config.lc_addr);

    let mut delay = Duration::from_secs(1);
    for attempt in 1..=3u32 {
        match client.post(&url).json(&completion).send().await {
            Ok(resp) if resp.status().is_success() => {
                return Ok(resp.json::<TaskCompletionResponse>().await?);
            }
            Ok(resp) => {
                tracing::warn!(
                    "Completion attempt {}/3 for batch {} returned status {}",
                    attempt,
                    completion.batch_id,
                    resp.status()
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Completion attempt {}/3 for batch {} failed: {}",
                    attempt,
                    completion.batch_id,
                    e
                );
            }
        }
        if attempt < 3 {
            tokio::time::sleep(delay).await;
            delay *= 2;
        }
    }

    anyhow::bail!(
        "Failed to report completion for batch {} after 3 attempts",
        completion.batch_id
    )
}

/// Send a heartbeat to the local controller.
async fn send_heartbeat(
    config: &AgentConfig,
    client: &reqwest::Client,
    current_batch_id: Option<u64>,
) -> anyhow::Result<()> {
    let url = format!("http://{}/agent/heartbeat", config.lc_addr);
    let body = AgentHeartbeat {
        agent_id: config.agent_id.clone(),
        current_batch_id,
    };

    client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Heartbeat failed: {}", e))?;

    Ok(())
}

// endregion

// region feature extraction

/// Process all images in a batch by running the feature extractor on each.
/// Returns a list of (image_path, label) pairs.
async fn process_batch(
    config: &AgentConfig,
    assignment: &TaskAssignment,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut results = Vec::with_capacity(assignment.image_paths.len());

    for image_path in &assignment.image_paths {
        let full_path = format!("{}/{}", config.image_base_path, image_path);
        let label = run_feature_extractor(&config.python, &config.extractor_script, &full_path).await?;
        results.push((image_path.clone(), label));
    }

    Ok(results)
}

/// Run the provided Python feature extraction script on a single image.
/// Returns the predicted label (the trimmed first line of stdout).
async fn run_feature_extractor(python: &str, script_path: &str, image_path: &str) -> anyhow::Result<String> {
    let output = tokio::process::Command::new(python)
        .arg(script_path)
        .arg(image_path)
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to spawn {}: {}", python, e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "Feature extractor exited with {}: {}",
            output.status,
            stderr.trim()
        );
    }

    let label = String::from_utf8(output.stdout)
        .map_err(|e| anyhow::anyhow!("Non-UTF8 output from feature extractor: {}", e))?;

    // Take the first non-empty line as the label.
    let label = label
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim()
        .to_string();

    if label.is_empty() {
        anyhow::bail!("Feature extractor produced no output for {}", image_path);
    }

    Ok(label)
}

// endregion

// region background heartbeat

/// Periodically sends heartbeats to the local controller every 5 seconds,
/// including the current batch ID so the LC can report accurate load.
async fn heartbeat_loop(
    config: Arc<AgentConfig>,
    current_batch: Arc<Mutex<Option<u64>>>,
    client: reqwest::Client,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let batch_id = *current_batch.lock().unwrap();
        if let Err(e) = send_heartbeat(&config, &client, batch_id).await {
            tracing::warn!("Heartbeat error: {}", e);
        }
    }
}

// endregion
