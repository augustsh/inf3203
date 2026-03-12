use rand::Rng;
use rand::seq::SliceRandom;
use serde_json::json;
use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

// Check if a specific port is in use on a remote node via SSH
fn port_in_use(node: &str, port: u16) -> bool {
    let output = Command::new("ssh")
        .arg(node)
        .arg(format!("ss -ltnp | grep :{}", port))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output();

    // If ssh worked and the command found something, the port is in use
    match output {
        Ok(output) => output.status.success(),
        Err(e) => {
            eprintln!("Failed to execute ssh command: {}", e);
            false // Assume port is not in use if check fails
        }
    }
}

// Find a free port on a remote node by randomly selecting ports and checking if they are in use
fn find_free_port(node: &str, max_attempts: u32) -> Option<u16> {
    let mut rng = rand::rng();
    for _ in 0..max_attempts {
        // Generate a random port
        let port: u16 = rng.random_range(49152..=65535);

        // Check if the port is in use
        if !port_in_use(&node, port) {
            return Some(port); // Return the free port
        }

        // Wait a bit before trying again to avoid rapid-fire checks
        thread::sleep(Duration::from_millis(50));
    }
    None // Return None if no free port is found after max_attempts
}

fn main() {
    // Read number of servers from command line arguments
    let args: Vec<String> = env::args().collect();

    // Number of servers to deploy
    let num_servers: usize = args[1]
        .parse()
        .expect("number of servers must be an integer");

    // Get the current working directory
    let current_dir = env::current_dir().expect("failed to get current directory");

    // Create absolute path to run-node.sh
    let run_node_path = current_dir.join("run-node.sh");
    let run_node_path = run_node_path.to_str().unwrap();

    // Download run-node.sh if it doesn't exist
    if !std::path::Path::new(run_node_path).exists() {
        println!("Downloading run-node.sh...");
        
        // Try to read version from .env file, otherwise use default
        let version = std::fs::read_to_string(current_dir.parent().unwrap().join(".env"))
            .ok()
            .and_then(|content| {
                content.lines()
                    .find(|line| line.starts_with("VERSION="))
                    .and_then(|line| line.split('=').nth(1))
                    .map(|v| v.trim().to_string())
            })
            .unwrap_or_else(|| "v0.0.1".to_string());
        
        let download_url = format!(
            "https://github.com/VikingTheDev/inf3203/releases/download/{}/run-node.sh",
            version
        );
        
        let status = Command::new("curl")
            .args([
                "-L",
                "-o",
                run_node_path,
                &download_url,
            ])
            .status()
            .expect("failed to download run-node.sh");

        if !status.success() {
            eprintln!("Failed to download run-node.sh");
            std::process::exit(1);
        }

        // Make the script executable
        fs::set_permissions(run_node_path, fs::Permissions::from_mode(0o775))
            .expect("failed to set permissions on run-node.sh");
    }

    // Get the list of available nodes
    let output = Command::new("/share/ifi/available-nodes.sh")
        .stdout(Stdio::piped())
        .output()
        .expect("failed to execute available-nodes.sh");

    // Convert output into a list of node names
    let nodes: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Ensure that there are available nodes
    if nodes.is_empty() {
        eprintln!("No available nodes found.");
        std::process::exit(1);
    }

    // Shuffle the nodes to distribute load
    let mut rng = rand::rng();
    let mut shuffled_nodes = nodes.clone();
    shuffled_nodes.shuffle(&mut rng);

    let mut servers = Vec::new();

    let mut servers_started = 0;
    let mut counter = 0;

    while servers_started < num_servers {
        // Select a node in a round-robin fashion
        let node = &shuffled_nodes[counter % shuffled_nodes.len()];

        counter += 1;

        // Find a free port on the selected node
        let port = match find_free_port(node, 20) {
            Some(port) => port,
            None => {
                eprintln!("Failed to find a free port on node {}", node);
                continue; // Skip this server if no free port is found
            }
        };

        let ssh_cmd = format!("bash {} {} {}", run_node_path, node, port);

        // Start the web server on the selected port
        let status = Command::new("ssh")
            .arg(node)
            .arg(ssh_cmd)
            .status()
            .expect("failed to execute ssh command");

        if status.success() {
            servers.push(format!("{}:{}", node, port));
            servers_started += 1;
        } else {
            eprintln!(
                "Failed to start server on {}:{}\nTrying another node.",
                node, port
            );
        }
    }

    println!("Initialized DHT on {} nodes.", servers.len());
    println!("Node list (in json format for testscript.py):");
    // Output the list of servers in JSON format
    println!("'{}'", json!(servers).to_string());
    println!("");
    // Output the list of servers in plain text format
    println!("Node list (plain text for other test scripts):");
    for server in &servers {
        print!("{} ", server);
    }
    println!();
}
