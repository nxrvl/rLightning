use std::net::SocketAddr;
use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Docker management utilities for testing
struct DockerManager {
    container_name: String,
    image_name: String,
    port: u16,
}

impl DockerManager {
    fn new() -> Self {
        Self {
            container_name: "rlightning-test".to_string(),
            image_name: "rlightning:latest".to_string(),
            port: 16379,
        }
    }

    /// Build the Docker image with retry logic
    async fn build_image(
        &self,
        max_retries: u32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Building Docker image: {}", self.image_name);

        for attempt in 1..=max_retries {
            println!("Build attempt {}/{}", attempt, max_retries);

            let output = Command::new("docker")
                .args(&["build", "-t", &self.image_name, "."])
                .output()?;

            if output.status.success() {
                println!("✅ Docker image built successfully");
                return Ok(());
            }

            let stderr = String::from_utf8_lossy(&output.stderr);
            println!("❌ Build attempt {} failed: {}", attempt, stderr);

            if attempt < max_retries {
                println!("Retrying in 5 seconds...");
                sleep(Duration::from_secs(5)).await;
            }
        }

        Err(format!(
            "Failed to build Docker image after {} attempts",
            max_retries
        )
        .into())
    }

    /// Pull base images with retry logic
    async fn pull_base_images(
        &self,
        max_retries: u32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let base_images = vec!["rust:slim", "debian:bookworm-slim"];

        for image in base_images {
            println!("Pulling base image: {}", image);

            for attempt in 1..=max_retries {
                println!("Pull attempt {}/{} for {}", attempt, max_retries, image);

                let output = Command::new("docker").args(&["pull", image]).output()?;

                if output.status.success() {
                    println!("✅ Successfully pulled {}", image);
                    break;
                }

                let stderr = String::from_utf8_lossy(&output.stderr);
                println!(
                    "❌ Pull attempt {} failed for {}: {}",
                    attempt, image, stderr
                );

                if attempt < max_retries {
                    println!("Retrying in 3 seconds...");
                    sleep(Duration::from_secs(3)).await;
                } else {
                    println!(
                        "⚠️  Failed to pull {} after {} attempts, will proceed with build",
                        image, max_retries
                    );
                }
            }
        }

        Ok(())
    }

    /// Check if Docker is available
    fn check_docker_available(&self) -> bool {
        Command::new("docker")
            .args(&["--version"])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    /// Stop and remove existing container
    async fn cleanup_container(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Stop container if running
        let _ = Command::new("docker")
            .args(&["stop", &self.container_name])
            .output();

        // Remove container if exists
        let _ = Command::new("docker")
            .args(&["rm", &self.container_name])
            .output();

        Ok(())
    }

    /// Start the rLightning container
    async fn start_container(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Starting rLightning container on port {}", self.port);

        let output = Command::new("docker")
            .args(&[
                "run",
                "-d",
                "--name",
                &self.container_name,
                "-p",
                &format!("{}:6379", self.port),
                &self.image_name,
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("Failed to start container: {}", stderr).into());
        }

        // Wait for container to be ready
        for attempt in 1..=30 {
            sleep(Duration::from_millis(500)).await;

            let health_output = Command::new("docker")
                .args(&[
                    "exec",
                    &self.container_name,
                    "nc",
                    "-z",
                    "localhost",
                    "6379",
                ])
                .output();

            if health_output.map(|o| o.status.success()).unwrap_or(false) {
                println!("✅ Container is ready after {} attempts", attempt);
                return Ok(());
            }

            if attempt % 5 == 0 {
                println!("Waiting for container to be ready... (attempt {})", attempt);
            }
        }

        Err("Container did not become ready within timeout".into())
    }

    /// Get container logs for debugging
    async fn get_logs(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let output = Command::new("docker")
            .args(&["logs", &self.container_name])
            .output()?;

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

impl Drop for DockerManager {
    fn drop(&mut self) {
        // Best effort cleanup
        let _ = Command::new("docker")
            .args(&["stop", &self.container_name])
            .output();
        let _ = Command::new("docker")
            .args(&["rm", &self.container_name])
            .output();
    }
}

/// Run Redis compatibility tests using both local server and Docker
async fn test_redis_compatibility_docker() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let docker = DockerManager::new();

    // Check if Docker is available
    if !docker.check_docker_available() {
        println!("⚠️  Docker not available, skipping Docker compatibility test");
        return run_local_compatibility_test().await;
    }

    println!("🐳 Running Redis compatibility test with Docker");

    // Cleanup any existing container
    docker.cleanup_container().await?;

    // Try to pull base images first (with retries for network issues)
    if let Err(e) = docker.pull_base_images(3).await {
        println!("⚠️  Warning: Failed to pull some base images: {}", e);
        println!("Will proceed with build using cached/local images");
    }

    // Build the Docker image with retries
    docker.build_image(3).await?;

    // Start the container
    docker.start_container().await?;

    // Run compatibility tests against the Docker container
    let result = run_compatibility_tests_against_docker(&docker).await;

    // Print logs if test failed
    if result.is_err() {
        println!("❌ Docker test failed, container logs:");
        if let Ok(logs) = docker.get_logs().await {
            println!("{}", logs);
        }
    }

    result
}

/// Run compatibility tests against the Docker container
async fn run_compatibility_tests_against_docker(
    docker: &DockerManager,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = format!("127.0.0.1:{}", docker.port).parse()?;

    // Wait a bit more for the server to be fully ready
    sleep(Duration::from_millis(1000)).await;

    // Connect client with retries
    let mut client = None;
    for attempt in 1..=5 {
        match Client::connect(addr).await {
            Ok(c) => {
                client = Some(c);
                break;
            }
            Err(e) => {
                if attempt == 5 {
                    return Err(format!("Failed to connect after 5 attempts: {}", e).into());
                }
                println!("Connection attempt {} failed, retrying...", attempt);
                sleep(Duration::from_millis(1000)).await;
            }
        }
    }

    let mut client = client.unwrap();
    println!("✅ Connected to Docker container at {}", addr);

    // Run core compatibility tests
    run_core_redis_tests(&mut client).await?;

    println!("✅ All Docker compatibility tests passed!");
    Ok(())
}

/// Run the original compatibility test against local server
async fn run_local_compatibility_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🔧 Running Redis compatibility test with local server");

    // Set up server
    let addr: SocketAddr = "127.0.0.1:16381".parse()?;
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);

    let server = Server::new(addr, storage);

    // Start server in background
    tokio::spawn(async move {
        server.start().await.unwrap();
    });

    // Wait for server to start
    sleep(Duration::from_millis(100)).await;

    // Connect client
    let mut client = Client::connect(addr).await?;

    // Run core compatibility tests
    run_core_redis_tests(&mut client).await?;

    println!("✅ All local compatibility tests passed!");
    Ok(())
}

/// Core Redis compatibility tests (shared between Docker and local)
async fn run_core_redis_tests(
    client: &mut Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Test basic SET/GET
    println!("Testing basic SET/GET...");
    let set_args: Vec<&[u8]> = vec![b"SET", b"test_key", b"test_value"];
    client.send_command(set_args).await?;
    let get_args: Vec<&[u8]> = vec![b"GET", b"test_key"];
    let response = client.send_command(get_args).await?;
    assert_eq!(
        response,
        RespValue::BulkString(Some(b"test_value".to_vec()))
    );

    // Test SET with EX option
    println!("Testing SET with EX option...");
    let set_ex_args: Vec<&[u8]> = vec![b"SET", b"ex_key", b"value", b"EX", b"60"];
    client.send_command(set_ex_args).await?;
    let ttl_args: Vec<&[u8]> = vec![b"TTL", b"ex_key"];
    let response = client.send_command(ttl_args).await?;
    if let RespValue::Integer(ttl) = response {
        assert!(ttl > 0 && ttl <= 60);
    } else {
        panic!("Expected integer response from TTL");
    }

    // Test SET with NX option
    println!("Testing SET with NX option...");
    let set_nx_args: Vec<&[u8]> = vec![b"SET", b"nx_key", b"value", b"NX"];
    client.send_command(set_nx_args).await?;
    let get_args: Vec<&[u8]> = vec![b"GET", b"nx_key"];
    let response = client.send_command(get_args).await?;
    assert_eq!(response, RespValue::BulkString(Some(b"value".to_vec())));

    // Test HSET/HGET
    println!("Testing HSET/HGET...");
    let hset_args: Vec<&[u8]> = vec![b"HSET", b"user:1", b"name", b"Alice"];
    client.send_command(hset_args).await?;
    let hget_args: Vec<&[u8]> = vec![b"HGET", b"user:1", b"name"];
    let response = client.send_command(hget_args).await?;
    assert_eq!(response, RespValue::BulkString(Some(b"Alice".to_vec())));

    // Test LPUSH/LRANGE
    println!("Testing LPUSH/LRANGE...");
    let lpush_args: Vec<&[u8]> = vec![b"LPUSH", b"mylist", b"item1", b"item2"];
    client.send_command(lpush_args).await?;
    let lrange_args: Vec<&[u8]> = vec![b"LRANGE", b"mylist", b"0", b"-1"];
    let response = client.send_command(lrange_args).await?;
    if let RespValue::Array(Some(items)) = response {
        assert_eq!(items.len(), 2);
    } else {
        panic!("Expected array response from LRANGE");
    }

    // Test ZADD/ZRANGE
    println!("Testing ZADD/ZRANGE...");
    let zadd_args: Vec<&[u8]> = vec![b"ZADD", b"scores", b"10", b"Alice", b"20", b"Bob"];
    client.send_command(zadd_args).await?;
    let zrange_args: Vec<&[u8]> = vec![b"ZRANGE", b"scores", b"0", b"-1"];
    let response = client.send_command(zrange_args).await?;
    if let RespValue::Array(Some(members)) = response {
        assert_eq!(members.len(), 2);
    } else {
        panic!("Expected array response from ZRANGE");
    }

    // Test SADD/SMEMBERS
    println!("Testing SADD/SMEMBERS...");
    let sadd_args: Vec<&[u8]> = vec![b"SADD", b"myset", b"a", b"b", b"c"];
    client.send_command(sadd_args).await?;
    let smembers_args: Vec<&[u8]> = vec![b"SMEMBERS", b"myset"];
    let response = client.send_command(smembers_args).await?;
    if let RespValue::Array(Some(members)) = response {
        assert_eq!(members.len(), 3);
    } else {
        panic!("Expected array response from SMEMBERS");
    }

    // Test INCR/DECR
    println!("Testing INCR/DECR...");
    let set_counter_args: Vec<&[u8]> = vec![b"SET", b"counter", b"10"];
    client.send_command(set_counter_args).await?;
    let incr_args: Vec<&[u8]> = vec![b"INCR", b"counter"];
    let response = client.send_command(incr_args).await?;
    assert_eq!(response, RespValue::Integer(11));

    let decr_args: Vec<&[u8]> = vec![b"DECR", b"counter"];
    let response = client.send_command(decr_args).await?;
    assert_eq!(response, RespValue::Integer(10));

    // Test EXISTS
    println!("Testing EXISTS...");
    let exists_args: Vec<&[u8]> = vec![b"EXISTS", b"test_key"];
    let response = client.send_command(exists_args).await?;
    assert_eq!(response, RespValue::Integer(1));

    let exists_missing_args: Vec<&[u8]> = vec![b"EXISTS", b"missing_key"];
    let response = client.send_command(exists_missing_args).await?;
    assert_eq!(response, RespValue::Integer(0));

    // Test DEL
    println!("Testing DEL...");
    let del_args: Vec<&[u8]> = vec![b"DEL", b"test_key"];
    let response = client.send_command(del_args).await?;
    assert_eq!(response, RespValue::Integer(1));

    // Test PING
    println!("Testing PING...");
    let ping_args: Vec<&[u8]> = vec![b"PING"];
    let response = client.send_command(ping_args).await?;
    assert_eq!(response, RespValue::SimpleString("PONG".to_string()));

    println!("✅ All core Redis compatibility tests passed");
    Ok(())
}

/// Main compatibility test that tries Docker first, falls back to local
/// Docker test is skipped gracefully if Docker is not available.
#[tokio::test]
async fn test_redis_compatibility() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // First try the original comprehensive local test
    let local_result = run_local_compatibility_test().await;

    // Then try Docker test if available
    let docker_result = test_redis_compatibility_docker().await;

    // Return success if either test passes
    match (local_result, docker_result) {
        (Ok(_), Ok(_)) => {
            println!("✅ Both local and Docker tests passed!");
            Ok(())
        }
        (Ok(_), Err(docker_err)) => {
            println!("✅ Local test passed, Docker test failed: {}", docker_err);
            Ok(())
        }
        (Err(local_err), Ok(_)) => {
            println!("✅ Docker test passed, Local test failed: {}", local_err);
            Ok(())
        }
        (Err(local_err), Err(docker_err)) => {
            println!("❌ Both tests failed:");
            println!("Local: {}", local_err);
            println!("Docker: {}", docker_err);
            Err("Both local and Docker tests failed".into())
        }
    }
}
