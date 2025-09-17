use criterion::{criterion_group, criterion_main, Criterion};
use std::process::Command as ProcessCommand;
use std::sync::Once;
use std::time::Duration;
use redis::{Client, Connection};

// Configure benchmark parameters
const CONNECTION_TIMEOUT: u64 = 10;
const DOCKER_MEMORY_LIMIT: &str = "128m";
const DOCKER_CPU_LIMIT: &str = "1.0";
const STARTUP_WAIT_TIME: u64 = 10;
const DOCKER_BUILD_RETRIES: u8 = 3;
const CONNECTION_RETRY_SLEEP_MS: u64 = 1000;

// Redis Docker settings (using Redis Stack for JSON support)
const REDIS_PORT: u16 = 6379;
const REDIS_CONTAINER_NAME: &str = "benchmark-redis";
const REDIS_IMAGE: &str = "redis/redis-stack:latest";

// rLightning Docker settings
const RLIGHTNING_PORT: u16 = 6380;
const RLIGHTNING_CONTAINER_NAME: &str = "benchmark-rlightning";
const RLIGHTNING_CONNECTION_RETRIES: u8 = 5;

// Static flag to ensure we only build the image once
static BUILD_RLIGHTNING_IMAGE: Once = Once::new();

// Helper function to run a command and print its output
fn run_command(cmd: &mut ProcessCommand) -> Result<(), String> {
    let output = cmd.output().map_err(|e| format!("Failed to execute command: {}", e))?;
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Command failed: {}", stderr));
    }
    
    Ok(())
}

// Helper function to run a command with retries
fn run_command_with_retries(cmd: &mut ProcessCommand, retries: u8) -> Result<(), String> {
    let mut last_error = None;
    
    for attempt in 1..=retries {
        match cmd.output() {
            Ok(output) => {
                if output.status.success() {
                    if attempt > 1 {
                        eprintln!("Command succeeded on attempt {}", attempt);
                    }
                    return Ok(());
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    let error_msg = format!("Command failed: {}", stderr);
                    if attempt < retries {
                        eprintln!("Attempt {} failed: {}", attempt, error_msg);
                        eprintln!("Retrying in 3 seconds...");
                        std::thread::sleep(Duration::from_secs(3));
                    }
                    last_error = Some(error_msg);
                }
            },
            Err(e) => {
                let error_msg = format!("Failed to execute command: {}", e);
                if attempt < retries {
                    eprintln!("Attempt {} failed: {}", attempt, error_msg);
                    eprintln!("Retrying in 3 seconds...");
                    std::thread::sleep(Duration::from_secs(3));
                }
                last_error = Some(error_msg);
            }
        }
    }
    
    Err(last_error.unwrap_or_else(|| "Command failed after multiple attempts".to_string()))
}

// Build rLightning Docker image - called only once across all benchmarks
fn build_rlightning_image() -> Result<(), String> {
    eprintln!("Building rLightning Docker image (this will only happen once)...");
    run_command_with_retries(
        &mut ProcessCommand::new("docker")
            .args([
                "build",
                "--quiet",
                "-t", "rlightning:latest",
                "."
            ]),
        DOCKER_BUILD_RETRIES
    )
}

// Start Redis in Docker
fn start_redis() -> Result<(), String> {
    // Stop any existing container
    let _ = ProcessCommand::new("docker")
        .args(["stop", REDIS_CONTAINER_NAME])
        .output();
    
    let _ = ProcessCommand::new("docker")
        .args(["rm", REDIS_CONTAINER_NAME])
        .output();
    
    // Start Redis with memory and CPU limits, protected mode disabled
    run_command(
        ProcessCommand::new("docker")
            .args([
                "run",
                "-d",
                "--name", REDIS_CONTAINER_NAME,
                "-p", &format!("{}:6379", REDIS_PORT),
                "--memory", DOCKER_MEMORY_LIMIT,
                "--cpus", DOCKER_CPU_LIMIT,
                REDIS_IMAGE,
                "redis-server",
                "--maxmemory", "100mb",
                "--maxmemory-policy", "allkeys-lru",
                "--protected-mode", "no"
            ])
    )?;
    
    // Wait for Redis to start
    std::thread::sleep(Duration::from_secs(STARTUP_WAIT_TIME));
    
    Ok(())
}

// Start rLightning in Docker
fn start_rlightning() -> Result<(), String> {
    // Build the image only once across all benchmark runs
    BUILD_RLIGHTNING_IMAGE.call_once(|| {
        if let Err(e) = build_rlightning_image() {
            eprintln!("Warning: Failed to build rLightning image: {}", e);
            eprintln!("rLightning benchmarks will be skipped.");
        }
    });
    
    // Stop any existing container
    let _ = ProcessCommand::new("docker")
        .args(["stop", RLIGHTNING_CONTAINER_NAME])
        .output();
    
    let _ = ProcessCommand::new("docker")
        .args(["rm", RLIGHTNING_CONTAINER_NAME])
        .output();
    
    // Start rLightning with memory and CPU limits
    eprintln!("Starting rLightning container...");
    run_command(
        ProcessCommand::new("docker")
            .args([
                "run",
                "-d",
                "--name", RLIGHTNING_CONTAINER_NAME,
                "-p", &format!("{}:6379", RLIGHTNING_PORT),
                "--memory", DOCKER_MEMORY_LIMIT,
                "--cpus", DOCKER_CPU_LIMIT,
                "-v", "./config/benchmark.toml:/etc/rlightning/config.toml",
                "-e", "RUST_LOG=warn",
                "rlightning:latest",
                "rlightning",
                "--config", "/etc/rlightning/config.toml"
            ])
    )?;
    
    // Wait for rLightning to start
    eprintln!("Waiting for rLightning to start ({} seconds)...", STARTUP_WAIT_TIME);
    std::thread::sleep(Duration::from_secs(STARTUP_WAIT_TIME));
    
    Ok(())
}

// Stop Docker containers
fn cleanup() {
    eprintln!("Cleaning up Docker containers...");
    let _ = ProcessCommand::new("docker")
        .args(["stop", REDIS_CONTAINER_NAME, RLIGHTNING_CONTAINER_NAME])
        .output();
}

// Connect to Redis
fn connect_to_redis(port: u16) -> Result<Client, redis::RedisError> {
    let url = format!("redis://localhost:{}", port);
    eprintln!("Connecting to Redis on {}...", url);
    let client = Client::open(url)?;
    // Test the connection to ensure it works before proceeding
    let conn = client.get_connection_with_timeout(std::time::Duration::from_secs(CONNECTION_TIMEOUT))?;
    drop(conn);
    Ok(client)
}

// Connect to rLightning with retries
fn connect_to_rlightning() -> Result<Client, redis::RedisError> {
    let mut last_error = None;
    
    eprintln!("Attempting to connect to rLightning on port {}...", RLIGHTNING_PORT);
    
    for attempt in 1..=RLIGHTNING_CONNECTION_RETRIES {
        match connect_to_redis(RLIGHTNING_PORT) {
            Ok(client) => {
                eprintln!("Successfully connected to rLightning on attempt {}", attempt);
                return Ok(client);
            },
            Err(e) => {
                eprintln!("Failed to connect to rLightning on attempt {}: {} ({:?})", 
                          attempt, e, e.kind());
                last_error = Some(e);
                if attempt < RLIGHTNING_CONNECTION_RETRIES {
                    eprintln!("Retrying in {} ms...", CONNECTION_RETRY_SLEEP_MS);
                    std::thread::sleep(Duration::from_millis(CONNECTION_RETRY_SLEEP_MS));
                }
            }
        }
    }
    
    Err(last_error.unwrap_or_else(|| {
        redis::RedisError::from(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to connect to rLightning after multiple attempts"
        ))
    }))
}

// Get a new connection from a client with retries
fn get_connection_with_retry(client: &Client, retries: u8) -> Result<Connection, redis::RedisError> {
    let mut last_error = None;
    
    for attempt in 1..=retries {
        match client.get_connection_with_timeout(std::time::Duration::from_secs(CONNECTION_TIMEOUT)) {
            Ok(conn) => {
                if attempt > 1 {
                    eprintln!("Successfully established connection on attempt {}", attempt);
                }
                return Ok(conn);
            },
            Err(e) => {
                eprintln!("Failed to get connection on attempt {}: {} ({:?})", 
                          attempt, e, e.kind());
                last_error = Some(e);
                if attempt < retries {
                    std::thread::sleep(Duration::from_millis(CONNECTION_RETRY_SLEEP_MS));
                }
            }
        }
    }
    
    Err(last_error.unwrap_or_else(|| {
        redis::RedisError::from(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to get connection after multiple attempts"
        ))
    }))
}

// Test if JSON commands are supported on the server
// This allows the benchmark to work with both standard Redis and Redis Stack
fn supports_json_commands(client: &Client) -> bool {
    if let Ok(mut conn) = get_connection_with_retry(client, 3) {
        match redis::cmd("JSON.SET")
            .arg("test_json_support")
            .arg("$")
            .arg(r#"{"test": true}"#)
            .query::<String>(&mut conn)
        {
            Ok(_) => {
                // Clean up test key
                let _ = redis::cmd("DEL").arg("test_json_support").query::<i32>(&mut conn);
                true
            },
            Err(_) => false,
        }
    } else {
        false
    }
}

// Helper function to setup connections for both servers
fn setup_connections() -> (Option<Client>, Option<Client>) {
    // Start Redis
    if let Err(e) = start_redis() {
        eprintln!("Failed to start Redis: {}", e);
        return (None, None);
    }
    
    // Attempt to start rLightning
    let start_rl_result = start_rlightning();
    let rlightning_available = start_rl_result.is_ok();
    
    if let Err(e) = &start_rl_result {
        eprintln!("Warning: Failed to start rLightning: {}", e);
        eprintln!("Continuing with Redis-only benchmarks...");
    }
    
    // Connect to Redis
    let redis_client = match connect_to_redis(REDIS_PORT) {
        Ok(client) => Some(client),
        Err(e) => {
            eprintln!("Failed to connect to Redis on port {}: {} ({:?})", REDIS_PORT, e, e.kind());
            None
        }
    };
    
    // Connect to rLightning if available
    let rlightning_client = if rlightning_available {
        match connect_to_rlightning() {
            Ok(client) => {
                // Verify rLightning connection with a simple command
                let conn_result = get_connection_with_retry(&client, 3);
                if let Err(e) = conn_result {
                    eprintln!("Connection to rLightning failed verification: {} ({:?})", e, e.kind());
                    eprintln!("Skipping rLightning benchmarks...");
                    None
                } else {
                    Some(client)
                }
            },
            Err(e) => {
                eprintln!("Failed to connect to rLightning: {} ({:?})", e, e.kind());
                eprintln!("Continuing with Redis-only benchmarks...");
                None
            }
        }
    } else {
        None
    };
    
    (redis_client, rlightning_client)
}

// Macro to create benchmark functions
macro_rules! bench_command {
    ($name:ident, $test_name:expr, $setup:expr, $redis_cmd:expr, $rl_cmd:expr) => {
        fn $name(c: &mut Criterion) {
            let (redis_client, rlightning_client) = setup_connections();
            
            if redis_client.is_none() && rlightning_client.is_none() {
                cleanup();
                return;
            }
            
            let mut group = c.benchmark_group($test_name);
            
            // Setup test data if needed
            if let Some(setup_fn) = $setup {
                if let Some(client) = &redis_client {
                    setup_fn(client);
                }
                if let Some(client) = &rlightning_client {
                    setup_fn(client);
                }
            }
            
            // Benchmark Redis if available
            if let Some(client) = &redis_client {
                group.bench_function("Redis", |b| {
                    b.iter_with_setup(
                        || {
                            match get_connection_with_retry(&client, 3) {
                                Ok(conn) => conn,
                                Err(e) => {
                                    eprintln!("Failed to get Redis connection: {} ({:?})", e, e.kind());
                                    panic!("Failed to connect to Redis");
                                }
                            }
                        },
                        |mut conn| {
                            if let Err(e) = $redis_cmd(&mut conn) {
                                eprintln!("Redis operation failed: {} ({:?})", e, e.kind());
                                panic!("Redis operation failed");
                            }
                        }
                    );
                });
            }
            
            // Benchmark rLightning if available
            if let Some(client) = &rlightning_client {
                group.bench_function("rLightning", |b| {
                    b.iter_with_setup(
                        || {
                            match get_connection_with_retry(&client, 3) {
                                Ok(conn) => conn,
                                Err(e) => {
                                    eprintln!("Failed to get rLightning connection: {} ({:?})", e, e.kind());
                                    panic!("Failed to connect to rLightning");
                                }
                            }
                        },
                        |mut conn| {
                            if let Err(e) = $rl_cmd(&mut conn) {
                                eprintln!("rLightning operation failed: {} ({:?})", e, e.kind());
                                panic!("rLightning operation failed");
                            }
                        }
                    );
                });
            }
            
            group.finish();
            cleanup();
        }
    };
}

// Macro to create JSON benchmark functions with support detection
macro_rules! bench_json_command {
    ($name:ident, $test_name:expr, $setup:expr, $redis_cmd:expr, $rl_cmd:expr) => {
        fn $name(c: &mut Criterion) {
            let (redis_client, rlightning_client) = setup_connections();
            
            if redis_client.is_none() && rlightning_client.is_none() {
                cleanup();
                return;
            }
            
            // Check if Redis supports JSON commands
            let redis_supports_json = redis_client.as_ref()
                .map(|client| {
                    let supports = supports_json_commands(client);
                    eprintln!("Redis JSON support detection for {}: {}", $test_name, supports);
                    supports
                })
                .unwrap_or(false);
            
            if !redis_supports_json && redis_client.is_some() {
                eprintln!("Warning: Redis instance does not support JSON commands. Skipping Redis benchmark for {}.", $test_name);
            }
            
            let mut group = c.benchmark_group($test_name);
            
            // Setup test data if needed
            if let Some(setup_fn) = $setup {
                if let Some(client) = &redis_client {
                    if redis_supports_json {
                        setup_fn(client);
                    }
                }
                if let Some(client) = &rlightning_client {
                    setup_fn(client);
                }
            }
            
            // Benchmark Redis if available and supports JSON
            if let Some(client) = &redis_client {
                if redis_supports_json {
                    group.bench_function("Redis", |b| {
                        b.iter_with_setup(
                            || {
                                match get_connection_with_retry(&client, 3) {
                                    Ok(conn) => conn,
                                    Err(e) => {
                                        eprintln!("Failed to get Redis connection: {} ({:?})", e, e.kind());
                                        panic!("Failed to connect to Redis");
                                    }
                                }
                            },
                            |mut conn| {
                                if let Err(e) = $redis_cmd(&mut conn) {
                                    eprintln!("Redis operation failed: {} ({:?})", e, e.kind());
                                    panic!("Redis operation failed");
                                }
                            }
                        );
                    });
                }
            }
            
            // Benchmark rLightning if available
            if let Some(client) = &rlightning_client {
                group.bench_function("rLightning", |b| {
                    b.iter_with_setup(
                        || {
                            match get_connection_with_retry(&client, 3) {
                                Ok(conn) => conn,
                                Err(e) => {
                                    eprintln!("Failed to get rLightning connection: {} ({:?})", e, e.kind());
                                    panic!("Failed to connect to rLightning");
                                }
                            }
                        },
                        |mut conn| {
                            if let Err(e) = $rl_cmd(&mut conn) {
                                eprintln!("rLightning operation failed: {} ({:?})", e, e.kind());
                                panic!("rLightning operation failed");
                            }
                        }
                    );
                });
            }
            
            group.finish();
            cleanup();
        }
    };
}

// Helper function to populate test data
fn populate_test_data(client: &Client) {
    if let Ok(mut conn) = get_connection_with_retry(client, 3) {
        // Populate string data
        for i in 0..100 {
            let _ = redis::cmd("SET").arg(format!("key:{}", i)).arg(format!("value:{}", i)).query::<()>(&mut conn);
        }
        
        // Populate hash data
        for i in 0..50 {
            let _ = redis::cmd("HSET").arg(format!("hash:{}", i)).arg("field1").arg(format!("value:{}", i)).query::<()>(&mut conn);
            let _ = redis::cmd("HSET").arg(format!("hash:{}", i)).arg("field2").arg(format!("value2:{}", i)).query::<()>(&mut conn);
        }
        
        // Populate list data
        for i in 0..50 {
            let _ = redis::cmd("LPUSH").arg(format!("list:{}", i)).arg(format!("item1:{}", i)).query::<()>(&mut conn);
            let _ = redis::cmd("LPUSH").arg(format!("list:{}", i)).arg(format!("item2:{}", i)).query::<()>(&mut conn);
        }
        
        // Populate set data
        for i in 0..50 {
            let _ = redis::cmd("SADD").arg(format!("set:{}", i)).arg(format!("member1:{}", i)).query::<()>(&mut conn);
            let _ = redis::cmd("SADD").arg(format!("set:{}", i)).arg(format!("member2:{}", i)).query::<()>(&mut conn);
        }
        
        // Populate sorted set data
        for i in 0..50 {
            let _ = redis::cmd("ZADD").arg(format!("zset:{}", i)).arg(i as f64).arg(format!("member:{}", i)).query::<()>(&mut conn);
            let _ = redis::cmd("ZADD").arg(format!("zset:{}", i)).arg((i + 100) as f64).arg(format!("member2:{}", i)).query::<()>(&mut conn);
        }
    }
}

// String Operations Benchmarks
bench_command!(
    bench_set,
    "SET Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("bench_key_{}", fastrand::u32(..10000));
        let val = format!("value_{}", fastrand::u32(..10000));
        redis::cmd("SET").arg(&key).arg(&val).query::<()>(conn)
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("bench_key_{}", fastrand::u32(..10000));
        let val = format!("value_{}", fastrand::u32(..10000));
        redis::cmd("SET").arg(&key).arg(&val).query::<()>(conn)
    }
);

bench_command!(
    bench_get,
    "GET Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("key:{}", fastrand::u32(..100));
        redis::cmd("GET").arg(&key).query::<Option<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("key:{}", fastrand::u32(..100));
        redis::cmd("GET").arg(&key).query::<Option<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_mget,
    "MGET Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let base = fastrand::u32(..90);
        let keys: Vec<String> = (0..10).map(|i| format!("key:{}", base + i)).collect();
        redis::cmd("MGET").arg(&keys).query::<Vec<Option<String>>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let base = fastrand::u32(..90);
        let keys: Vec<String> = (0..10).map(|i| format!("key:{}", base + i)).collect();
        redis::cmd("MGET").arg(&keys).query::<Vec<Option<String>>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_mset,
    "MSET Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let base = fastrand::u32(..10000);
        let mut args = vec![];
        for i in 0..5 {
            args.push(format!("mset_key_{}_{}", base, i));
            args.push(format!("value_{}", i));
        }
        redis::cmd("MSET").arg(&args).query::<()>(conn)
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let base = fastrand::u32(..10000);
        let mut args = vec![];
        for i in 0..5 {
            args.push(format!("mset_key_{}_{}", base, i));
            args.push(format!("value_{}", i));
        }
        redis::cmd("MSET").arg(&args).query::<()>(conn)
    }
);

bench_command!(
    bench_incr,
    "INCR Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let _ = redis::cmd("SET").arg(format!("counter:{}", i)).arg("0").query::<()>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("counter:{}", fastrand::u32(..100));
        redis::cmd("INCR").arg(&key).query::<i64>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("counter:{}", fastrand::u32(..100));
        redis::cmd("INCR").arg(&key).query::<i64>(conn).map(|_| ())
    }
);

bench_command!(
    bench_del,
    "DEL Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("del_key_{}", fastrand::u32(..1000000));
        redis::cmd("SET").arg(&key).arg("value").query::<()>(conn)?;
        redis::cmd("DEL").arg(&key).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("del_key_{}", fastrand::u32(..1000000));
        redis::cmd("SET").arg(&key).arg("value").query::<()>(conn)?;
        redis::cmd("DEL").arg(&key).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_exists,
    "EXISTS Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("key:{}", fastrand::u32(..100));
        redis::cmd("EXISTS").arg(&key).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("key:{}", fastrand::u32(..100));
        redis::cmd("EXISTS").arg(&key).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_type,
    "TYPE Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("key:{}", fastrand::u32(..100));
        redis::cmd("TYPE").arg(&key).query::<String>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("key:{}", fastrand::u32(..100));
        redis::cmd("TYPE").arg(&key).query::<String>(conn).map(|_| ())
    }
);

bench_command!(
    bench_append,
    "APPEND Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("key:{}", fastrand::u32(..100));
        redis::cmd("APPEND").arg(&key).arg("_appended").query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("key:{}", fastrand::u32(..100));
        redis::cmd("APPEND").arg(&key).arg("_appended").query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_strlen,
    "STRLEN Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("key:{}", fastrand::u32(..100));
        redis::cmd("STRLEN").arg(&key).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("key:{}", fastrand::u32(..100));
        redis::cmd("STRLEN").arg(&key).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_decr,
    "DECR Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let _ = redis::cmd("SET").arg(format!("counter:{}", i)).arg("100").query::<()>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("counter:{}", fastrand::u32(..100));
        redis::cmd("DECR").arg(&key).query::<i64>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("counter:{}", fastrand::u32(..100));
        redis::cmd("DECR").arg(&key).query::<i64>(conn).map(|_| ())
    }
);

bench_command!(
    bench_incrby,
    "INCRBY Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let _ = redis::cmd("SET").arg(format!("counter:{}", i)).arg("0").query::<()>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("counter:{}", fastrand::u32(..100));
        redis::cmd("INCRBY").arg(&key).arg(5).query::<i64>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("counter:{}", fastrand::u32(..100));
        redis::cmd("INCRBY").arg(&key).arg(5).query::<i64>(conn).map(|_| ())
    }
);

bench_command!(
    bench_decrby,
    "DECRBY Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let _ = redis::cmd("SET").arg(format!("counter:{}", i)).arg("100").query::<()>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("counter:{}", fastrand::u32(..100));
        redis::cmd("DECRBY").arg(&key).arg(3).query::<i64>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("counter:{}", fastrand::u32(..100));
        redis::cmd("DECRBY").arg(&key).arg(3).query::<i64>(conn).map(|_| ())
    }
);

bench_command!(
    bench_setnx,
    "SETNX Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("setnx_key_{}", fastrand::u32(..10000));
        redis::cmd("SETNX").arg(&key).arg("value").query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("setnx_key_{}", fastrand::u32(..10000));
        redis::cmd("SETNX").arg(&key).arg("value").query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_setex,
    "SETEX Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("setex_key_{}", fastrand::u32(..10000));
        redis::cmd("SETEX").arg(&key).arg(60).arg("value").query::<()>(conn)
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("setex_key_{}", fastrand::u32(..10000));
        redis::cmd("SETEX").arg(&key).arg(60).arg("value").query::<()>(conn)
    }
);

bench_command!(
    bench_expire,
    "EXPIRE Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("expire_key_{}", fastrand::u32(..1000000));
        redis::cmd("SET").arg(&key).arg("value").query::<()>(conn)?;
        redis::cmd("EXPIRE").arg(&key).arg(3600).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("expire_key_{}", fastrand::u32(..1000000));
        redis::cmd("SET").arg(&key).arg("value").query::<()>(conn)?;
        redis::cmd("EXPIRE").arg(&key).arg(3600).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_ttl,
    "TTL Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let key = format!("ttl_key:{}", i);
                let _ = redis::cmd("SET").arg(&key).arg("value").query::<()>(&mut conn);
                let _ = redis::cmd("EXPIRE").arg(&key).arg(3600).query::<()>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("ttl_key:{}", fastrand::u32(..100));
        redis::cmd("TTL").arg(&key).query::<i64>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("ttl_key:{}", fastrand::u32(..100));
        redis::cmd("TTL").arg(&key).query::<i64>(conn).map(|_| ())
    }
);

// Hash Operations Benchmarks
bench_command!(
    bench_hset,
    "HSET Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash_{}", fastrand::u32(..10000));
        let field = format!("field_{}", fastrand::u32(..100));
        let value = format!("value_{}", fastrand::u32(..1000));
        redis::cmd("HSET").arg(&key).arg(&field).arg(&value).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash_{}", fastrand::u32(..10000));
        let field = format!("field_{}", fastrand::u32(..100));
        let value = format!("value_{}", fastrand::u32(..1000));
        redis::cmd("HSET").arg(&key).arg(&field).arg(&value).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_hget,
    "HGET Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HGET").arg(&key).arg("field1").query::<Option<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HGET").arg(&key).arg("field1").query::<Option<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_hgetall,
    "HGETALL Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HGETALL").arg(&key).query::<Vec<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HGETALL").arg(&key).query::<Vec<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_hdel,
    "HDEL Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HDEL").arg(&key).arg("field1").query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HDEL").arg(&key).arg("field1").query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_hexists,
    "HEXISTS Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HEXISTS").arg(&key).arg("field1").query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HEXISTS").arg(&key).arg("field1").query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_hkeys,
    "HKEYS Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HKEYS").arg(&key).query::<Vec<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HKEYS").arg(&key).query::<Vec<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_hvals,
    "HVALS Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HVALS").arg(&key).query::<Vec<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HVALS").arg(&key).query::<Vec<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_hlen,
    "HLEN Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HLEN").arg(&key).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HLEN").arg(&key).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_hincrby,
    "HINCRBY Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..50 {
                let _ = redis::cmd("HSET").arg(format!("hash:{}", i)).arg("counter").arg("0").query::<()>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HINCRBY").arg(&key).arg("counter").arg(1).query::<i64>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("hash:{}", fastrand::u32(..50));
        redis::cmd("HINCRBY").arg(&key).arg("counter").arg(1).query::<i64>(conn).map(|_| ())
    }
);

// List Operations Benchmarks
bench_command!(
    bench_lpush,
    "LPUSH Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list_{}", fastrand::u32(..10000));
        let value = format!("item_{}", fastrand::u32(..1000));
        redis::cmd("LPUSH").arg(&key).arg(&value).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list_{}", fastrand::u32(..10000));
        let value = format!("item_{}", fastrand::u32(..1000));
        redis::cmd("LPUSH").arg(&key).arg(&value).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_rpush,
    "RPUSH Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list_{}", fastrand::u32(..10000));
        let value = format!("item_{}", fastrand::u32(..1000));
        redis::cmd("RPUSH").arg(&key).arg(&value).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list_{}", fastrand::u32(..10000));
        let value = format!("item_{}", fastrand::u32(..1000));
        redis::cmd("RPUSH").arg(&key).arg(&value).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_lpop,
    "LPOP Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list:{}", fastrand::u32(..50));
        redis::cmd("LPOP").arg(&key).query::<Option<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list:{}", fastrand::u32(..50));
        redis::cmd("LPOP").arg(&key).query::<Option<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_llen,
    "LLEN Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list:{}", fastrand::u32(..50));
        redis::cmd("LLEN").arg(&key).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list:{}", fastrand::u32(..50));
        redis::cmd("LLEN").arg(&key).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_rpop,
    "RPOP Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list:{}", fastrand::u32(..50));
        redis::cmd("RPOP").arg(&key).query::<Option<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list:{}", fastrand::u32(..50));
        redis::cmd("RPOP").arg(&key).query::<Option<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_lindex,
    "LINDEX Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list:{}", fastrand::u32(..50));
        redis::cmd("LINDEX").arg(&key).arg(0).query::<Option<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list:{}", fastrand::u32(..50));
        redis::cmd("LINDEX").arg(&key).arg(0).query::<Option<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_lrange,
    "LRANGE Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list:{}", fastrand::u32(..50));
        redis::cmd("LRANGE").arg(&key).arg(0).arg(-1).query::<Vec<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list:{}", fastrand::u32(..50));
        redis::cmd("LRANGE").arg(&key).arg(0).arg(-1).query::<Vec<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_ltrim,
    "LTRIM Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list:{}", fastrand::u32(..50));
        redis::cmd("LTRIM").arg(&key).arg(0).arg(1).query::<()>(conn)
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("list:{}", fastrand::u32(..50));
        redis::cmd("LTRIM").arg(&key).arg(0).arg(1).query::<()>(conn)
    }
);

// Set Operations Benchmarks
bench_command!(
    bench_sadd,
    "SADD Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set_{}", fastrand::u32(..10000));
        let member = format!("member_{}", fastrand::u32(..1000));
        redis::cmd("SADD").arg(&key).arg(&member).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set_{}", fastrand::u32(..10000));
        let member = format!("member_{}", fastrand::u32(..1000));
        redis::cmd("SADD").arg(&key).arg(&member).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_smembers,
    "SMEMBERS Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set:{}", fastrand::u32(..50));
        redis::cmd("SMEMBERS").arg(&key).query::<Vec<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set:{}", fastrand::u32(..50));
        redis::cmd("SMEMBERS").arg(&key).query::<Vec<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_srem,
    "SREM Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set:{}", fastrand::u32(..50));
        redis::cmd("SREM").arg(&key).arg("member1").query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set:{}", fastrand::u32(..50));
        redis::cmd("SREM").arg(&key).arg("member1").query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_sismember,
    "SISMEMBER Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set:{}", fastrand::u32(..50));
        redis::cmd("SISMEMBER").arg(&key).arg("member1").query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set:{}", fastrand::u32(..50));
        redis::cmd("SISMEMBER").arg(&key).arg("member1").query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_scard,
    "SCARD Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set:{}", fastrand::u32(..50));
        redis::cmd("SCARD").arg(&key).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set:{}", fastrand::u32(..50));
        redis::cmd("SCARD").arg(&key).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_spop,
    "SPOP Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set:{}", fastrand::u32(..50));
        redis::cmd("SPOP").arg(&key).query::<Option<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set:{}", fastrand::u32(..50));
        redis::cmd("SPOP").arg(&key).query::<Option<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_srandmember,
    "SRANDMEMBER Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set:{}", fastrand::u32(..50));
        redis::cmd("SRANDMEMBER").arg(&key).query::<Option<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("set:{}", fastrand::u32(..50));
        redis::cmd("SRANDMEMBER").arg(&key).query::<Option<String>>(conn).map(|_| ())
    }
);

// Sorted Set Operations Benchmarks
bench_command!(
    bench_zadd,
    "ZADD Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset_{}", fastrand::u32(..10000));
        let score = fastrand::f64() * 1000.0;
        let member = format!("member_{}", fastrand::u32(..1000));
        redis::cmd("ZADD").arg(&key).arg(score).arg(&member).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset_{}", fastrand::u32(..10000));
        let score = fastrand::f64() * 1000.0;
        let member = format!("member_{}", fastrand::u32(..1000));
        redis::cmd("ZADD").arg(&key).arg(score).arg(&member).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_zrange,
    "ZRANGE Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZRANGE").arg(&key).arg(0).arg(-1).query::<Vec<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZRANGE").arg(&key).arg(0).arg(-1).query::<Vec<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_zrem,
    "ZREM Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZREM").arg(&key).arg("member").query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZREM").arg(&key).arg("member").query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_zscore,
    "ZSCORE Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZSCORE").arg(&key).arg("member").query::<Option<f64>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZSCORE").arg(&key).arg("member").query::<Option<f64>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_zcard,
    "ZCARD Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZCARD").arg(&key).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZCARD").arg(&key).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_zcount,
    "ZCOUNT Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZCOUNT").arg(&key).arg("-inf").arg("+inf").query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZCOUNT").arg(&key).arg("-inf").arg("+inf").query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_zrank,
    "ZRANK Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZRANK").arg(&key).arg("member").query::<Option<i32>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZRANK").arg(&key).arg("member").query::<Option<i32>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_zrevrange,
    "ZREVRANGE Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZREVRANGE").arg(&key).arg(0).arg(-1).query::<Vec<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZREVRANGE").arg(&key).arg(0).arg(-1).query::<Vec<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_zincrby,
    "ZINCRBY Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZINCRBY").arg(&key).arg(1.5).arg("member").query::<f64>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("zset:{}", fastrand::u32(..50));
        redis::cmd("ZINCRBY").arg(&key).arg(1.5).arg("member").query::<f64>(conn).map(|_| ())
    }
);

// Server Operations Benchmarks
bench_command!(
    bench_ping,
    "PING Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        redis::cmd("PING").query::<String>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        redis::cmd("PING").query::<String>(conn).map(|_| ())
    }
);

bench_command!(
    bench_info,
    "INFO Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        redis::cmd("INFO").query::<String>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        redis::cmd("INFO").query::<String>(conn).map(|_| ())
    }
);

bench_command!(
    bench_dbsize,
    "DBSIZE Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        redis::cmd("DBSIZE").query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        redis::cmd("DBSIZE").query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_keys,
    "KEYS Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        redis::cmd("KEYS").arg("key:*").query::<Vec<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        redis::cmd("KEYS").arg("key:*").query::<Vec<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_randomkey,
    "RANDOMKEY Operation",
    Some(populate_test_data),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        redis::cmd("RANDOMKEY").query::<Option<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        redis::cmd("RANDOMKEY").query::<Option<String>>(conn).map(|_| ())
    }
);

bench_command!(
    bench_rename,
    "RENAME Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let old_key = format!("rename_old_{}", fastrand::u32(..1000000));
        let new_key = format!("rename_new_{}", fastrand::u32(..1000000));
        redis::cmd("SET").arg(&old_key).arg("value").query::<()>(conn)?;
        redis::cmd("RENAME").arg(&old_key).arg(&new_key).query::<()>(conn)
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let old_key = format!("rename_old_{}", fastrand::u32(..1000000));
        let new_key = format!("rename_new_{}", fastrand::u32(..1000000));
        redis::cmd("SET").arg(&old_key).arg("value").query::<()>(conn)?;
        redis::cmd("RENAME").arg(&old_key).arg(&new_key).query::<()>(conn)
    }
);

bench_command!(
    bench_persist,
    "PERSIST Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let key = format!("persist_key:{}", i);
                let _ = redis::cmd("SET").arg(&key).arg("value").query::<()>(&mut conn);
                let _ = redis::cmd("EXPIRE").arg(&key).arg(3600).query::<()>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("persist_key:{}", fastrand::u32(..100));
        redis::cmd("PERSIST").arg(&key).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("persist_key:{}", fastrand::u32(..100));
        redis::cmd("PERSIST").arg(&key).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_pexpire,
    "PEXPIRE Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("pexpire_key_{}", fastrand::u32(..1000000));
        redis::cmd("SET").arg(&key).arg("value").query::<()>(conn)?;
        redis::cmd("PEXPIRE").arg(&key).arg(60000).query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("pexpire_key_{}", fastrand::u32(..1000000));
        redis::cmd("SET").arg(&key).arg("value").query::<()>(conn)?;
        redis::cmd("PEXPIRE").arg(&key).arg(60000).query::<i32>(conn).map(|_| ())
    }
);

bench_command!(
    bench_pttl,
    "PTTL Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let key = format!("pttl_key:{}", i);
                let _ = redis::cmd("SET").arg(&key).arg("value").query::<()>(&mut conn);
                let _ = redis::cmd("PEXPIRE").arg(&key).arg(3600000).query::<()>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("pttl_key:{}", fastrand::u32(..100));
        redis::cmd("PTTL").arg(&key).query::<i64>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("pttl_key:{}", fastrand::u32(..100));
        redis::cmd("PTTL").arg(&key).query::<i64>(conn).map(|_| ())
    }
);

// JSON Operations Benchmarks
bench_json_command!(
    bench_json_set,
    "JSON.SET Operation",
    None::<fn(&Client)>,
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_key_{}", fastrand::u32(..10000));
        let json_value = format!(r#"{{"id": {}, "name": "test", "active": true}}"#, fastrand::u32(..1000));
        redis::cmd("JSON.SET").arg(&key).arg("$").arg(&json_value).query::<String>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_key_{}", fastrand::u32(..10000));
        let json_value = format!(r#"{{"id": {}, "name": "test", "active": true}}"#, fastrand::u32(..1000));
        redis::cmd("JSON.SET").arg(&key).arg("$").arg(&json_value).query::<String>(conn).map(|_| ())
    }
);

bench_json_command!(
    bench_json_get,
    "JSON.GET Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let key = format!("json_data:{}", i);
                let json_value = format!(r#"{{"id": {}, "name": "test{}", "active": true}}"#, i, i);
                let _ = redis::cmd("JSON.SET").arg(&key).arg("$").arg(&json_value).query::<String>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_data:{}", fastrand::u32(..100));
        redis::cmd("JSON.GET").arg(&key).query::<String>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_data:{}", fastrand::u32(..100));
        redis::cmd("JSON.GET").arg(&key).query::<String>(conn).map(|_| ())
    }
);

bench_json_command!(
    bench_json_del,
    "JSON.DEL Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let key = format!("json_del_data:{}", i);
                let json_value = format!(r#"{{"id": {}, "name": "test{}", "active": true}}"#, i, i);
                let _ = redis::cmd("JSON.SET").arg(&key).arg("$").arg(&json_value).query::<String>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_del_data:{}", fastrand::u32(..100));
        redis::cmd("JSON.DEL").arg(&key).arg("$.name").query::<i32>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_del_data:{}", fastrand::u32(..100));
        redis::cmd("JSON.DEL").arg(&key).arg("$.name").query::<i32>(conn).map(|_| ())
    }
);

bench_json_command!(
    bench_json_type,
    "JSON.TYPE Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let key = format!("json_type_data:{}", i);
                let json_value = format!(r#"{{"id": {}, "name": "test{}", "active": true}}"#, i, i);
                let _ = redis::cmd("JSON.SET").arg(&key).arg("$").arg(&json_value).query::<String>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_type_data:{}", fastrand::u32(..100));
        redis::cmd("JSON.TYPE").arg(&key).arg("$.id").query::<Vec<String>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_type_data:{}", fastrand::u32(..100));
        redis::cmd("JSON.TYPE").arg(&key).arg("$.id").query::<Vec<String>>(conn).map(|_| ())
    }
);

bench_json_command!(
    bench_json_strlen,
    "JSON.STRLEN Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let key = format!("json_strlen_data:{}", i);
                let json_value = format!(r#"{{"id": {}, "name": "test{}", "active": true}}"#, i, i);
                let _ = redis::cmd("JSON.SET").arg(&key).arg("$").arg(&json_value).query::<String>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_strlen_data:{}", fastrand::u32(..100));
        redis::cmd("JSON.STRLEN").arg(&key).arg("$.name").query::<Vec<Option<i64>>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_strlen_data:{}", fastrand::u32(..100));
        redis::cmd("JSON.STRLEN").arg(&key).arg("$.name").query::<Vec<Option<i64>>>(conn).map(|_| ())
    }
);

bench_json_command!(
    bench_json_arrlen,
    "JSON.ARRLEN Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let key = format!("json_arrlen_data:{}", i);
                let json_value = format!(r#"{{"id": {}, "items": [1, 2, 3, 4]}}"#, i);
                let _ = redis::cmd("JSON.SET").arg(&key).arg("$").arg(&json_value).query::<String>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_arrlen_data:{}", fastrand::u32(..100));
        redis::cmd("JSON.ARRLEN").arg(&key).arg("$.items").query::<Vec<Option<i64>>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_arrlen_data:{}", fastrand::u32(..100));
        redis::cmd("JSON.ARRLEN").arg(&key).arg("$.items").query::<Vec<Option<i64>>>(conn).map(|_| ())
    }
);

bench_json_command!(
    bench_json_objlen,
    "JSON.OBJLEN Operation",
    Some(|client: &Client| {
        if let Ok(mut conn) = get_connection_with_retry(client, 3) {
            for i in 0..100 {
                let key = format!("json_objlen_data:{}", i);
                let json_value = format!(r#"{{"id": {}, "meta": {{"created": true, "updated": false}}}}"#, i);
                let _ = redis::cmd("JSON.SET").arg(&key).arg("$").arg(&json_value).query::<String>(&mut conn);
            }
        }
    }),
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_objlen_data:{}", fastrand::u32(..100));
        redis::cmd("JSON.OBJLEN").arg(&key).arg("$.meta").query::<Vec<Option<i64>>>(conn).map(|_| ())
    },
    |conn: &mut Connection| -> Result<(), redis::RedisError> {
        let key = format!("json_objlen_data:{}", fastrand::u32(..100));
        redis::cmd("JSON.OBJLEN").arg(&key).arg("$.meta").query::<Vec<Option<i64>>>(conn).map(|_| ())
    }
);

criterion_group!(
    string_operations,
    bench_set,
    bench_get,
    bench_mget,
    bench_mset,
    bench_incr,
    bench_del,
    bench_expire,
    bench_ttl,
    bench_exists,
    bench_type,
    bench_append,
    bench_strlen,
    bench_decr,
    bench_incrby,
    bench_decrby,
    bench_setnx,
    bench_setex
);

criterion_group!(
    hash_operations,
    bench_hset,
    bench_hget,
    bench_hgetall,
    bench_hdel,
    bench_hexists,
    bench_hkeys,
    bench_hvals,
    bench_hlen,
    bench_hincrby
);

criterion_group!(
    list_operations,
    bench_lpush,
    bench_rpush,
    bench_lpop,
    bench_llen,
    bench_rpop,
    bench_lindex,
    bench_lrange,
    bench_ltrim
);

criterion_group!(
    set_operations,
    bench_sadd,
    bench_smembers,
    bench_srem,
    bench_sismember,
    bench_scard,
    bench_spop,
    bench_srandmember
);

criterion_group!(
    sorted_set_operations,
    bench_zadd,
    bench_zrange,
    bench_zrem,
    bench_zscore,
    bench_zcard,
    bench_zcount,
    bench_zrank,
    bench_zrevrange,
    bench_zincrby
);

criterion_group!(
    server_operations,
    bench_ping,
    bench_info,
    bench_dbsize,
    bench_keys,
    bench_randomkey,
    bench_rename,
    bench_persist,
    bench_pexpire,
    bench_pttl
);

criterion_group!(
    json_operations,
    bench_json_set,
    bench_json_get,
    bench_json_del,
    bench_json_type,
    bench_json_strlen,
    bench_json_arrlen,
    bench_json_objlen
);

criterion_main!(
    string_operations,
    hash_operations,
    list_operations,
    set_operations,
    sorted_set_operations,
    server_operations
    // json_operations  // Disabled for now due to Redis Stack setup issues
);