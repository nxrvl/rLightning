use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Semaphore;

use rlightning::storage::engine::{StorageConfig, StorageEngine};
use rlightning::command::Command;
use rlightning::command::handler::CommandHandler;

// Helper macro to create runtime once and use it consistently
macro_rules! async_bench {
    ($b:expr, $async_block:expr) => {{
        let rt = Arc::new(Runtime::new().unwrap());
        $b.iter(|| {
            let rt = Arc::clone(&rt);
            rt.handle().block_on($async_block)
        })
    }};
}

macro_rules! async_bench_with_setup {
    ($b:expr, $setup:expr, $bench_fn:expr) => {{
        let rt = Arc::new(Runtime::new().unwrap());
        $b.iter_with_setup(
            || {
                let rt = Arc::clone(&rt);
                rt.handle().block_on($setup)
            },
            |setup_result| {
                let rt = Arc::clone(&rt);
                rt.handle().block_on($bench_fn(setup_result))
            }
        )
    }};
}

fn bench_auth_command_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("Authentication - Command Overhead");
    
    // Optimize benchmark settings
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(15));
    
    for &auth_enabled in &[false, true] {
        let auth_label = if auth_enabled { "with_auth" } else { "without_auth" };
        
        group.bench_with_input(BenchmarkId::new("SET command", auth_label), &auth_enabled, |b, &auth_enabled| {
            async_bench_with_setup!(
                b,
                async {
                    let storage_config = StorageConfig::default();
                    let storage = StorageEngine::new(storage_config);
                    let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));
                    
                    // Simulate auth setup if enabled
                    if auth_enabled {
                        // In a real implementation, this would set up authentication
                        // For now, we simulate the overhead
                        storage.set(b"auth:enabled".to_vec(), b"true".to_vec(), None).await.unwrap();
                    }
                    
                    handler
                },
                |handler: Arc<CommandHandler>| async move {
                    let key = format!("auth_key_{}", fastrand::u32(..10000)).into_bytes();
                    let value = format!("auth_value_{}", fastrand::u32(..10000)).into_bytes();
                    
                    // Simulate auth check overhead if enabled
                    if auth_enabled {
                        // In a real implementation, this would check authentication via command
                        let auth_cmd = Command {
                            name: "exists".to_string(),
                            args: vec![b"auth:enabled".to_vec()],
                        };
                        let _auth_check = handler.process(auth_cmd).await.unwrap();
                    }
                    
                    let set_cmd = Command {
                        name: "set".to_string(),
                        args: vec![key, value],
                    };
                    
                    black_box(handler.process(set_cmd).await.unwrap());
                }
            );
        });
    }
    
    group.finish();
}

fn bench_auth_session_management(c: &mut Criterion) {
    let mut group = c.benchmark_group("Authentication - Session Management");
    
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(15));
    
    group.bench_function("Session creation", |b| {
        async_bench!(b, async {
            let storage_config = StorageConfig::default();
            let storage = StorageEngine::new(storage_config);
            
            // Simulate session creation
            let session_id = format!("session_{}", fastrand::u64(..));
            let session_data = format!("{{\"user\":\"user_{}\",\"created\":{}}}", 
                                     fastrand::u32(..), 
                                     std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());
            
            storage.set(
                black_box(format!("session:{}", session_id).into_bytes()),
                black_box(session_data.into_bytes()),
                Some(std::time::Duration::from_secs(3600)), // 1 hour TTL
            ).await.unwrap();
        });
    });
    
    group.bench_function("Session validation", |b| {
        let rt = Runtime::new().unwrap();
        let storage = rt.block_on(async {
            let storage_config = StorageConfig::default();
            StorageEngine::new(storage_config)
        });
        
        // Pre-create sessions
        rt.block_on(async {
            for i in 0..500 { // Reduced from 1000
                let session_id = format!("session_{}", i);
                let session_data = format!("{{\"user\":\"user_{}\",\"created\":{}}}", i, 1600000000);
                storage.set(
                    format!("session:{}", session_id).into_bytes(),
                    session_data.into_bytes(),
                    Some(std::time::Duration::from_secs(3600)),
                ).await.unwrap();
            }
        });
        
        async_bench!(b, async {
            let session_id = format!("session_{}", fastrand::u32(..500));
            let session_key = format!("session:{}", session_id);
            
            // Validate session exists and is not expired
            let _session_data = storage.get(black_box(session_key.as_bytes())).await.unwrap();
        });
    });
    
    group.bench_function("Session cleanup", |b| {
        async_bench!(b, async {
            let storage_config = StorageConfig::default();
            let storage = StorageEngine::new(storage_config);
            
            // Create expired sessions
            for i in 0..20 { // Reduced iterations
                let session_id = format!("expired_session_{}", i);
                let session_data = format!("{{\"user\":\"user_{}\",\"created\":0}}", i);
                storage.set(
                    format!("session:{}", session_id).into_bytes(),
                    session_data.into_bytes(),
                    Some(std::time::Duration::from_millis(1)), // Very short TTL
                ).await.unwrap();
            }
            
            // Wait for expiration
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            
            // Simulate cleanup by checking all session keys
            let all_keys = storage.all_keys().await.unwrap();
            let session_keys: Vec<_> = all_keys.iter()
                .filter(|key| key.starts_with(b"session:"))
                .collect();
            
            black_box(session_keys.len());
        });
    });
    
    group.finish();
}

fn bench_auth_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("Authentication - Concurrent Operations");
    
    // Optimize settings to avoid warnings
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(30));
    
    for concurrency in [10, 25].iter() { // Reduced concurrency levels
        group.bench_with_input(BenchmarkId::new("Concurrent auth checks", concurrency), concurrency, |b, &concurrency| {
            async_bench_with_setup!(
                b,
                async {
                    let storage_config = StorageConfig::default();
                    let storage = StorageEngine::new(storage_config);
                    let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));
                    
                    // Pre-create auth sessions
                    for i in 0..concurrency {
                        let session_id = format!("concurrent_session_{}", i);
                        let session_data = format!("{{\"user\":\"user_{}\",\"created\":{}}}", i, 1600000000);
                        storage.set(
                            format!("session:{}", session_id).into_bytes(),
                            session_data.into_bytes(),
                            Some(std::time::Duration::from_secs(3600)),
                        ).await.unwrap();
                    }
                    
                    handler
                },
                |handler: Arc<CommandHandler>| async move {
                    let semaphore = Arc::new(Semaphore::new(concurrency));
                    let mut handles = Vec::new();
                    
                    for i in 0..concurrency {
                        let handler_ref = Arc::clone(&handler);
                        let semaphore_ref = Arc::clone(&semaphore);
                        
                        let handle = tokio::spawn(async move {
                            let _permit = semaphore_ref.acquire().await.unwrap();
                            
                            // Simulate auth check
                            let session_id = format!("concurrent_session_{}", i);
                            let session_key = format!("session:{}", session_id);
                            let get_cmd = Command {
                                name: "get".to_string(),
                                args: vec![session_key.into_bytes()],
                            };
                            let _session_data = handler_ref.process(get_cmd).await.unwrap();
                            
                            // Execute authenticated command
                            let key = format!("auth_data_{}", i).into_bytes();
                            let value = format!("value_{}", i).into_bytes();
                            
                            let set_cmd = Command {
                                name: "set".to_string(),
                                args: vec![key, value],
                            };
                            
                            black_box(handler_ref.process(set_cmd).await.unwrap());
                        });
                        
                        handles.push(handle);
                    }
                    
                    for handle in handles {
                        handle.await.unwrap();
                    }
                }
            );
        });
    }
    
    group.finish();
}

fn bench_auth_password_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("Authentication - Password Operations");
    
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(10));
    
    group.bench_function("Password hash creation", |b| {
        b.iter(|| {
            let password = format!("password_{}", fastrand::u32(..));
            // Simulate password hashing (in a real implementation, this would use bcrypt or similar)
            let hash = format!("hash_{}", password.len() * 42); // Simplified simulation
            black_box(hash);
        });
    });
    
    group.bench_function("Password verification", |b| {
        // Pre-create password hashes
        let passwords: Vec<_> = (0..500).map(|i| { // Reduced from 1000
            let password = format!("password_{}", i);
            let hash = format!("hash_{}", password.len() * 42);
            (password, hash)
        }).collect();
        
        b.iter(|| {
            let (password, hash) = &passwords[fastrand::usize(..passwords.len())];
            // Simulate password verification
            let expected_hash = format!("hash_{}", password.len() * 42);
            let is_valid = hash == &expected_hash;
            black_box(is_valid);
        });
    });
    
    group.finish();
}

fn bench_auth_acl_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("Authentication - ACL Operations");
    
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(15));
    
    group.bench_function("Permission check", |b| {
        let rt = Runtime::new().unwrap();
        let storage = rt.block_on(async {
            let storage_config = StorageConfig::default();
            StorageEngine::new(storage_config)
        });
        
        // Pre-create ACL data
        rt.block_on(async {
            for i in 0..50 { // Reduced from 100
                let user_id = format!("user_{}", i);
                let permissions = format!("{{\"commands\":[\"GET\",\"SET\"],\"keys\":[\"user_{}:*\"]}}", i);
                storage.set(
                    format!("acl:user:{}", user_id).into_bytes(),
                    permissions.into_bytes(),
                    None,
                ).await.unwrap();
            }
        });
        
        async_bench!(b, async {
            let user_id = format!("user_{}", fastrand::u32(..50));
            let acl_key = format!("acl:user:{}", user_id);
            
            // Check user permissions
            let _permissions = storage.get(black_box(acl_key.as_bytes())).await.unwrap();
            
            // Simulate permission validation for SET command on user_X:data key
            let command = "SET";
            let key = format!("user_{}:data", user_id);
            
            // In a real implementation, this would parse permissions and validate
            let has_permission = command == "SET" && key.starts_with(&format!("user_{}:", user_id));
            black_box(has_permission);
        });
    });
    
    group.bench_function("ACL rule compilation", |b| {
        b.iter(|| {
            // Simulate ACL rule compilation
            let rules = vec![
                "user default on >password ~* &* +@all",
                "user alice on >alice123 ~alice:* +@read +@write -flushall",
                "user bob on >bob456 ~bob:* +@read +set +get",
            ];
            
            for rule in rules {
                // Simulate parsing and compiling ACL rule
                let parts: Vec<&str> = rule.split_whitespace().collect();
                let compiled = format!("compiled_{}", parts.len());
                black_box(compiled);
            }
        });
    });
    
    group.finish();
}

fn bench_auth_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("Authentication - Memory Usage");
    
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));
    
    for &user_count in &[50, 100, 200] { // Reduced user counts
        group.bench_with_input(BenchmarkId::new("Memory usage with users", user_count), &user_count, |b, &user_count| {
            async_bench!(b, async {
                let storage_config = StorageConfig::default();
                let storage = StorageEngine::new(storage_config);
                
                // Create users with sessions and ACLs
                for i in 0..user_count {
                    let user_id = format!("user_{}", i);
                    
                    // User session
                    let session_data = format!("{{\"user\":\"{}\",\"created\":{}}}", user_id, 1600000000);
                    storage.set(
                        format!("session:{}", user_id).into_bytes(),
                        session_data.into_bytes(),
                        Some(std::time::Duration::from_secs(3600)),
                    ).await.unwrap();
                    
                    // User ACL
                    let permissions = format!("{{\"commands\":[\"GET\",\"SET\"],\"keys\":[\"{}:*\"]}}", user_id);
                    storage.set(
                        format!("acl:user:{}", user_id).into_bytes(),
                        permissions.into_bytes(),
                        None,
                    ).await.unwrap();
                    
                    // User password hash
                    let password_hash = format!("hash_{}_{}", user_id, i * 42);
                    storage.set(
                        format!("auth:hash:{}", user_id).into_bytes(),
                        password_hash.into_bytes(),
                        None,
                    ).await.unwrap();
                }
                
                // Check total memory usage (simulated)
                let all_keys = storage.all_keys().await.unwrap();
                let auth_keys: Vec<_> = all_keys.iter()
                    .filter(|key| {
                        key.starts_with(b"session:") || 
                        key.starts_with(b"acl:") || 
                        key.starts_with(b"auth:")
                    })
                    .collect();
                
                black_box(auth_keys.len());
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    auth_overhead,
    bench_auth_command_overhead
);

criterion_group!(
    auth_sessions,
    bench_auth_session_management,
    bench_auth_concurrent_operations
);

criterion_group!(
    auth_security,
    bench_auth_password_operations,
    bench_auth_acl_operations
);

criterion_group!(
    auth_scalability,
    bench_auth_memory_usage
);

criterion_main!(
    auth_overhead,
    auth_sessions,
    auth_security,
    auth_scalability
);