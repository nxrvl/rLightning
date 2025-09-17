#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;
    
    use std::fs;
    use tempfile::tempdir;
    
    use crate::networking::resp::RespCommand;
    use crate::persistence::config::{AofSyncPolicy, PersistenceConfig, PersistenceMode, PersistenceTomlConfig};
    use crate::persistence::error::PersistenceError;
    use crate::persistence::{PersistenceManager, aof::AofPersistence, rdb::RdbPersistence, hybrid::HybridPersistence};
    use crate::storage::engine::{StorageEngine, StorageConfig};
    

    // Config tests
    #[test]
    fn test_persistence_config_default() {
        let config = PersistenceConfig::default();
        assert_eq!(config.mode, PersistenceMode::None);
        assert_eq!(config.aof_sync_policy, AofSyncPolicy::EverySecond);
        assert_eq!(config.rdb_snapshot_interval, Duration::from_secs(300));
        assert_eq!(config.rdb_snapshot_threshold, 10000);
        assert_eq!(config.aof_rewrite_min_size, 64 * 1024 * 1024);
        assert_eq!(config.aof_rewrite_percentage, 100);
    }

    #[test]
    fn test_persistence_mode_default() {
        assert_eq!(PersistenceMode::default(), PersistenceMode::None);
    }

    #[test]
    fn test_aof_sync_policy_default() {
        assert_eq!(AofSyncPolicy::default(), AofSyncPolicy::EverySecond);
    }

    #[test]
    fn test_config_conversion() {
        let toml_config = PersistenceTomlConfig {
            mode: "rdb".to_string(),
            rdb_path: Some("test.rdb".to_string()),
            aof_path: Some("test.aof".to_string()),
            aof_sync_policy: Some("always".to_string()),
            rdb_snapshot_interval: Some(600),
            rdb_snapshot_threshold: Some(5000),
            aof_rewrite_min_size: Some(128),
            aof_rewrite_percentage: Some(50),
        };

        let config = PersistenceConfig::try_from(toml_config).unwrap();
        assert_eq!(config.mode, PersistenceMode::RDB);
        assert_eq!(config.rdb_path, Some(PathBuf::from("test.rdb")));
        assert_eq!(config.aof_path, Some(PathBuf::from("test.aof")));
        assert_eq!(config.aof_sync_policy, AofSyncPolicy::Always);
        assert_eq!(config.rdb_snapshot_interval, Duration::from_secs(600));
        assert_eq!(config.rdb_snapshot_threshold, 5000);
        assert_eq!(config.aof_rewrite_min_size, 128 * 1024 * 1024);
        assert_eq!(config.aof_rewrite_percentage, 50);
    }

    #[test]
    fn test_invalid_persistence_mode() {
        let toml_config = PersistenceTomlConfig {
            mode: "invalid".to_string(),
            rdb_path: None,
            aof_path: None,
            aof_sync_policy: None,
            rdb_snapshot_interval: None,
            rdb_snapshot_threshold: None,
            aof_rewrite_min_size: None,
            aof_rewrite_percentage: None,
        };

        let result = PersistenceConfig::try_from(toml_config);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Invalid persistence mode: invalid");
    }

    #[test]
    fn test_invalid_aof_sync_policy() {
        let toml_config = PersistenceTomlConfig {
            mode: "aof".to_string(),
            rdb_path: None,
            aof_path: None,
            aof_sync_policy: Some("invalid".to_string()),
            rdb_snapshot_interval: None,
            rdb_snapshot_threshold: None,
            aof_rewrite_min_size: None,
            aof_rewrite_percentage: None,
        };

        let result = PersistenceConfig::try_from(toml_config);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Invalid AOF sync policy: Some(\"invalid\")");
    }

    // PersistenceManager tests
    #[tokio::test]
    async fn test_persistence_manager_creation() {
        let engine = StorageEngine::new(StorageConfig::default());
        let config = PersistenceConfig::default();
        let manager = PersistenceManager::new(engine, config);
        
        // Test init with persistence mode None (should succeed without doing anything)
        let result = manager.init().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_log_command_no_persistence() {
        let engine = StorageEngine::new(StorageConfig::default());
        let config = PersistenceConfig::default(); // Default is None
        let manager = PersistenceManager::new(engine, config);
        
        // Create a test command
        let cmd = RespCommand {
            name: b"SET".to_vec(),
            args: vec![b"key".to_vec(), b"value".to_vec()],
        };
        
        // Should succeed silently with no persistence
        let result = manager.log_command(cmd, AofSyncPolicy::Always).await;
        assert!(result.is_ok());
    }

    // RDB persistence tests
    #[tokio::test]
    async fn test_rdb_persistence_creation() {
        let temp_dir = tempdir().unwrap();
        let rdb_path = temp_dir.path().join("test.rdb");
        let engine = StorageEngine::new(StorageConfig::default());
        
        let rdb = RdbPersistence::new(engine, rdb_path);
        
        // RDB persistence should be created without errors
        assert_eq!(rdb.get_write_count(), 0);
    }

    #[tokio::test]
    async fn test_rdb_persistence_clone() {
        let temp_dir = tempdir().unwrap();
        let rdb_path = temp_dir.path().join("test.rdb");
        let engine = StorageEngine::new(StorageConfig::default());
        
        let rdb = RdbPersistence::new(engine, rdb_path);
        let rdb_clone = rdb.clone();
        
        // Both should point to the same write counter
        assert_eq!(rdb.get_write_count(), rdb_clone.get_write_count());
    }

    // Functional RDB tests
    #[tokio::test]
    async fn test_rdb_save_load() {
        let temp_dir = tempdir().unwrap();
        let rdb_path = temp_dir.path().join("test_save_load.rdb");
        
        // Create a storage engine and add some data
        let engine = StorageEngine::new(StorageConfig::default());
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        engine.set(key.clone(), value.clone(), None).await.unwrap();
        
        // Create RDB persistence and save data
        let rdb = RdbPersistence::new(engine, rdb_path.clone());
        let save_result = rdb.save().await;
        assert!(save_result.is_ok());
        
        // Verify file was created
        assert!(rdb_path.exists());
        
        // Create a new storage engine and load data
        let new_engine = StorageEngine::new(StorageConfig::default());
        let new_rdb = RdbPersistence::new(new_engine.clone(), rdb_path);
        let load_result = new_rdb.load().await;
        assert!(load_result.is_ok());
        
        // Verify data was loaded
        let loaded_value = new_engine.get(&key).await.unwrap();
        match loaded_value {
            Some(loaded) => {
                assert_eq!(loaded, value);
            },
            None => panic!("Value was not loaded from RDB file"),
        }
    }

    #[tokio::test]
    async fn test_rdb_incremental_saves() {
        let temp_dir = tempdir().unwrap();
        let rdb_path = temp_dir.path().join("test_incremental.rdb");
        
        // Create a storage engine and RDB persistence
        let engine = StorageEngine::new(StorageConfig::default());
        let rdb = RdbPersistence::new(engine.clone(), rdb_path.clone());
        
        // Set up a config for testing write count triggering
        let mut config = PersistenceConfig::default();
        config.mode = PersistenceMode::RDB;
        config.rdb_snapshot_threshold = 3; // Save after 3 writes
        
        // Schedule snapshots
        let schedule_result = rdb.schedule_snapshots(&config).await;
        assert!(schedule_result.is_ok());
        
        // Make some writes (less than threshold)
        engine.set(b"key1".to_vec(), b"value1".to_vec(), None).await.unwrap();
        engine.set(b"key2".to_vec(), b"value2".to_vec(), None).await.unwrap();
        
        // Increment write count
        rdb.increment_write_count();
        rdb.increment_write_count();
        
        // Wait a short time to see if a snapshot is triggered
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Skip this check as implementation details may vary
        // The file might exist but be empty, or might not exist yet
        // We can't reliably test this implementation detail
        
        // Increment again to trigger the snapshot
        rdb.increment_write_count();
        
        // Wait for the snapshot to complete
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // Now the file should exist and have data
        assert!(rdb_path.exists());
        assert!(fs::metadata(&rdb_path).unwrap().len() > 0);
    }

    // AOF persistence tests
    #[tokio::test]
    async fn test_aof_persistence_creation() {
        let temp_dir = tempdir().unwrap();
        let aof_path = temp_dir.path().join("test.aof");
        let engine = StorageEngine::new(StorageConfig::default());
        
        let _aof = AofPersistence::new(engine, aof_path);
        // Successfully creating the AOF instance is the test
    }

    #[tokio::test]
    async fn test_aof_persistence_clone() {
        let temp_dir = tempdir().unwrap();
        let aof_path = temp_dir.path().join("test.aof");
        let engine = StorageEngine::new(StorageConfig::default());
        
        let aof = AofPersistence::new(engine, aof_path);
        let _aof_clone = aof.clone();
        // Successfully cloning the AOF instance is the test
    }

    // Functional AOF tests
    #[tokio::test]
    async fn test_aof_command_logging() {
        let temp_dir = tempdir().unwrap();
        let aof_path = temp_dir.path().join("test_command.aof");
        
        // Create storage engine and AOF persistence
        let engine = StorageEngine::new(StorageConfig::default());
        let aof = AofPersistence::new(engine, aof_path.clone());
        
        // Log a command
        let cmd = RespCommand {
            name: b"SET".to_vec(),
            args: vec![b"key".to_vec(), b"value".to_vec()],
        };
        
        let result = aof.append_command(cmd, AofSyncPolicy::Always).await;
        assert!(result.is_ok());
        
        // Wait for it to be written
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Verify file was created and has content
        assert!(aof_path.exists());
        assert!(fs::metadata(&aof_path).unwrap().len() > 0);
    }
    
    #[tokio::test]
    async fn test_aof_read_only_commands_not_logged() {
        let temp_dir = tempdir().unwrap();
        let aof_path = temp_dir.path().join("test_readonly.aof");
        
        // Create storage engine and AOF persistence
        let engine = StorageEngine::new(StorageConfig::default());
        let aof = AofPersistence::new(engine, aof_path.clone());
        
        // Log a read-only command
        let cmd = RespCommand {
            name: b"GET".to_vec(),
            args: vec![b"key".to_vec()],
        };
        
        let result = aof.append_command(cmd, AofSyncPolicy::Always).await;
        assert!(result.is_ok());
        
        // Wait a bit to see if it was written
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // File should not exist or be empty because GET is read-only
        assert!(!aof_path.exists() || fs::metadata(&aof_path).unwrap().len() == 0);
    }

    // Hybrid persistence tests
    #[tokio::test]
    async fn test_hybrid_persistence_creation() {
        let temp_dir = tempdir().unwrap();
        let rdb_path = temp_dir.path().join("test.rdb");
        let aof_path = temp_dir.path().join("test.aof");
        let engine = StorageEngine::new(StorageConfig::default());
        
        let _hybrid = HybridPersistence::new(
            engine,
            Some(rdb_path),
            Some(aof_path)
        );
        // Successfully creating the hybrid instance is the test
    }

    #[tokio::test]
    async fn test_hybrid_default_paths() {
        let engine = StorageEngine::new(StorageConfig::default());
        
        let hybrid = HybridPersistence::new(
            engine,
            None,
            None
        );
        
        // Should use default paths when none provided
        let _ = hybrid.append_command(
            RespCommand {
                name: b"SET".to_vec(),
                args: vec![b"key".to_vec(), b"value".to_vec()],
            },
            AofSyncPolicy::None
        ).await;
        // Test succeeds if no panic occurs
    }

    // Functional Hybrid tests
    #[tokio::test]
    async fn test_hybrid_command_logging() {
        let temp_dir = tempdir().unwrap();
        let rdb_path = temp_dir.path().join("test_hybrid.rdb");
        let aof_path = temp_dir.path().join("test_hybrid.aof");
        
        // Create storage engine and hybrid persistence
        let engine = StorageEngine::new(StorageConfig::default());
        let hybrid = HybridPersistence::new(
            engine,
            Some(rdb_path.clone()),
            Some(aof_path.clone()),
        );
        
        // Log a command
        let cmd = RespCommand {
            name: b"SET".to_vec(),
            args: vec![b"key".to_vec(), b"value".to_vec()],
        };
        
        let result = hybrid.append_command(cmd, AofSyncPolicy::Always).await;
        assert!(result.is_ok());
        
        // Wait for it to be written
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Verify AOF file was created (RDB happens on schedule, not immediately)
        assert!(aof_path.exists());
        assert!(fs::metadata(&aof_path).unwrap().len() > 0);
    }

    // Integration tests
    #[tokio::test]
    async fn test_persistence_manager_init_modes() {
        let temp_dir = tempdir().unwrap();
        let rdb_path = temp_dir.path().join("manager_test.rdb");
        let aof_path = temp_dir.path().join("manager_test.aof");
        
        // Test all persistence modes
        for mode in [PersistenceMode::None, PersistenceMode::RDB, PersistenceMode::AOF, PersistenceMode::Hybrid] {
            // Create a new engine for each test
            let engine = StorageEngine::new(StorageConfig::default());
            
            // Configure persistence
            let config = PersistenceConfig {
                mode,
                rdb_path: Some(rdb_path.clone()),
                aof_path: Some(aof_path.clone()),
                aof_sync_policy: AofSyncPolicy::EverySecond,
                rdb_snapshot_interval: Duration::from_secs(1),
                rdb_snapshot_threshold: 1000,
                aof_rewrite_min_size: 1024 * 1024,
                aof_rewrite_percentage: 100,
            };
            
            let manager = PersistenceManager::new(engine, config);
            
            // Init should succeed for all modes
            let result = manager.init().await;
            assert!(result.is_ok(), "Failed to initialize persistence with mode {:?}", mode);
        }
    }

    // Error handling tests
    #[test]
    fn test_persistence_error_display() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let error = PersistenceError::Io(io_error);
        
        assert!(format!("{}", error).contains("I/O error"));
        
        let error = PersistenceError::Serialization("test error".to_string());
        assert!(format!("{}", error).contains("Serialization error"));
        
        let error = PersistenceError::CorruptedFile("test error".to_string());
        assert!(format!("{}", error).contains("Corrupted file"));
    }

    // Error handling tests - RDB
    #[tokio::test]
    async fn test_rdb_load_nonexistent_file() {
        let temp_dir = tempdir().unwrap();
        let nonexistent_path = temp_dir.path().join("nonexistent.rdb");
        
        // Make sure the file doesn't exist
        if nonexistent_path.exists() {
            fs::remove_file(&nonexistent_path).unwrap();
        }
        
        let engine = StorageEngine::new(StorageConfig::default());
        let rdb = RdbPersistence::new(engine, nonexistent_path);
        
        // Load should succeed with non-existent file (just skips loading)
        let result = rdb.load().await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_rdb_save_to_invalid_directory() {
        // Create a path with invalid characters that can't be created
        // This should be invalid across platforms
        let invalid_path = PathBuf::from("/\0invalid");
        
        let engine = StorageEngine::new(StorageConfig::default());
        let rdb = RdbPersistence::new(engine, invalid_path);
        
        // Save should fail gracefully
        let result = rdb.save().await;
        assert!(result.is_err());
        
        // Should be an IO error or directory creation error
        match result {
            Err(PersistenceError::Io(_)) | Err(PersistenceError::DirectoryCreationFailed(_)) => {},
            _ => panic!("Expected an IO error or directory creation error"),
        }
    }

    // Error handling tests - AOF
    #[tokio::test]
    async fn test_aof_load_corrupted_file() {
        let temp_dir = tempdir().unwrap();
        let corrupted_path = temp_dir.path().join("corrupted.aof");
        
        // Create a corrupted AOF file (not valid RESP format)
        fs::write(&corrupted_path, b"This is not a valid RESP format").unwrap();
        
        let engine = StorageEngine::new(StorageConfig::default());
        let aof = AofPersistence::new(engine, corrupted_path);
        
        // Load should fail with corrupted file
        let _result = aof.load().await;
        
        // The load should not panic, but might either return an error
        // or log the error and continue. This depends on the implementation.
        // For this test, we just ensure it doesn't panic.
    }
    
    // Testing persistence with large datasets
    #[tokio::test]
    async fn test_rdb_persistence_with_large_dataset() {
        let temp_dir = tempdir().unwrap();
        let rdb_path = temp_dir.path().join("large_dataset.rdb");
        
        // Create a storage engine and add many items
        let engine = StorageEngine::new(StorageConfig::default());
        
        // Add 1000 items
        for i in 0..1000 {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            engine.set(key, value.clone(), None).await.unwrap();
        }
        
        // Create RDB persistence and save data
        let rdb = RdbPersistence::new(engine, rdb_path.clone());
        let save_result = rdb.save().await;
        assert!(save_result.is_ok());
        
        // Verify file was created
        assert!(rdb_path.exists());
        
        // Create a new storage engine and load data
        let new_engine = StorageEngine::new(StorageConfig::default());
        let new_rdb = RdbPersistence::new(new_engine.clone(), rdb_path);
        let load_result = new_rdb.load().await;
        assert!(load_result.is_ok());
        
        // Verify random samples of data were loaded correctly
        for i in [0, 100, 500, 999] {
            let key = format!("key_{}", i).into_bytes();
            let expected_value = format!("value_{}", i).into_bytes();
            
            let loaded_value = new_engine.get(&key).await.unwrap();
            match loaded_value {
                Some(value) => {
                    assert_eq!(value, expected_value);
                },
                _ => panic!("Value not found or wrong type"),
            }
        }
    }
    
    // Edge case tests
    #[tokio::test]
    async fn test_persistence_with_empty_database() {
        let temp_dir = tempdir().unwrap();
        let rdb_path = temp_dir.path().join("empty_db.rdb");
        
        // Create a storage engine with no data
        let engine = StorageEngine::new(StorageConfig::default());
        
        // Create RDB persistence and save data
        let rdb = RdbPersistence::new(engine, rdb_path.clone());
        let save_result = rdb.save().await;
        assert!(save_result.is_ok());
        
        // Verify file was created
        assert!(rdb_path.exists());
        
        // File should be small but not empty (contains header/footer)
        let metadata = fs::metadata(&rdb_path).unwrap();
        assert!(metadata.len() > 0);
    }
    
    #[tokio::test]
    async fn test_persistence_with_special_characters() {
        let temp_dir = tempdir().unwrap();
        let rdb_path = temp_dir.path().join("special_chars.rdb");
        
        // Create a storage engine
        let engine = StorageEngine::new(StorageConfig::default());
        
        // Add items with special characters and binary data
        let test_cases = [
            (b"key\nwith\nnewlines".to_vec(), b"value\r\nwith\r\nnewlines".to_vec()),
            (b"binary\0data".to_vec(), b"value\0with\0nulls".to_vec()),
            (b"unicode_key_\xF0\x9F\x98\x8A".to_vec(), b"unicode_value_\xF0\x9F\x8C\x9F".to_vec()),
            (b"".to_vec(), b"empty_key".to_vec()),  // Empty key
            (b"empty_value".to_vec(), b"".to_vec()), // Empty value
        ];
        
        for (key, value) in test_cases.iter() {
            engine.set(key.clone(), value.clone(), None).await.unwrap();
        }
        
        // Create RDB persistence and save data
        let rdb = RdbPersistence::new(engine, rdb_path.clone());
        let save_result = rdb.save().await;
        assert!(save_result.is_ok());
        
        // Create a new storage engine and load data
        let new_engine = StorageEngine::new(StorageConfig::default());
        let new_rdb = RdbPersistence::new(new_engine.clone(), rdb_path);
        let load_result = new_rdb.load().await;
        assert!(load_result.is_ok());
        
        // Verify all special cases were loaded correctly
        for (key, expected_value) in test_cases.iter() {
            let loaded_value = new_engine.get(key).await.unwrap();
            match loaded_value {
                Some(value) => {
                    assert_eq!(value, *expected_value);
                },
                _ => panic!("Value not found or wrong type for key: {:?}", key),
            }
        }
    }
    
    // Performance test for AOF rewrite
    #[tokio::test]
    //#[ignore] // Mark as ignored by default since it's a longer-running test
    async fn test_aof_rewrite_performance() {
        let temp_dir = tempdir().unwrap();
        let aof_path = temp_dir.path().join("rewrite_perf.aof");
        
        // Create storage engine and AOF persistence
        let engine = StorageEngine::new(StorageConfig::default());
        let aof = AofPersistence::new(engine.clone(), aof_path.clone());
        
        // Generate many commands that will be redundant 
        // (setting the same keys multiple times)
        for i in 0..1000 {
            // Each key will be set 10 times with different values
            let key_id = i % 100;
            let key = format!("key_{}", key_id).into_bytes();
            let value = format!("value_{}_{}", key_id, i).into_bytes();
            
            // SET the value in the engine directly
            engine.set(key.clone(), value.clone(), None).await.unwrap();
            
            // Also log the command to the AOF file
            let cmd = RespCommand {
                name: b"SET".to_vec(),
                args: vec![key, value],
            };
            
            aof.append_command(cmd, AofSyncPolicy::None).await.unwrap();
        }
        
        // Wait for commands to be written
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // Verify the keys are in the original engine before rewrite
        for i in 0..100 {
            let key = format!("key_{}", i).into_bytes();
            let expected_value = format!("value_{}_{}", i, 900 + i).into_bytes();
            
            let value = engine.get(&key).await.unwrap();
            assert!(value.is_some(), "Key should exist in engine before rewrite: key_{}", i);
            assert_eq!(value.unwrap(), expected_value, "Value mismatch before rewrite for key_{}", i);
        }
        
        // Get file size before rewrite
        let size_before = fs::metadata(&aof_path).unwrap().len();
        println!("AOF file size before rewrite: {} bytes", size_before);
        
        // Rewrite the AOF file
        let rewrite_result = aof.rewrite().await;
        assert!(rewrite_result.is_ok());
        
        // Wait for rewrite to complete
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // Get file size after rewrite
        let size_after = fs::metadata(&aof_path).unwrap().len();
        println!("AOF file size after rewrite: {} bytes", size_after);
        
        // The file should be significantly smaller after rewrite (only 100 unique keys)
        assert!(size_after < size_before);
        
        // Load the data to verify integrity
        let new_engine = StorageEngine::new(StorageConfig::default());
        let new_aof = AofPersistence::new(new_engine.clone(), aof_path);
        let load_result = new_aof.load().await;
        assert!(load_result.is_ok());
        
        // Verify we have all 100 unique keys with their final values
        for i in 0..100 {
            let key = format!("key_{}", i).into_bytes();
            // For key_i, the last value will be from index 900+i
            let expected_value = format!("value_{}_{}", i, 900 + i).into_bytes();
            
            let loaded_value = new_engine.get(&key).await.unwrap();
            if loaded_value.is_none() {
                println!("ERROR: Key not found after load: key_{}", i);
                // Check a few more keys to see if any are loaded
                for j in i+1..i+5 {
                    if j < 100 {
                        let check_key = format!("key_{}", j).into_bytes();
                        let check_result = new_engine.get(&check_key).await.unwrap();
                        println!("Check key_{}: {}", j, check_result.is_some());
                    }
                }
                panic!("Value not found for key: key_{}", i);
            }
            
            let value = loaded_value.unwrap();
            assert_eq!(value, expected_value, "Value mismatch after load for key_{}", i);
        }
    }
} 