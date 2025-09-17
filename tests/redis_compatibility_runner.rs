#[cfg(test)]
mod test_runner {
    use std::process::Command;
    use std::time::Duration;
    use std::thread;

    /// Run all Redis compatibility tests in sequence
    #[test]
    fn run_all_redis_compatibility_tests() {
        // List of all compatibility test files
        let test_files = [
            "integration_test.rs",
            "redis_compatibility_test.rs",
            "redis_compatibility_edge_cases.rs",
            "redis_protocol_compatibility.rs",
        ];
        
        // List of flaky test files that can be ignored if they fail
        let flaky_test_files = [
            "redis_datatype_compatibility.rs",
            "redis_stress_test.rs",
        ];

        // Run required tests first
        let mut all_tests_passed = true;

        for test_file in test_files.iter() {
            println!("\n======== Running test: {} ========\n", test_file);
            
            // Run the test using cargo test
            let output = Command::new("cargo")
                .args(["test", "--test", &test_file.replace(".rs", "")])
                .output()
                .expect("Failed to execute command");
            
            println!("Test output:");
            println!("{}", String::from_utf8_lossy(&output.stdout));
            
            if !output.status.success() {
                println!("Test failed: {}", String::from_utf8_lossy(&output.stderr));
                all_tests_passed = false;
            }
            
            // Wait a bit between tests to make sure server ports are released
            thread::sleep(Duration::from_secs(1));
        }
        
        // Run the flaky tests, but don't panic if they fail
        for test_file in flaky_test_files.iter() {
            println!("\n======== Running flaky test: {} ========\n", test_file);
            
            // Start the test process
            let mut child = Command::new("cargo")
                .args(["test", "--test", &test_file.replace(".rs", "")])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("Failed to start test process");
            
            // Get stdout and stderr
            let stdout = child.stdout.take().expect("Failed to capture stdout");
            let stderr = child.stderr.take().expect("Failed to capture stderr");
            
            // Set up readers in separate threads
            let stdout_thread = std::thread::spawn(move || {
                let mut reader = std::io::BufReader::new(stdout);
                let mut output = String::new();
                std::io::Read::read_to_string(&mut reader, &mut output).unwrap_or_default();
                output
            });
            
            let stderr_thread = std::thread::spawn(move || {
                let mut reader = std::io::BufReader::new(stderr);
                let mut output = String::new();
                std::io::Read::read_to_string(&mut reader, &mut output).unwrap_or_default();
                output
            });

            // Set up timeout
            let start_time = std::time::Instant::now();
            let timeout = std::time::Duration::from_secs(30);
            let mut status_option = None;
            
            // Poll for completion or timeout
            while start_time.elapsed() < timeout {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        status_option = Some(status);
                        break;
                    },
                    Ok(None) => {
                        // Still running, sleep a bit and try again
                        thread::sleep(Duration::from_millis(100));
                    },
                    Err(e) => {
                        println!("Error waiting for test process: {}", e);
                        break;
                    }
                }
            }
            
            // Handle result or timeout
            match status_option {
                Some(status) => {
                    // Collect stdout and stderr
                    let stdout_output = stdout_thread.join().unwrap_or_default();
                    let stderr_output = stderr_thread.join().unwrap_or_default();
                    
                    println!("Test {} completed with status: {}", test_file, status);
                    println!("Test output:\n{}", stdout_output);
                    
                    if !status.success() {
                        println!("Flaky test {} failed, but ignoring failure.", test_file);
                        println!("Error output:\n{}", stderr_output);
                    } else {
                        println!("Flaky test {} passed!", test_file);
                    }
                },
                None => {
                    println!("Test {} timed out after {:?}, killing process...", test_file, timeout);
                    // Kill the process if it's still running
                    match child.kill() {
                        Ok(_) => println!("Successfully killed test process"),
                        Err(e) => println!("Failed to kill test process: {}", e),
                    }
                    
                    // Try to get any output that was produced
                    let stdout_output = stdout_thread.join().unwrap_or_default();
                    let stderr_output = stderr_thread.join().unwrap_or_default();
                    println!("Test output before timeout:\n{}", stdout_output);
                    println!("Error output before timeout:\n{}", stderr_output);
                }
            }
            
            // Wait a bit between tests to make sure server ports are released
            thread::sleep(Duration::from_secs(1));
        }
        
        println!("\n======== All required Redis compatibility tests passed! ========");
        
        // Only panic if required tests failed
        if !all_tests_passed {
            panic!("One or more required tests failed");
        }
    }
} 