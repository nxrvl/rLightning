/// Redis Compatibility Runner
///
/// Validates that all required compatibility test files exist in the test suite.
/// The actual tests are run directly by cargo test (not via subprocess invocation).
#[cfg(test)]
mod test_runner {
    use std::path::Path;

    /// Verify that all required compatibility test files exist
    #[test]
    fn verify_compatibility_test_files_exist() {
        let test_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");

        let required_test_files = [
            "integration_test.rs",
            "redis_compatibility_test.rs",
            "redis_compatibility_edge_cases.rs",
            "redis_protocol_compatibility.rs",
            "redis_full_compatibility_test.rs",
            "acceptance_test.rs",
        ];

        let mut missing_files = Vec::new();

        for test_file in &required_test_files {
            let path = test_dir.join(test_file);
            if !path.exists() {
                missing_files.push(test_file.to_string());
            }
        }

        assert!(
            missing_files.is_empty(),
            "Missing required compatibility test files: {:?}",
            missing_files
        );
    }

    /// Verify that the acceptance test file covers all command categories
    #[test]
    fn verify_acceptance_test_coverage() {
        let acceptance_test = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/acceptance_test.rs"),
        )
        .expect("Failed to read acceptance_test.rs");

        let required_categories = [
            "acceptance_test_string_commands",
            "acceptance_test_list_commands",
            "acceptance_test_set_commands",
            "acceptance_test_hash_commands",
            "acceptance_test_sorted_set_commands",
            "acceptance_test_stream_commands",
            "acceptance_test_bitmap_commands",
            "acceptance_test_hyperloglog_commands",
            "acceptance_test_geo_commands",
            "acceptance_test_transaction_commands",
            "acceptance_test_scripting_commands",
            "acceptance_test_acl_commands",
            "acceptance_test_pubsub_commands",
            "acceptance_test_scenario_session_store",
            "acceptance_test_scenario_cache",
            "acceptance_test_scenario_rate_limiter",
            "acceptance_test_scenario_leaderboard",
            "acceptance_test_scenario_message_queue",
        ];

        let mut missing_tests = Vec::new();

        for test_name in &required_categories {
            if !acceptance_test.contains(test_name) {
                missing_tests.push(test_name.to_string());
            }
        }

        assert!(
            missing_tests.is_empty(),
            "Missing acceptance test functions: {:?}",
            missing_tests
        );
    }
}
