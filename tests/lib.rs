// Test utilities module shared by all tests
pub mod test_utils;

// Test modules
mod integration;
mod list_operations_test;
mod redis_commands_spec;
mod redis_compatibility_edge_cases;
mod redis_compatibility_runner;
mod redis_compatibility_test;
mod redis_datatype_compatibility;
mod redis_protocol_compatibility;
mod redis_stress_test;

// Datetime handling tests
mod datetime_utils_test;
mod json_datetime_test;

// Authentication tests
pub mod auth_integration_test;

// Large data handling tests
#[path = "large_json_integration_test.rs"]
mod large_json_integration_test;
mod large_json_test;

// Full Redis compatibility test suite (Task 21)
// Note: redis_full_compatibility_test, redis_cluster_compat_test, and
// redis_sentinel_compat_test run as standalone test binaries (not via lib.rs)
// because they use `mod test_utils;` directly for standalone execution.
