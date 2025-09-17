// Test utilities module shared by all tests
pub mod test_utils;

// Test modules
mod integration;
mod redis_compatibility_edge_cases;
mod redis_compatibility_test;
mod redis_datatype_compatibility;
mod redis_protocol_compatibility;
mod redis_commands_spec;
mod redis_stress_test;
mod list_operations_test;
mod redis_compatibility_runner;

// Datetime handling tests
mod datetime_utils_test;
mod json_datetime_test;

// Authentication tests
pub mod auth_integration_test;

// Large data handling tests
mod large_json_test;
#[path = "large_json_integration_test.rs"]
mod large_json_integration_test;