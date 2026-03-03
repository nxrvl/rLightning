use chrono::{Datelike, Timelike};
use rlightning::utils::datetime::DateTimeFormat;
use serde_json::{Value, json};

#[test]
fn test_process_json_for_serialization() {
    // Test datetime conversion in a simple string
    let datetime_str = "2023-04-01 12:34:56.789";
    let json_value = json!(datetime_str);

    // The updated function should now convert all datetime-looking strings to ISO-8601
    let processed = rlightning::utils::datetime::process_json_for_serialization(json_value);
    assert_eq!(processed, json!("2023-04-01T12:34:56.789"));

    // Test with Python "at" format
    let datetime_with_at = "2023-04-01 at 12:34:56";
    let json_value = json!(datetime_with_at);

    // This should be converted to ISO-8601
    let processed = rlightning::utils::datetime::process_json_for_serialization(json_value);
    let expected = json!("2023-04-01T12:34:56");
    assert_eq!(processed, expected);

    // Test with an object containing datetime fields
    let obj = json!({
        "name": "Test",
        "created_at": "2023-04-01 at 12:34:56",
        "updated_at": "2023-04-02 at 10:20:30",
        "metadata": {
            "timestamp": "2023-04-03 at 15:45:12"
        }
    });

    let processed = rlightning::utils::datetime::process_json_for_serialization(obj);

    // Check that nested datetimes were converted
    if let Value::Object(map) = &processed {
        assert_eq!(
            map.get("created_at").unwrap(),
            &json!("2023-04-01T12:34:56")
        );
        assert_eq!(
            map.get("updated_at").unwrap(),
            &json!("2023-04-02T10:20:30")
        );

        if let Value::Object(metadata) = map.get("metadata").unwrap() {
            assert_eq!(
                metadata.get("timestamp").unwrap(),
                &json!("2023-04-03T15:45:12")
            );
        } else {
            panic!("metadata is not an object");
        }
    } else {
        panic!("processed value is not an object");
    }

    // Test with an array containing datetimes
    let arr = json!([
        "2023-04-01 at 12:34:56",
        "regular string",
        ["2023-04-02 at 10:20:30"]
    ]);

    let processed = rlightning::utils::datetime::process_json_for_serialization(arr);

    // Check that array items were converted correctly
    if let Value::Array(items) = &processed {
        assert_eq!(items[0], json!("2023-04-01T12:34:56"));
        assert_eq!(items[1], json!("regular string"));

        if let Value::Array(nested) = &items[2] {
            assert_eq!(nested[0], json!("2023-04-02T10:20:30"));
        } else {
            panic!("nested item is not an array");
        }
    } else {
        panic!("processed value is not an array");
    }
}

#[test]
fn test_serialize_with_datetime() {
    // Test serializing a struct with datetime fields
    let data = json!({
        "user": {
            "name": "John Doe",
            "registered": "2023-04-01 at 12:34:56"
        },
        "posts": [
            {
                "title": "First Post",
                "created": "2023-04-02 at 10:20:30"
            }
        ]
    });

    let serialized = rlightning::utils::datetime::serialize_with_datetime(&data)
        .expect("Failed to serialize data");

    // The serialized string should contain the ISO-8601 formatted dates
    assert!(serialized.contains("2023-04-01T12:34:56"));
    assert!(serialized.contains("2023-04-02T10:20:30"));

    // It should be valid JSON
    let parsed: Value = serde_json::from_str(&serialized).expect("Failed to parse serialized JSON");

    // Verify the structure was preserved
    assert!(parsed.is_object());
    assert!(parsed["user"].is_object());
    assert!(parsed["posts"].is_array());
}

#[test]
fn test_convert_to_iso8601() {
    // Test regular datetime conversion
    let datetime = "2023-04-01 12:34:56.789";
    let iso8601 = rlightning::utils::datetime::convert_to_iso8601(datetime)
        .expect("Failed to convert datetime");
    assert_eq!(iso8601, "2023-04-01T12:34:56.789");

    // Test with different date formats
    let datetime = "2023/04/01 12:34:56";
    let iso8601 = rlightning::utils::datetime::convert_to_iso8601(datetime)
        .expect("Failed to convert datetime");
    assert_eq!(iso8601, "2023-04-01T12:34:56");

    // Test with Python "at" format
    let datetime = "2023-04-01 at 12:34:56";
    let iso8601 = rlightning::utils::datetime::convert_to_iso8601(datetime)
        .expect("Failed to convert datetime");
    assert_eq!(iso8601, "2023-04-01T12:34:56");

    // Test with already ISO-8601 format (should keep as is)
    let datetime = "2023-04-01T12:34:56.789";
    let iso8601 = rlightning::utils::datetime::convert_to_iso8601(datetime)
        .expect("Failed to convert datetime");
    assert_eq!(iso8601, "2023-04-01T12:34:56.789");

    // Test with non-datetime string (should return None)
    let not_datetime = "This is not a datetime";
    let result = rlightning::utils::datetime::convert_to_iso8601(not_datetime);
    assert!(result.is_none());
}

#[test]
fn test_convert_from_iso8601() {
    // Test bidirectional conversion from ISO-8601 to different formats
    let iso8601 = "2023-04-01T12:34:56.789";

    // To space-separated format
    let space_fmt =
        rlightning::utils::datetime::convert_from_iso8601(iso8601, DateTimeFormat::SpaceSeparated)
            .expect("Failed to convert from ISO-8601");
    assert_eq!(space_fmt, "2023-04-01 12:34:56.789");

    // To Python "at" format
    let python_fmt =
        rlightning::utils::datetime::convert_from_iso8601(iso8601, DateTimeFormat::PythonAt)
            .expect("Failed to convert from ISO-8601");
    assert_eq!(python_fmt, "2023-04-01 at 12:34:56.789");

    // Back to ISO-8601 (should be identical)
    let back_to_iso =
        rlightning::utils::datetime::convert_from_iso8601(iso8601, DateTimeFormat::ISO8601)
            .expect("Failed to convert from ISO-8601");
    assert_eq!(back_to_iso, iso8601);
}

#[test]
fn test_parse_iso8601() {
    // Test parsing ISO-8601 strings to DateTime objects
    let iso8601 = "2023-04-01T12:34:56.789Z";
    let dt = rlightning::utils::datetime::parse_iso8601(iso8601)
        .expect("Failed to parse ISO-8601 string");

    assert_eq!(dt.year(), 2023);
    assert_eq!(dt.month(), 4);
    assert_eq!(dt.day(), 1);
    assert_eq!(dt.hour(), 12);
    assert_eq!(dt.minute(), 34);
    assert_eq!(dt.second(), 56);

    // Test with timezone offset
    let iso8601_offset = "2023-04-01T12:34:56+02:00";
    let dt = rlightning::utils::datetime::parse_iso8601(iso8601_offset)
        .expect("Failed to parse ISO-8601 with offset");

    // The time should be converted to UTC
    assert_eq!(dt.hour(), 10); // 12 - 2 hours

    // Test without timezone (assumes UTC)
    let iso8601_no_tz = "2023-04-01T12:34:56";
    let dt = rlightning::utils::datetime::parse_iso8601(iso8601_no_tz)
        .expect("Failed to parse ISO-8601 without timezone");

    assert_eq!(dt.hour(), 12);
}
