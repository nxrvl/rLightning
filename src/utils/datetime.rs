use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};

/// A wrapper for datetime fields that ensures they are serialized as ISO-8601 strings
#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
pub enum DateTimeField {
    Iso8601String(String),
    TimestampMillis(i64),
}

impl Serialize for DateTimeField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            DateTimeField::Iso8601String(s) => serializer.serialize_str(s),
            DateTimeField::TimestampMillis(ts) => {
                let dt = DateTime::<Utc>::from_timestamp_millis(*ts)
                    .ok_or_else(|| serde::ser::Error::custom("Invalid timestamp"))?;
                serializer.serialize_str(&dt.to_rfc3339())
            }
        }
    }
}

lazy_static! {
    // Regex patterns for common datetime formats
    static ref DATETIME_REGEX: Regex = Regex::new(
        r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})[ T](\d{1,2}):(\d{1,2}):(\d{1,2})(?:\.(\d+))?(?:Z|([+-]\d{2}:?\d{2}))?$"
    ).unwrap();

    static ref PYTHON_AT_REGEX: Regex = Regex::new(
        r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2}) at (\d{1,2}):(\d{1,2}):(\d{1,2})(?:\.(\d+))?$"
    ).unwrap();
}

/// Processes a JSON value to ensure all datetime objects are properly serialized to ISO-8601
pub fn process_json_for_serialization(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut new_map = Map::new();
            for (k, v) in map {
                new_map.insert(k, process_json_for_serialization(v));
            }
            Value::Object(new_map)
        }
        Value::Array(arr) => {
            let new_arr = arr
                .into_iter()
                .map(process_json_for_serialization)
                .collect();
            Value::Array(new_arr)
        }
        Value::String(s) => {
            // Check if this string looks like a datetime and convert it to ISO-8601
            if let Some(iso_date) = convert_to_iso8601(&s) {
                return Value::String(iso_date);
            }
            Value::String(s)
        }
        // Other value types remain unchanged
        _ => value,
    }
}

/// Convert a string to ISO-8601 format if it resembles a datetime
/// Returns None if the string doesn't appear to be a datetime
pub fn convert_to_iso8601(s: &str) -> Option<String> {
    // If it's already in ISO-8601 format (with T separator), return as is
    if s.contains('T') && DATETIME_REGEX.is_match(s) {
        return Some(s.to_string());
    }

    // Handle Python "at" format
    if let Some(caps) = PYTHON_AT_REGEX.captures(s) {
        let year = &caps[1];
        let month = &caps[2];
        let day = &caps[3];
        let hour = &caps[4];
        let minute = &caps[5];
        let second = &caps[6];
        let fraction = caps.get(7).map_or("", |m| m.as_str());

        if !fraction.is_empty() {
            return Some(format!(
                "{}-{:02}-{:02}T{:02}:{:02}:{:02}.{}",
                year,
                month.parse::<u8>().unwrap_or(1),
                day.parse::<u8>().unwrap_or(1),
                hour.parse::<u8>().unwrap_or(0),
                minute.parse::<u8>().unwrap_or(0),
                second.parse::<u8>().unwrap_or(0),
                fraction
            ));
        } else {
            return Some(format!(
                "{}-{:02}-{:02}T{:02}:{:02}:{:02}",
                year,
                month.parse::<u8>().unwrap_or(1),
                day.parse::<u8>().unwrap_or(1),
                hour.parse::<u8>().unwrap_or(0),
                minute.parse::<u8>().unwrap_or(0),
                second.parse::<u8>().unwrap_or(0)
            ));
        }
    }

    // Handle standard datetime format with space separator
    if let Some(caps) = DATETIME_REGEX.captures(s) {
        let year = &caps[1];
        let month = &caps[2];
        let day = &caps[3];
        let hour = &caps[4];
        let minute = &caps[5];
        let second = &caps[6];
        let fraction = caps.get(7).map_or("", |m| m.as_str());
        let timezone = caps.get(8).map_or("", |m| m.as_str());

        let datetime_part = if !fraction.is_empty() {
            format!(
                "{}-{:02}-{:02}T{:02}:{:02}:{:02}.{}",
                year,
                month.parse::<u8>().unwrap_or(1),
                day.parse::<u8>().unwrap_or(1),
                hour.parse::<u8>().unwrap_or(0),
                minute.parse::<u8>().unwrap_or(0),
                second.parse::<u8>().unwrap_or(0),
                fraction
            )
        } else {
            format!(
                "{}-{:02}-{:02}T{:02}:{:02}:{:02}",
                year,
                month.parse::<u8>().unwrap_or(1),
                day.parse::<u8>().unwrap_or(1),
                hour.parse::<u8>().unwrap_or(0),
                minute.parse::<u8>().unwrap_or(0),
                second.parse::<u8>().unwrap_or(0)
            )
        };

        if !timezone.is_empty() {
            return Some(format!("{}{}", datetime_part, timezone));
        } else {
            return Some(datetime_part);
        }
    }

    // Not a recognized datetime format
    None
}

/// Convert ISO-8601 format back to the original format if needed
/// This allows bidirectional conversion between formats
#[allow(dead_code)]
pub fn convert_from_iso8601(iso_string: &str, target_format: DateTimeFormat) -> Option<String> {
    if let Some(caps) = DATETIME_REGEX.captures(iso_string) {
        let year = &caps[1];
        let month = &caps[2];
        let day = &caps[3];
        let hour = &caps[4];
        let minute = &caps[5];
        let second = &caps[6];
        let fraction = caps.get(7).map_or("", |m| m.as_str());

        match target_format {
            DateTimeFormat::SpaceSeparated => {
                if !fraction.is_empty() {
                    Some(format!(
                        "{}-{}-{} {}:{}:{}.{}",
                        year, month, day, hour, minute, second, fraction
                    ))
                } else {
                    Some(format!(
                        "{}-{}-{} {}:{}:{}",
                        year, month, day, hour, minute, second
                    ))
                }
            }
            DateTimeFormat::PythonAt => {
                if !fraction.is_empty() {
                    Some(format!(
                        "{}-{}-{} at {}:{}:{}.{}",
                        year, month, day, hour, minute, second, fraction
                    ))
                } else {
                    Some(format!(
                        "{}-{}-{} at {}:{}:{}",
                        year, month, day, hour, minute, second
                    ))
                }
            }
            DateTimeFormat::ISO8601 => Some(iso_string.to_string()),
        }
    } else {
        None
    }
}

/// Supported datetime formats for conversion
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
pub enum DateTimeFormat {
    ISO8601,
    SpaceSeparated,
    PythonAt,
}

/// Preprocesses data for JSON serialization with datetime handling
#[allow(dead_code)]
pub fn preprocess_data_for_json<T>(data: &mut T)
where
    T: for<'a> serde::Serialize + serde::de::DeserializeOwned + Clone,
{
    // First, serialize to Value
    if let Ok(mut value) = serde_json::to_value(data.clone()) {
        // Process the value to handle datetime fields
        value = process_json_for_serialization(value);

        // Deserialize back to the original type
        if let Ok(processed) = serde_json::from_value::<T>(value) {
            *data = processed;
        }
    }
}

/// Wrapper function to directly serialize any data with datetime handling
#[allow(dead_code)]
pub fn serialize_with_datetime<T>(data: &T) -> Result<String, String>
where
    T: Serialize,
{
    // First convert to Value
    let value = match serde_json::to_value(data) {
        Ok(v) => v,
        Err(e) => return Err(format!("Failed to convert to JSON value: {}", e)),
    };

    // Process to handle datetime fields
    let processed = process_json_for_serialization(value);

    // Serialize to string
    match serde_json::to_string(&processed) {
        Ok(s) => Ok(s),
        Err(e) => Err(format!("Failed to serialize JSON: {}", e)),
    }
}

/// Parse an ISO-8601 string to a DateTime object
#[allow(dead_code)]
pub fn parse_iso8601(iso_string: &str) -> Option<DateTime<Utc>> {
    // Try to parse with chrono's built-in parser
    if let Ok(dt) = DateTime::parse_from_rfc3339(iso_string) {
        return Some(dt.with_timezone(&Utc));
    }

    // Handle ISO-8601 without timezone (assume UTC)
    if let Some(caps) = DATETIME_REGEX.captures(iso_string) {
        let year = caps[1].parse::<i32>().ok()?;
        let month = caps[2].parse::<u32>().ok()?;
        let day = caps[3].parse::<u32>().ok()?;
        let hour = caps[4].parse::<u32>().ok()?;
        let minute = caps[5].parse::<u32>().ok()?;
        let second = caps[6].parse::<u32>().ok()?;

        let mut nano = 0;
        if let Some(frac) = caps.get(7) {
            let frac_str = frac.as_str();
            let frac_val = frac_str.parse::<u32>().ok()?;
            nano = frac_val * 10u32.pow((9 - frac_str.len() as u32).min(9));
        }

        // Create NaiveDate and NaiveTime separately
        let naive_date = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
        let naive_time = chrono::NaiveTime::from_hms_nano_opt(hour, minute, second, nano)?;

        // Combine them to create a NaiveDateTime
        let naive_dt = chrono::NaiveDateTime::new(naive_date, naive_time);

        // Convert to DateTime<Utc>
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc));
    }

    None
}
