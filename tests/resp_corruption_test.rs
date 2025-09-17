#[cfg(test)]
mod resp_corruption_tests {
    use rlightning::networking::resp::RespValue;
    use base64::{Engine as _, engine::general_purpose};

    #[test]
    fn test_resp_serialization_data_corruption() {
        // Test the exact scenario that causes corruption in RESP serialization
        
        // Create base64-encoded JSON like the backend uses
        let json_data = r#"{"id": 1, "title": "Test Story", "content": "Test\u{0001}content", "active": true}"#;
        let encoded_data = general_purpose::STANDARD.encode(json_data.as_bytes());
        let prefixed_data = format!("B64JSON:{}", encoded_data);
        
        println!("Original data: {}", prefixed_data);
        println!("Data length: {} bytes", prefixed_data.len());
        
        // Create RespValue and serialize it
        let resp_value = RespValue::BulkString(Some(prefixed_data.as_bytes().to_vec()));
        let serialized = resp_value.serialize().unwrap();
        
        // Parse it back
        let mut buffer = bytes::BytesMut::from(&serialized[..]);
        let parsed = RespValue::parse(&mut buffer).unwrap().unwrap();
        
        // Extract the data
        match parsed {
            RespValue::BulkString(Some(data)) => {
                let retrieved = String::from_utf8_lossy(&data);
                println!("Retrieved data: {}", retrieved);
                
                // Check if data was corrupted
                if retrieved != prefixed_data {
                    println!("❌ DATA CORRUPTION DETECTED!");
                    println!("Original: {}", prefixed_data);
                    println!("Retrieved: {}", retrieved);
                    
                    // Find differences
                    for (i, (orig, retr)) in prefixed_data.bytes().zip(retrieved.bytes()).enumerate() {
                        if orig != retr {
                            println!("Difference at position {}: original={:02x} ({:?}), retrieved={:02x} ({:?})", 
                                    i, orig, orig as char, retr, retr as char);
                        }
                    }
                    
                    panic!("Data corruption in RESP serialization!");
                } else {
                    println!("✓ Data integrity preserved");
                }
            },
            other => panic!("Expected BulkString(Some(_)), got: {:?}", other),
        }
    }
    
    #[test]
    fn test_json_with_control_characters_corruption() {
        // Test JSON data with control characters that might trigger sanitization
        let base64_json = format!("B64JSON:{}", general_purpose::STANDARD.encode(r#"{"test": "value"}"#.as_bytes()));
        let test_cases = vec![
            ("Simple JSON", r#"{"test": "value"}"#),
            ("JSON with null byte", "{\u{0000}\"test\": \"value\"}"),
            ("JSON with control chars", "{\u{0001}\"test\": \"val\u{0002}ue\"}"),
            ("JSON with CRLF", "{\"test\": \"val\r\nue\"}"),
            ("JSON with tab", "{\"test\": \"val\tue\"}"),
            ("Base64 JSON", base64_json.as_str()),
        ];
        
        for (desc, original_data) in test_cases {
            println!("\nTesting: {}", desc);
            
            let resp_value = RespValue::BulkString(Some(original_data.as_bytes().to_vec()));
            let serialized = resp_value.serialize().unwrap();
            
            let mut buffer = bytes::BytesMut::from(&serialized[..]);
            let parsed = RespValue::parse(&mut buffer).unwrap().unwrap();
            
            match parsed {
                RespValue::BulkString(Some(data)) => {
                    let retrieved = String::from_utf8_lossy(&data);
                    
                    if retrieved.as_bytes() != original_data.as_bytes() {
                        println!("❌ CORRUPTION in {}: '{}' -> '{}'", desc, original_data, retrieved);
                        panic!("Data corrupted for: {}", desc);
                    } else {
                        println!("✓ {} preserved correctly", desc);
                    }
                },
                other => panic!("Expected BulkString(Some(_)) for {}, got: {:?}", desc, other),
            }
        }
    }
    
    #[test]
    fn test_large_json_serialization_corruption() {
        // Test large JSON data that might trigger special handling
        let mut large_json = String::from(r#"{"items": ["#);
        for i in 0..1000 {
            if i > 0 { large_json.push_str(", "); }
            large_json.push_str(&format!(r#"{{"id": {}, "data": "item_{}_data"}}"#, i, i));
        }
        large_json.push_str("]}");
        
        let encoded_data = general_purpose::STANDARD.encode(large_json.as_bytes());
        let prefixed_data = format!("B64JSON:{}", encoded_data);
        
        println!("Large JSON test - data size: {} bytes", prefixed_data.len());
        
        let resp_value = RespValue::BulkString(Some(prefixed_data.as_bytes().to_vec()));
        let serialized = resp_value.serialize().unwrap();
        
        let mut buffer = bytes::BytesMut::from(&serialized[..]);
        let parsed = RespValue::parse(&mut buffer).unwrap().unwrap();
        
        match parsed {
            RespValue::BulkString(Some(data)) => {
                let retrieved = String::from_utf8_lossy(&data);
                
                if retrieved.len() != prefixed_data.len() {
                    panic!("Length mismatch: original {} bytes, retrieved {} bytes", 
                           prefixed_data.len(), retrieved.len());
                }
                
                if retrieved.as_bytes() != prefixed_data.as_bytes() {
                    println!("❌ Large JSON data corrupted!");
                    // Find first difference
                    for (i, (orig, retr)) in prefixed_data.bytes().zip(retrieved.bytes()).enumerate().take(100) {
                        if orig != retr {
                            println!("First difference at position {}: original={:02x}, retrieved={:02x}", i, orig, retr);
                            break;
                        }
                    }
                    panic!("Large JSON data corruption detected!");
                } else {
                    println!("✓ Large JSON data preserved correctly");
                }
            },
            other => panic!("Expected BulkString(Some(_)), got: {:?}", other),
        }
    }
}