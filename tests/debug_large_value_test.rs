/// Debug test to understand buffer behavior with large values
use bytes::BytesMut;
use rlightning::networking::raw_command::RawCommand;
use rlightning::networking::resp::RespValue;

#[test]
fn test_parse_large_set_command_in_chunks() {
    // Simulate what happens when a large SET command arrives in chunks

    let key = b"testkey";
    let value_size = 50000;
    let value = vec![b'x'; value_size];

    // Build the complete SET command
    let mut complete_command = BytesMut::new();
    complete_command.extend_from_slice(b"*3\r\n");
    complete_command.extend_from_slice(b"$3\r\nSET\r\n");
    complete_command.extend_from_slice(b"$");
    complete_command.extend_from_slice(key.len().to_string().as_bytes());
    complete_command.extend_from_slice(b"\r\n");
    complete_command.extend_from_slice(key);
    complete_command.extend_from_slice(b"\r\n");
    complete_command.extend_from_slice(b"$");
    complete_command.extend_from_slice(value_size.to_string().as_bytes());
    complete_command.extend_from_slice(b"\r\n");
    complete_command.extend_from_slice(&value);
    complete_command.extend_from_slice(b"\r\n");

    println!("Total command size: {} bytes", complete_command.len());

    // Simulate chunked reading
    let chunk_size = 16384; // 16KB chunks (typical TCP window)
    let mut offset = 0;
    let mut partial_buffer = BytesMut::new();
    let mut chunk_num = 0;

    while offset < complete_command.len() {
        chunk_num += 1;
        let end = (offset + chunk_size).min(complete_command.len());
        let chunk = &complete_command[offset..end];

        println!(
            "\n=== Chunk {} ({} bytes, offset={}) ===",
            chunk_num,
            chunk.len(),
            offset
        );

        // Simulate server behavior: combine partial buffer with new chunk
        let mut buffer = BytesMut::new();
        if !partial_buffer.is_empty() {
            println!("Prepending partial buffer ({} bytes)", partial_buffer.len());
            buffer.extend_from_slice(&partial_buffer);
            partial_buffer.clear();
        }
        buffer.extend_from_slice(chunk);

        println!("Total buffer size: {} bytes", buffer.len());
        println!(
            "Buffer starts with: {:?}",
            String::from_utf8_lossy(&buffer[0..20.min(buffer.len())])
        );

        // Try to parse
        match RespValue::parse(&mut buffer) {
            Ok(Some(value)) => {
                println!("✓ Successfully parsed command!");
                println!("Remaining buffer: {} bytes", buffer.len());
                match value {
                    RespValue::Array(Some(elements)) => {
                        println!("Array with {} elements", elements.len());
                        for (i, el) in elements.iter().enumerate() {
                            match el {
                                RespValue::BulkString(Some(data)) => {
                                    println!("  Element {}: BulkString({} bytes)", i, data.len());
                                }
                                other => println!("  Element {}: {:?}", i, other),
                            }
                        }
                    }
                    other => println!("Parsed: {:?}", other),
                }
                break;
            }
            Ok(None) => {
                println!("○ Incomplete - need more data");
                println!("Saving {} bytes to partial buffer", buffer.len());
                partial_buffer.extend_from_slice(&buffer);
            }
            Err(e) => {
                println!("✗ Parse error: {}", e);
                println!(
                    "Buffer content (first 100 bytes): {:?}",
                    String::from_utf8_lossy(&buffer[0..100.min(buffer.len())])
                );
                panic!("Should not get parse error for valid command: {}", e);
            }
        }

        offset = end;
    }

    assert!(partial_buffer.is_empty(), "Should have consumed all data");
}

#[test]
fn test_fast_path_with_large_value() {
    // Test if the zero-copy fast path (RawCommand::try_parse) handles large values correctly

    let key = b"testkey";
    let value_size = 50000;
    let value = vec![b'x'; value_size];

    let mut buffer = BytesMut::new();
    buffer.extend_from_slice(b"*3\r\n$3\r\nSET\r\n");
    buffer.extend_from_slice(b"$");
    buffer.extend_from_slice(key.len().to_string().as_bytes());
    buffer.extend_from_slice(b"\r\n");
    buffer.extend_from_slice(key);
    buffer.extend_from_slice(b"\r\n");
    buffer.extend_from_slice(b"$");
    buffer.extend_from_slice(value_size.to_string().as_bytes());
    buffer.extend_from_slice(b"\r\n");
    buffer.extend_from_slice(&value);
    buffer.extend_from_slice(b"\r\n");

    println!("Testing fast path with {} byte buffer", buffer.len());

    // Try zero-copy fast path
    match RawCommand::try_parse(&buffer) {
        Ok(Some(raw_cmd)) => {
            let cmd = raw_cmd.to_command();
            println!("✓ Fast path succeeded");
            println!("Command: {:?}", cmd.name);
            println!("Args: {}", cmd.args.len());
            for (i, arg) in cmd.args.iter().enumerate() {
                println!("  Arg {}: {} bytes", i, arg.len());
            }
            assert_eq!(cmd.args.len(), 2);
            assert_eq!(cmd.args[1].len(), value_size);
        }
        Ok(None) => {
            println!("○ Fast path returned None, trying regular parse");
            let mut buf_clone = buffer.clone();
            match RespValue::parse(&mut buf_clone) {
                Ok(Some(_)) => println!("✓ Regular parse succeeded"),
                Ok(None) => panic!("Regular parse returned None (incomplete)"),
                Err(e) => panic!("Regular parse failed: {}", e),
            }
        }
        Err(e) => {
            panic!("Fast path error: {}", e);
        }
    }
}
