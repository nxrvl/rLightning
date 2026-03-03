use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

use rlightning::networking::resp::RespValue;

fn bench_parse_simple_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("RESP Parsing - Simple Types");

    // Simple string
    let simple_string = BytesMut::from(b"+OK\r\n".as_slice());
    group.bench_function("SimpleString", |b| {
        b.iter(|| {
            let mut buffer = simple_string.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Error
    let error = BytesMut::from(b"-Error message\r\n".as_slice());
    group.bench_function("Error", |b| {
        b.iter(|| {
            let mut buffer = error.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Integer
    let integer = BytesMut::from(b":1000\r\n".as_slice());
    group.bench_function("Integer", |b| {
        b.iter(|| {
            let mut buffer = integer.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Large integer
    let large_integer = BytesMut::from(b":9223372036854775807\r\n".as_slice());
    group.bench_function("LargeInteger", |b| {
        b.iter(|| {
            let mut buffer = large_integer.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Negative integer
    let negative_integer = BytesMut::from(b":-12345\r\n".as_slice());
    group.bench_function("NegativeInteger", |b| {
        b.iter(|| {
            let mut buffer = negative_integer.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    group.finish();
}

fn bench_parse_bulk_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("RESP Parsing - Bulk Strings");

    // Small bulk string
    let small_bulk = BytesMut::from(b"$5\r\nhello\r\n".as_slice());
    group.bench_function("SmallBulkString", |b| {
        b.iter(|| {
            let mut buffer = small_bulk.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Medium bulk string
    let medium_data = "a".repeat(1000);
    let medium_bulk =
        BytesMut::from(format!("${}\r\n{}\r\n", medium_data.len(), medium_data).as_str());
    group.bench_function("MediumBulkString", |b| {
        b.iter(|| {
            let mut buffer = medium_bulk.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Large bulk string
    let large_data = "x".repeat(10000);
    let large_bulk =
        BytesMut::from(format!("${}\r\n{}\r\n", large_data.len(), large_data).as_str());
    group.bench_function("LargeBulkString", |b| {
        b.iter(|| {
            let mut buffer = large_bulk.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Null bulk string
    let null_bulk = BytesMut::from(b"$-1\r\n".as_slice());
    group.bench_function("NullBulkString", |b| {
        b.iter(|| {
            let mut buffer = null_bulk.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Empty bulk string
    let empty_bulk = BytesMut::from(b"$0\r\n\r\n".as_slice());
    group.bench_function("EmptyBulkString", |b| {
        b.iter(|| {
            let mut buffer = empty_bulk.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    group.finish();
}

fn bench_parse_arrays(c: &mut Criterion) {
    let mut group = c.benchmark_group("RESP Parsing - Arrays");

    // Small array
    let small_array = BytesMut::from(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_slice());
    group.bench_function("SmallArray", |b| {
        b.iter(|| {
            let mut buffer = small_array.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Nested array
    let nested_array =
        BytesMut::from(b"*2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*1\r\n$3\r\nbaz\r\n".as_slice());
    group.bench_function("NestedArray", |b| {
        b.iter(|| {
            let mut buffer = nested_array.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Mixed type array
    let mixed_array =
        BytesMut::from(b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n:100\r\n".as_slice());
    group.bench_function("MixedTypeArray", |b| {
        b.iter(|| {
            let mut buffer = mixed_array.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Large array with many elements
    let mut large_array_data = "*10\r\n".to_string();
    for _i in 0..10 {
        large_array_data.push_str(&format!("$4\r\nitem\r\n"));
    }
    let large_array = BytesMut::from(large_array_data.as_str());
    group.bench_function("LargeArray", |b| {
        b.iter(|| {
            let mut buffer = large_array.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Empty array
    let empty_array = BytesMut::from(b"*0\r\n".as_slice());
    group.bench_function("EmptyArray", |b| {
        b.iter(|| {
            let mut buffer = empty_array.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // Null array
    let null_array = BytesMut::from(b"*-1\r\n".as_slice());
    group.bench_function("NullArray", |b| {
        b.iter(|| {
            let mut buffer = null_array.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    group.finish();
}

fn bench_parse_real_commands(c: &mut Criterion) {
    let mut group = c.benchmark_group("RESP Parsing - Real Commands");

    // SET command
    let set_cmd = BytesMut::from(b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n".as_slice());
    group.bench_function("SET Command", |b| {
        b.iter(|| {
            let mut buffer = set_cmd.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // GET command
    let get_cmd = BytesMut::from(b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n".as_slice());
    group.bench_function("GET Command", |b| {
        b.iter(|| {
            let mut buffer = get_cmd.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // MSET command
    let mset_cmd = BytesMut::from(
        b"*5\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n"
            .as_slice(),
    );
    group.bench_function("MSET Command", |b| {
        b.iter(|| {
            let mut buffer = mset_cmd.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // HSET command
    let hset_cmd = BytesMut::from(
        b"*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n".as_slice(),
    );
    group.bench_function("HSET Command", |b| {
        b.iter(|| {
            let mut buffer = hset_cmd.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // LPUSH command
    let lpush_cmd = BytesMut::from(
        b"*4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$5\r\nitem1\r\n$5\r\nitem2\r\n".as_slice(),
    );
    group.bench_function("LPUSH Command", |b| {
        b.iter(|| {
            let mut buffer = lpush_cmd.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // ZADD command
    let zadd_cmd = BytesMut::from(
        b"*4\r\n$4\r\nZADD\r\n$6\r\nmyzset\r\n$3\r\n1.5\r\n$6\r\nmember\r\n".as_slice(),
    );
    group.bench_function("ZADD Command", |b| {
        b.iter(|| {
            let mut buffer = zadd_cmd.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    // JSON.SET command
    let json_set_cmd = BytesMut::from(b"*4\r\n$8\r\nJSON.SET\r\n$6\r\nmyjson\r\n$1\r\n$\r\n$27\r\n{\"name\":\"test\",\"value\":123}\r\n".as_slice());
    group.bench_function("JSON.SET Command", |b| {
        b.iter(|| {
            let mut buffer = json_set_cmd.clone();
            black_box(RespValue::parse(&mut buffer).unwrap());
        })
    });

    group.finish();
}

fn bench_serialize_simple_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("RESP Serialization - Simple Types");

    // Simple string
    let simple_string = RespValue::SimpleString("OK".to_string());
    group.bench_function("SimpleString", |b| {
        b.iter(|| {
            black_box(simple_string.serialize().unwrap());
        })
    });

    // Error
    let error = RespValue::Error("Error message".to_string());
    group.bench_function("Error", |b| {
        b.iter(|| {
            black_box(error.serialize().unwrap());
        })
    });

    // Integer
    let integer = RespValue::Integer(1000);
    group.bench_function("Integer", |b| {
        b.iter(|| {
            black_box(integer.serialize().unwrap());
        })
    });

    // Large integer
    let large_integer = RespValue::Integer(9223372036854775807);
    group.bench_function("LargeInteger", |b| {
        b.iter(|| {
            black_box(large_integer.serialize().unwrap());
        })
    });

    // Negative integer
    let negative_integer = RespValue::Integer(-12345);
    group.bench_function("NegativeInteger", |b| {
        b.iter(|| {
            black_box(negative_integer.serialize().unwrap());
        })
    });

    group.finish();
}

fn bench_serialize_bulk_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("RESP Serialization - Bulk Strings");

    // Small bulk string
    let small_bulk = RespValue::BulkString(Some(b"hello".to_vec()));
    group.bench_function("SmallBulkString", |b| {
        b.iter(|| {
            black_box(small_bulk.serialize().unwrap());
        })
    });

    // Medium bulk string
    let medium_data = vec![b'a'; 1000];
    let medium_bulk = RespValue::BulkString(Some(medium_data));
    group.bench_function("MediumBulkString", |b| {
        b.iter(|| {
            black_box(medium_bulk.serialize().unwrap());
        })
    });

    // Large bulk string
    let large_data = vec![b'x'; 10000];
    let large_bulk = RespValue::BulkString(Some(large_data));
    group.bench_function("LargeBulkString", |b| {
        b.iter(|| {
            black_box(large_bulk.serialize().unwrap());
        })
    });

    // Null bulk string
    let null_bulk = RespValue::BulkString(None);
    group.bench_function("NullBulkString", |b| {
        b.iter(|| {
            black_box(null_bulk.serialize().unwrap());
        })
    });

    // Empty bulk string
    let empty_bulk = RespValue::BulkString(Some(vec![]));
    group.bench_function("EmptyBulkString", |b| {
        b.iter(|| {
            black_box(empty_bulk.serialize().unwrap());
        })
    });

    group.finish();
}

fn bench_serialize_arrays(c: &mut Criterion) {
    let mut group = c.benchmark_group("RESP Serialization - Arrays");

    // Small array
    let small_array = RespValue::Array(Some(vec![
        RespValue::BulkString(Some(b"hello".to_vec())),
        RespValue::BulkString(Some(b"world".to_vec())),
    ]));
    group.bench_function("SmallArray", |b| {
        b.iter(|| {
            black_box(small_array.serialize().unwrap());
        })
    });

    // Nested array
    let nested_array = RespValue::Array(Some(vec![
        RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"foo".to_vec())),
            RespValue::BulkString(Some(b"bar".to_vec())),
        ])),
        RespValue::Array(Some(vec![RespValue::BulkString(Some(b"baz".to_vec()))])),
    ]));
    group.bench_function("NestedArray", |b| {
        b.iter(|| {
            black_box(nested_array.serialize().unwrap());
        })
    });

    // Mixed type array
    let mixed_array = RespValue::Array(Some(vec![
        RespValue::BulkString(Some(b"SET".to_vec())),
        RespValue::BulkString(Some(b"key".to_vec())),
        RespValue::BulkString(Some(b"value".to_vec())),
        RespValue::Integer(100),
    ]));
    group.bench_function("MixedTypeArray", |b| {
        b.iter(|| {
            black_box(mixed_array.serialize().unwrap());
        })
    });

    // Large array
    let large_array_elements: Vec<RespValue> = (0..10)
        .map(|_| RespValue::BulkString(Some(b"item".to_vec())))
        .collect();
    let large_array = RespValue::Array(Some(large_array_elements));
    group.bench_function("LargeArray", |b| {
        b.iter(|| {
            black_box(large_array.serialize().unwrap());
        })
    });

    // Empty array
    let empty_array = RespValue::Array(Some(vec![]));
    group.bench_function("EmptyArray", |b| {
        b.iter(|| {
            black_box(empty_array.serialize().unwrap());
        })
    });

    // Null array
    let null_array = RespValue::Array(None);
    group.bench_function("NullArray", |b| {
        b.iter(|| {
            black_box(null_array.serialize().unwrap());
        })
    });

    group.finish();
}

fn bench_serialize_responses(c: &mut Criterion) {
    let mut group = c.benchmark_group("RESP Serialization - Command Responses");

    // OK response
    let ok_response = RespValue::SimpleString("OK".to_string());
    group.bench_function("OK Response", |b| {
        b.iter(|| {
            black_box(ok_response.serialize().unwrap());
        })
    });

    // String value response
    let string_response = RespValue::BulkString(Some(b"myvalue".to_vec()));
    group.bench_function("String Value Response", |b| {
        b.iter(|| {
            black_box(string_response.serialize().unwrap());
        })
    });

    // Integer response
    let integer_response = RespValue::Integer(42);
    group.bench_function("Integer Response", |b| {
        b.iter(|| {
            black_box(integer_response.serialize().unwrap());
        })
    });

    // Multi-value response (MGET)
    let multi_response = RespValue::Array(Some(vec![
        RespValue::BulkString(Some(b"value1".to_vec())),
        RespValue::BulkString(Some(b"value2".to_vec())),
        RespValue::BulkString(None), // null value
        RespValue::BulkString(Some(b"value4".to_vec())),
    ]));
    group.bench_function("Multi-Value Response", |b| {
        b.iter(|| {
            black_box(multi_response.serialize().unwrap());
        })
    });

    // Hash response (HGETALL)
    let hash_response = RespValue::Array(Some(vec![
        RespValue::BulkString(Some(b"field1".to_vec())),
        RespValue::BulkString(Some(b"value1".to_vec())),
        RespValue::BulkString(Some(b"field2".to_vec())),
        RespValue::BulkString(Some(b"value2".to_vec())),
    ]));
    group.bench_function("Hash Response", |b| {
        b.iter(|| {
            black_box(hash_response.serialize().unwrap());
        })
    });

    // Error response
    let error_response = RespValue::Error("ERR unknown command".to_string());
    group.bench_function("Error Response", |b| {
        b.iter(|| {
            black_box(error_response.serialize().unwrap());
        })
    });

    // JSON response
    let json_data = r#"{"name":"test","value":123,"active":true,"items":[1,2,3]}"#;
    let json_response = RespValue::BulkString(Some(json_data.as_bytes().to_vec()));
    group.bench_function("JSON Response", |b| {
        b.iter(|| {
            black_box(json_response.serialize().unwrap());
        })
    });

    group.finish();
}

fn bench_round_trip_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("RESP Round-Trip Performance");

    // Test different sizes for round-trip performance
    for &size in &[10, 100, 1000, 10000] {
        group.bench_with_input(BenchmarkId::new("RoundTrip", size), &size, |b, &size| {
            let data = vec![b'x'; size];
            let value = RespValue::BulkString(Some(data));

            b.iter(|| {
                let serialized = black_box(value.serialize().unwrap());
                let mut buffer = BytesMut::from(serialized.as_slice());
                black_box(RespValue::parse(&mut buffer).unwrap());
            });
        });
    }

    group.finish();
}

fn bench_error_handling(c: &mut Criterion) {
    let mut group = c.benchmark_group("RESP Error Handling");

    // Test parsing incomplete data (should return Ok(None))
    let incomplete_data = BytesMut::from(b"$10\r\nhel".as_slice());
    group.bench_function("Incomplete Data", |b| {
        b.iter(|| {
            let mut buffer = incomplete_data.clone();
            // This should return Ok(None) indicating more data needed
            let result = black_box(RespValue::parse(&mut buffer));
            assert!(result.is_ok());
        })
    });

    // Test valid data that requires multiple buffer extends
    let valid_data = BytesMut::from(b"$5\r\nhello\r\n".as_slice());
    group.bench_function("Valid Complete Data", |b| {
        b.iter(|| {
            let mut buffer = valid_data.clone();
            let result = black_box(RespValue::parse(&mut buffer));
            assert!(result.is_ok());
        })
    });

    group.finish();
}

criterion_group!(
    parsing_benchmarks,
    bench_parse_simple_types,
    bench_parse_bulk_strings,
    bench_parse_arrays,
    bench_parse_real_commands
);

criterion_group!(
    serialization_benchmarks,
    bench_serialize_simple_types,
    bench_serialize_bulk_strings,
    bench_serialize_arrays,
    bench_serialize_responses
);

criterion_group!(
    performance_benchmarks,
    bench_round_trip_performance,
    bench_error_handling
);

criterion_main!(
    parsing_benchmarks,
    serialization_benchmarks,
    performance_benchmarks
);
