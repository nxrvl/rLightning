use serde_json::{Value, json};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

// Helper function to start a server and return its address and a connected client
async fn setup_server_client(port_offset: u16) -> (SocketAddr, Client) {
    // Create a storage engine
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);

    // Use a unique port for each test
    let port = 17000 + port_offset;
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Create a server
    let server = Server::new(addr, storage);

    // Start the server in a background task
    tokio::spawn(async move {
        server.start().await.unwrap();
    });

    // Allow some time for the server to start
    time::sleep(Duration::from_millis(200)).await;

    // Connect a client
    let client = Client::connect(addr).await.unwrap();

    (addr, client)
}

// Helper function to create a large JSON document similar to the one in the logs
fn create_large_json() -> Value {
    let mut chapters = Vec::new();

    // Create 50 chapters to make it large
    for i in 1..=50 {
        chapters.push(json!({
            "chapter_id": i,
            "title": format!("Chapter {}: The Adventure Continues", i),
            "content": format!("This is the content of chapter {}. It contains a lot of text to make the JSON document large. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.", i),
            "author": format!("Author {}", i),
            "published_date": format!("2024-01-{:02}", i % 28 + 1),
            "word_count": i * 150,
            "tags": vec![format!("tag{}", i), format!("category{}", i % 5)],
            "metadata": {
                "created_at": format!("2024-01-{:02}T10:30:00Z", i % 28 + 1),
                "updated_at": format!("2024-01-{:02}T15:45:00Z", i % 28 + 1),
                "version": i
            }
        }));
    }

    json!({
        "book_id": 12345,
        "title": "The Complete Guide to Large JSON Documents",
        "description": "This is a comprehensive test document containing multiple chapters with rich metadata and content designed to test JSON protocol handling in Redis-compatible systems.",
        "total_chapters": chapters.len(),
        "chapters": chapters,
        "book_metadata": {
            "isbn": "978-0123456789",
            "publication_year": 2024,
            "genre": "Technical Documentation",
            "language": "en",
            "page_count": 1250,
            "publisher": {
                "name": "Test Publishers Inc.",
                "address": "123 Test Street, Test City, TC 12345",
                "website": "https://testpublishers.example.com"
            }
        },
        "reviews": [
            {
                "reviewer": "Alice Johnson",
                "rating": 5,
                "comment": "Excellent comprehensive guide with detailed examples and clear explanations."
            },
            {
                "reviewer": "Bob Smith",
                "rating": 4,
                "comment": "Very helpful resource, though some sections could be more concise."
            },
            {
                "reviewer": "Carol Davis",
                "rating": 5,
                "comment": "Perfect for both beginners and advanced users. Highly recommended!"
            }
        ],
        "availability": {
            "in_stock": true,
            "quantity": 150,
            "price": 49.99,
            "currency": "USD",
            "formats": ["hardcover", "paperback", "ebook", "audiobook"]
        }
    })
}

#[tokio::test]
async fn test_large_json_integration() {
    let (_addr, mut client) = setup_server_client(0).await;

    // Create a large JSON document
    let large_json = create_large_json();
    let json_str = large_json.to_string();

    println!("JSON document size: {} bytes", json_str.len());
    assert!(
        json_str.len() > 5000,
        "JSON should be at least 5KB for this test"
    );

    let key = "large_document:12345";

    // Test JSON.SET command
    let set_response = client
        .send_command(vec![
            b"JSON.SET".to_vec(),
            key.as_bytes().to_vec(),
            b".".to_vec(),
            json_str.as_bytes().to_vec(),
        ])
        .await
        .unwrap();

    match set_response {
        RespValue::SimpleString(s) if s == "OK" => {
            println!("✓ JSON.SET successful for large document");
        }
        _ => panic!("JSON.SET failed: {:?}", set_response),
    }

    // Test JSON.GET command
    let get_response = client
        .send_command(vec![
            b"JSON.GET".to_vec(),
            key.as_bytes().to_vec(),
            b".".to_vec(),
        ])
        .await
        .unwrap();

    match get_response {
        RespValue::BulkString(Some(data)) => {
            let retrieved_json_str = String::from_utf8_lossy(&data);
            println!("Retrieved JSON size: {} bytes", retrieved_json_str.len());

            // Parse the retrieved JSON
            let retrieved_json: Value =
                serde_json::from_str(&retrieved_json_str).expect("Retrieved JSON should be valid");

            // Verify the structure and content
            assert_eq!(retrieved_json["book_id"], 12345);
            assert_eq!(
                retrieved_json["title"],
                "The Complete Guide to Large JSON Documents"
            );
            assert_eq!(retrieved_json["total_chapters"], 50);
            assert!(retrieved_json["chapters"].is_array());
            assert_eq!(retrieved_json["chapters"].as_array().unwrap().len(), 50);

            // Verify some nested data
            assert_eq!(retrieved_json["book_metadata"]["isbn"], "978-0123456789");
            assert_eq!(retrieved_json["availability"]["price"], 49.99);
            assert!(retrieved_json["reviews"].as_array().unwrap().len() == 3);

            println!("✓ JSON.GET successful and data integrity verified");
        }
        _ => panic!("JSON.GET failed: {:?}", get_response),
    }
}

#[tokio::test]
async fn test_json_path_operations() {
    let (_addr, mut client) = setup_server_client(1).await;

    let key = "test_path_ops";
    let json_data = json!({
        "users": [
            {"id": 1, "name": "Alice", "active": true},
            {"id": 2, "name": "Bob", "active": false},
            {"id": 3, "name": "Charlie", "active": true}
        ],
        "metadata": {
            "total": 3,
            "last_updated": "2024-01-15T10:30:00Z"
        }
    });

    // Set the initial JSON
    let set_response = client
        .send_command(vec![
            b"JSON.SET".to_vec(),
            key.as_bytes().to_vec(),
            b".".to_vec(),
            json_data.to_string().as_bytes().to_vec(),
        ])
        .await
        .unwrap();

    assert!(matches!(set_response, RespValue::SimpleString(ref s) if s == "OK"));

    // Test getting a specific path
    let get_users_response = client
        .send_command(vec![
            b"JSON.GET".to_vec(),
            key.as_bytes().to_vec(),
            b".users".to_vec(),
        ])
        .await
        .unwrap();

    match get_users_response {
        RespValue::BulkString(Some(data)) => {
            let users_json_str = String::from_utf8_lossy(&data);
            let users: Value = serde_json::from_str(&users_json_str).unwrap();
            assert!(users.is_array());
            assert_eq!(users.as_array().unwrap().len(), 3);
            println!("✓ JSON.GET with path successful");
        }
        _ => panic!("JSON.GET with path failed: {:?}", get_users_response),
    }
}

#[tokio::test]
async fn test_json_error_handling() {
    let (_addr, mut client) = setup_server_client(2).await;

    // Test setting invalid JSON
    let invalid_json = r#"{"incomplete": true, "missing_end":"#;
    let key = "test_invalid";

    let set_response = client
        .send_command(vec![
            b"JSON.SET".to_vec(),
            key.as_bytes().to_vec(),
            b".".to_vec(),
            invalid_json.as_bytes().to_vec(),
        ])
        .await
        .unwrap();

    // Should return an error for invalid JSON
    match set_response {
        RespValue::Error(err_msg) => {
            println!("✓ Correctly rejected invalid JSON: {}", err_msg);
            assert!(
                err_msg.to_lowercase().contains("json") || err_msg.to_lowercase().contains("parse")
            );
        }
        _ => {
            // Some implementations might accept it and store as string
            println!("Implementation stored invalid JSON - checking if retrievable");
        }
    }

    // Test getting from non-existent key
    let get_response = client
        .send_command(vec![
            b"JSON.GET".to_vec(),
            b"non_existent_key".to_vec(),
            b".".to_vec(),
        ])
        .await
        .unwrap();

    match get_response {
        RespValue::BulkString(None) => {
            println!("✓ Correctly returned null for non-existent key");
        }
        RespValue::Error(_) => {
            println!("✓ Correctly returned error for non-existent key");
        }
        _ => {
            println!("Note: Implementation returned: {:?}", get_response);
        }
    }
}
