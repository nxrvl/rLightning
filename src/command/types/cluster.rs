use std::sync::Arc;

use crate::cluster::ClusterManager;
use crate::cluster::slot::key_hash_slot;
use crate::command::error::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;

/// Handle CLUSTER command with all subcommands
pub async fn cluster_command(
    _engine: &StorageEngine,
    args: &[Vec<u8>],
    cluster_mgr: Option<&Arc<ClusterManager>>,
) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();

    let mgr = match cluster_mgr {
        Some(m) => m,
        None => {
            // When cluster mode is not enabled, return appropriate responses
            return match subcommand.as_str() {
                "INFO" => Ok(RespValue::BulkString(Some(
                    b"cluster_enabled:0\r\n\
                          cluster_state:ok\r\n\
                          cluster_slots_assigned:0\r\n\
                          cluster_slots_ok:0\r\n\
                          cluster_slots_pfail:0\r\n\
                          cluster_slots_fail:0\r\n\
                          cluster_known_nodes:0\r\n\
                          cluster_size:0\r\n\
                          cluster_current_epoch:0\r\n\
                          cluster_my_epoch:0\r\n\
                          cluster_stats_messages_sent:0\r\n\
                          cluster_stats_messages_received:0\r\n\
                          total_cluster_links_buffer_limit_exceeded:0"
                        .to_vec(),
                ))),
                "MYID" => Ok(RespValue::BulkString(Some(
                    b"0000000000000000000000000000000000000000".to_vec(),
                ))),
                "KEYSLOT" => {
                    if args.len() < 2 {
                        return Err(CommandError::WrongNumberOfArguments);
                    }
                    let slot = key_hash_slot(&args[1]);
                    Ok(RespValue::Integer(slot as i64))
                }
                "HELP" => Ok(cluster_help()),
                _ => Ok(RespValue::Error(
                    "ERR This instance has cluster support disabled".to_string(),
                )),
            };
        }
    };

    match subcommand.as_str() {
        "INFO" => cluster_info(mgr).await,
        "NODES" => cluster_nodes(mgr).await,
        "SLOTS" => cluster_slots(mgr).await,
        "SHARDS" => cluster_shards(mgr).await,
        "MYID" => cluster_myid(mgr).await,
        "MEET" => {
            if args.len() < 3 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let host = String::from_utf8_lossy(&args[1]).to_string();
            let port: u16 = String::from_utf8_lossy(&args[2])
                .parse()
                .map_err(|_| CommandError::InvalidArgument("Invalid port".to_string()))?;
            cluster_meet(mgr, &host, port).await
        }
        "FORGET" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let node_id = String::from_utf8_lossy(&args[1]).to_string();
            cluster_forget(mgr, &node_id).await
        }
        "REPLICATE" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let master_id = String::from_utf8_lossy(&args[1]).to_string();
            cluster_replicate(mgr, &master_id).await
        }
        "RESET" => {
            let hard = args
                .get(1)
                .map(|a| String::from_utf8_lossy(a).to_uppercase() == "HARD")
                .unwrap_or(false);
            cluster_reset(mgr, hard).await
        }
        "ADDSLOTS" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let slots: Result<Vec<u16>, _> = args[1..]
                .iter()
                .map(|a| {
                    String::from_utf8_lossy(a)
                        .parse::<u16>()
                        .map_err(|_| CommandError::InvalidArgument("Invalid slot".to_string()))
                })
                .collect();
            cluster_addslots(mgr, &slots?).await
        }
        "DELSLOTS" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let slots: Result<Vec<u16>, _> = args[1..]
                .iter()
                .map(|a| {
                    String::from_utf8_lossy(a)
                        .parse::<u16>()
                        .map_err(|_| CommandError::InvalidArgument("Invalid slot".to_string()))
                })
                .collect();
            cluster_delslots(mgr, &slots?).await
        }
        "FLUSHSLOTS" => cluster_flushslots(mgr).await,
        "SETSLOT" => {
            if args.len() < 3 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let slot: u16 = String::from_utf8_lossy(&args[1])
                .parse()
                .map_err(|_| CommandError::InvalidArgument("Invalid slot".to_string()))?;
            let subcmd = String::from_utf8_lossy(&args[2]).to_uppercase();
            let node_id = args.get(3).map(|a| String::from_utf8_lossy(a).to_string());
            cluster_setslot(mgr, slot, &subcmd, node_id.as_deref()).await
        }
        "COUNTKEYSINSLOT" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let slot: u16 = String::from_utf8_lossy(&args[1])
                .parse()
                .map_err(|_| CommandError::InvalidArgument("Invalid slot".to_string()))?;
            cluster_countkeysinslot(mgr, slot).await
        }
        "GETKEYSINSLOT" => {
            if args.len() < 3 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let slot: u16 = String::from_utf8_lossy(&args[1])
                .parse()
                .map_err(|_| CommandError::InvalidArgument("Invalid slot".to_string()))?;
            let count: usize = String::from_utf8_lossy(&args[2])
                .parse()
                .map_err(|_| CommandError::InvalidArgument("Invalid count".to_string()))?;
            cluster_getkeysinslot(mgr, slot, count).await
        }
        "KEYSLOT" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            Ok(RespValue::Integer(key_hash_slot(&args[1]) as i64))
        }
        "FAILOVER" => {
            let force = args
                .get(1)
                .map(|a| String::from_utf8_lossy(a).to_uppercase() == "FORCE")
                .unwrap_or(false);
            cluster_failover(mgr, force).await
        }
        "SAVECONFIG" => cluster_saveconfig(mgr).await,
        "SET-CONFIG-EPOCH" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let epoch: u64 = String::from_utf8_lossy(&args[1])
                .parse()
                .map_err(|_| CommandError::InvalidArgument("Invalid config epoch".to_string()))?;
            cluster_set_config_epoch(mgr, epoch).await
        }
        "COUNT-FAILURE-REPORTS" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let node_id = String::from_utf8_lossy(&args[1]).to_string();
            cluster_count_failure_reports(mgr, &node_id).await
        }
        "LINKS" => cluster_links(mgr).await,
        "HELP" => Ok(cluster_help()),
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown subcommand or wrong number of arguments for '{}' command",
            subcommand
        ))),
    }
}

// --- CLUSTER subcommand implementations ---

async fn cluster_info(mgr: &ClusterManager) -> CommandResult {
    let info = mgr.get_cluster_info().await;
    Ok(RespValue::BulkString(Some(info.into_bytes())))
}

async fn cluster_nodes(mgr: &ClusterManager) -> CommandResult {
    let nodes = mgr.get_cluster_nodes().await;
    Ok(RespValue::BulkString(Some(nodes.into_bytes())))
}

async fn cluster_slots(mgr: &ClusterManager) -> CommandResult {
    let slots = mgr.get_cluster_slots().await;
    let mut result = Vec::new();

    for (start, end, nodes) in slots {
        let mut entry = vec![
            RespValue::Integer(start as i64),
            RespValue::Integer(end as i64),
        ];

        for (ip, port, id) in nodes {
            entry.push(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(ip.into_bytes())),
                RespValue::Integer(port as i64),
                RespValue::BulkString(Some(id.into_bytes())),
            ])));
        }

        result.push(RespValue::Array(Some(entry)));
    }

    Ok(RespValue::Array(Some(result)))
}

async fn cluster_shards(mgr: &ClusterManager) -> CommandResult {
    let shards = mgr.get_cluster_shards().await;
    let mut result = Vec::new();

    for shard in shards {
        let mut shard_map = Vec::new();

        // Slots
        shard_map.push(RespValue::BulkString(Some(b"slots".to_vec())));
        let mut slot_ranges = Vec::new();
        for range in &shard.slots {
            slot_ranges.push(RespValue::Integer(range.start as i64));
            slot_ranges.push(RespValue::Integer(range.end as i64));
        }
        shard_map.push(RespValue::Array(Some(slot_ranges)));

        // Nodes
        shard_map.push(RespValue::BulkString(Some(b"nodes".to_vec())));
        let mut node_list = Vec::new();
        for node in &shard.nodes {
            node_list.push(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"id".to_vec())),
                RespValue::BulkString(Some(node.id.clone().into_bytes())),
                RespValue::BulkString(Some(b"port".to_vec())),
                RespValue::Integer(node.port as i64),
                RespValue::BulkString(Some(b"ip".to_vec())),
                RespValue::BulkString(Some(node.ip.clone().into_bytes())),
                RespValue::BulkString(Some(b"role".to_vec())),
                RespValue::BulkString(Some(node.role.clone().into_bytes())),
                RespValue::BulkString(Some(b"health".to_vec())),
                RespValue::BulkString(Some(node.health.clone().into_bytes())),
                RespValue::BulkString(Some(b"replication-offset".to_vec())),
                RespValue::Integer(node.replication_offset as i64),
            ])));
        }
        shard_map.push(RespValue::Array(Some(node_list)));

        result.push(RespValue::Array(Some(shard_map)));
    }

    Ok(RespValue::Array(Some(result)))
}

async fn cluster_myid(mgr: &ClusterManager) -> CommandResult {
    let id = mgr.my_id().await;
    Ok(RespValue::BulkString(Some(id.into_bytes())))
}

async fn cluster_meet(mgr: &ClusterManager, host: &str, port: u16) -> CommandResult {
    mgr.meet(host, port)
        .await
        .map_err(CommandError::InternalError)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

async fn cluster_forget(mgr: &ClusterManager, node_id: &str) -> CommandResult {
    mgr.forget(node_id)
        .await
        .map_err(CommandError::InternalError)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

async fn cluster_replicate(mgr: &ClusterManager, master_id: &str) -> CommandResult {
    mgr.replicate(master_id)
        .await
        .map_err(CommandError::InternalError)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

async fn cluster_reset(mgr: &ClusterManager, hard: bool) -> CommandResult {
    mgr.reset(hard).await;
    Ok(RespValue::SimpleString("OK".to_string()))
}

async fn cluster_addslots(mgr: &ClusterManager, slots: &[u16]) -> CommandResult {
    mgr.add_slots(slots)
        .await
        .map_err(CommandError::InternalError)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

async fn cluster_delslots(mgr: &ClusterManager, slots: &[u16]) -> CommandResult {
    mgr.del_slots(slots)
        .await
        .map_err(CommandError::InternalError)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

async fn cluster_flushslots(mgr: &ClusterManager) -> CommandResult {
    mgr.flush_slots().await;
    Ok(RespValue::SimpleString("OK".to_string()))
}

async fn cluster_setslot(
    mgr: &ClusterManager,
    slot: u16,
    subcmd: &str,
    node_id: Option<&str>,
) -> CommandResult {
    mgr.set_slot(slot, subcmd, node_id)
        .await
        .map_err(CommandError::InternalError)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

async fn cluster_countkeysinslot(mgr: &ClusterManager, slot: u16) -> CommandResult {
    let count = mgr
        .count_keys_in_slot(slot)
        .await
        .map_err(CommandError::InternalError)?;
    Ok(RespValue::Integer(count))
}

async fn cluster_getkeysinslot(mgr: &ClusterManager, slot: u16, count: usize) -> CommandResult {
    let keys = mgr
        .get_keys_in_slot(slot, count)
        .await
        .map_err(CommandError::InternalError)?;
    let resp_keys: Vec<RespValue> = keys
        .into_iter()
        .map(|k| RespValue::BulkString(Some(k)))
        .collect();
    Ok(RespValue::Array(Some(resp_keys)))
}

async fn cluster_failover(mgr: &ClusterManager, force: bool) -> CommandResult {
    mgr.failover(force)
        .await
        .map_err(CommandError::InternalError)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

async fn cluster_saveconfig(mgr: &ClusterManager) -> CommandResult {
    mgr.save_config()
        .await
        .map_err(CommandError::InternalError)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

async fn cluster_set_config_epoch(mgr: &ClusterManager, epoch: u64) -> CommandResult {
    mgr.set_config_epoch(epoch)
        .await
        .map_err(CommandError::InternalError)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

async fn cluster_count_failure_reports(mgr: &ClusterManager, node_id: &str) -> CommandResult {
    let state = mgr.state().read().await;
    let count = state
        .nodes
        .get(node_id)
        .map(|n| n.count_failure_reports())
        .unwrap_or(0);
    Ok(RespValue::Integer(count as i64))
}

async fn cluster_links(mgr: &ClusterManager) -> CommandResult {
    let links = mgr.get_cluster_links().await;
    let mut result = Vec::new();

    for link in links {
        result.push(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"direction".to_vec())),
            RespValue::BulkString(Some(link.direction.into_bytes())),
            RespValue::BulkString(Some(b"node".to_vec())),
            RespValue::BulkString(Some(link.node_id.into_bytes())),
            RespValue::BulkString(Some(b"create-time".to_vec())),
            RespValue::Integer(link.create_time as i64),
            RespValue::BulkString(Some(b"events".to_vec())),
            RespValue::BulkString(Some(link.events.into_bytes())),
            RespValue::BulkString(Some(b"send-buffer-allocated".to_vec())),
            RespValue::Integer(link.send_buffer_allocated as i64),
            RespValue::BulkString(Some(b"send-buffer-used".to_vec())),
            RespValue::Integer(link.send_buffer_used as i64),
        ])));
    }

    Ok(RespValue::Array(Some(result)))
}

fn cluster_help() -> RespValue {
    let help_lines: Vec<RespValue> = vec![
        "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "ADDSLOTS <slot> [<slot> ...] -- Assign slots to current node.",
        "COUNTKEYSINSLOT <slot> -- Return the number of keys in <slot>.",
        "DELSLOTS <slot> [<slot> ...] -- Delete slots from current node.",
        "FAILOVER [FORCE] -- Promote current replica node to master.",
        "FLUSHSLOTS -- Delete current node's slots information.",
        "FORGET <node-id> -- Remove a node from the cluster.",
        "GETKEYSINSLOT <slot> <count> -- Return keys in <slot>.",
        "HELP -- Print this help message.",
        "INFO -- Return cluster information.",
        "KEYSLOT <key> -- Return the hash slot for <key>.",
        "LINKS -- Return information about cluster connections.",
        "MEET <ip> <port> -- Connect nodes into a working cluster.",
        "MYID -- Return the node id.",
        "NODES -- Return cluster configuration in nodes format.",
        "REPLICATE <node-id> -- Configure current node as replica of <node-id>.",
        "RESET [HARD|SOFT] -- Reset current node (default: SOFT).",
        "SAVECONFIG -- Force saving cluster state on disk.",
        "SET-CONFIG-EPOCH <epoch> -- Set config epoch.",
        "SETSLOT <slot> (IMPORTING|MIGRATING|NODE|STABLE) [<node-id>] -- Set slot state.",
        "SHARDS -- Return information about cluster shards.",
        "SLOTS -- Return information about slots assignment.",
        "COUNT-FAILURE-REPORTS <node-id> -- Return failure reports count.",
    ]
    .into_iter()
    .map(|s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
    .collect();

    RespValue::Array(Some(help_lines))
}

/// Handle ASKING command - sets per-connection flag for ASK redirect handling
pub async fn asking(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    // The ASKING flag is handled at the server/connection level
    // We just return OK here; the server.rs will track the state
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Handle READONLY command - allows reading from replica nodes in cluster mode
pub async fn readonly(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    // The READONLY mode is handled at the server/connection level
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Handle READWRITE command - disallow reading from replica (default behavior)
pub async fn readwrite(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Handle MIGRATE command
pub async fn migrate(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    // MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS key [key ...]]
    if args.len() < 5 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let host = String::from_utf8_lossy(&args[0]).to_string();
    let port: u16 = String::from_utf8_lossy(&args[1])
        .parse()
        .map_err(|_| CommandError::InvalidArgument("Invalid port".to_string()))?;
    let key_or_empty = &args[2];
    let db: u16 = String::from_utf8_lossy(&args[3])
        .parse()
        .map_err(|_| CommandError::InvalidArgument("Invalid db".to_string()))?;
    let timeout_ms: u64 = String::from_utf8_lossy(&args[4])
        .parse()
        .map_err(|_| CommandError::InvalidArgument("Invalid timeout".to_string()))?;

    let mut copy = false;
    let mut replace = false;
    let mut auth: Option<String> = None;
    let mut auth2: Option<(String, String)> = None;
    let mut extra_keys: Vec<Vec<u8>> = Vec::new();
    let mut i = 5;

    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "COPY" => copy = true,
            "REPLACE" => replace = true,
            "AUTH" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                auth = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                i += 1;
            }
            "AUTH2" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                auth2 = Some((
                    String::from_utf8_lossy(&args[i + 1]).to_string(),
                    String::from_utf8_lossy(&args[i + 2]).to_string(),
                ));
                i += 2;
            }
            "KEYS" => {
                // Collect remaining args as keys
                for arg in args.iter().skip(i + 1) {
                    extra_keys.push(arg.clone());
                }
                break;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unrecognized MIGRATE option: {}",
                    opt
                )));
            }
        }
        i += 1;
    }

    // Determine keys to migrate
    let mut keys = Vec::new();
    if !key_or_empty.is_empty() {
        keys.push(key_or_empty.clone());
    }
    keys.extend(extra_keys);

    if keys.is_empty() {
        return Ok(RespValue::SimpleString("NOKEY".to_string()));
    }

    let auth_ref = auth.as_deref();
    let auth2_ref = auth2.as_ref().map(|(u, p)| (u.as_str(), p.as_str()));

    match crate::cluster::migration::migrate_keys(
        engine, &host, port, &keys, db, timeout_ms, copy, replace, auth_ref, auth2_ref,
    )
    .await
    {
        Ok(resp) => Ok(resp),
        Err(e) => Ok(RespValue::Error(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::slot::CLUSTER_SLOTS;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_cluster_info_no_cluster() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        let result = cluster_command(&storage, &[b"INFO".to_vec()], None)
            .await
            .unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let info = String::from_utf8(data).unwrap();
            assert!(info.contains("cluster_enabled:0"));
        } else {
            panic!("Expected BulkString");
        }
    }

    #[tokio::test]
    async fn test_cluster_keyslot_no_cluster() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        let result = cluster_command(&storage, &[b"KEYSLOT".to_vec(), b"foo".to_vec()], None)
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(12182));
    }

    #[tokio::test]
    async fn test_cluster_help() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        let result = cluster_command(&storage, &[b"HELP".to_vec()], None)
            .await
            .unwrap();
        if let RespValue::Array(Some(lines)) = result {
            assert!(!lines.is_empty());
        } else {
            panic!("Expected Array");
        }
    }

    #[tokio::test]
    async fn test_cluster_info_with_manager() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = crate::cluster::ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let mgr = ClusterManager::new(Arc::clone(&storage), cluster_config);
        let addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
        mgr.init(addr).await;

        let result = cluster_command(&storage, &[b"INFO".to_vec()], Some(&mgr))
            .await
            .unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let info = String::from_utf8(data).unwrap();
            assert!(info.contains("cluster_enabled:1"));
        } else {
            panic!("Expected BulkString");
        }
    }

    #[tokio::test]
    async fn test_cluster_myid_with_manager() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = crate::cluster::ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let mgr = ClusterManager::new(Arc::clone(&storage), cluster_config);
        let addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
        mgr.init(addr).await;

        let result = cluster_command(&storage, &[b"MYID".to_vec()], Some(&mgr))
            .await
            .unwrap();
        if let RespValue::BulkString(Some(id)) = result {
            assert_eq!(id.len(), 40);
        } else {
            panic!("Expected BulkString");
        }
    }

    #[tokio::test]
    async fn test_cluster_addslots_and_info() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = crate::cluster::ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let mgr = ClusterManager::new(Arc::clone(&storage), cluster_config);
        let addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
        mgr.init(addr).await;

        // Add all slots
        let mut slot_args: Vec<Vec<u8>> = vec![b"ADDSLOTS".to_vec()];
        for i in 0..CLUSTER_SLOTS {
            slot_args.push(format!("{}", i).into_bytes());
        }

        let result = cluster_command(&storage, &slot_args, Some(&mgr))
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Check info shows all slots assigned
        let result = cluster_command(&storage, &[b"INFO".to_vec()], Some(&mgr))
            .await
            .unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let info = String::from_utf8(data).unwrap();
            assert!(info.contains("cluster_state:ok"));
            assert!(info.contains("cluster_slots_assigned:16384"));
        }
    }

    #[tokio::test]
    async fn test_cluster_meet_and_nodes() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = crate::cluster::ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let mgr = ClusterManager::new(Arc::clone(&storage), cluster_config);
        let addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
        mgr.init(addr).await;

        // Meet another node
        let result = cluster_command(
            &storage,
            &[b"MEET".to_vec(), b"127.0.0.1".to_vec(), b"6380".to_vec()],
            Some(&mgr),
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Check nodes output
        let result = cluster_command(&storage, &[b"NODES".to_vec()], Some(&mgr))
            .await
            .unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let nodes = String::from_utf8(data).unwrap();
            assert!(nodes.contains("myself,master"));
            assert!(nodes.lines().count() >= 2);
        }
    }

    #[tokio::test]
    async fn test_asking_readonly_readwrite() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);

        assert_eq!(
            asking(&storage, &[]).await.unwrap(),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            readonly(&storage, &[]).await.unwrap(),
            RespValue::SimpleString("OK".to_string())
        );
        assert_eq!(
            readwrite(&storage, &[]).await.unwrap(),
            RespValue::SimpleString("OK".to_string())
        );
    }

    #[tokio::test]
    async fn test_cluster_countkeysinslot() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = crate::cluster::ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let mgr = ClusterManager::new(Arc::clone(&storage), cluster_config);
        let addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
        mgr.init(addr).await;

        // Store a key - "foo" hashes to slot 12182
        storage
            .set(b"foo".to_vec(), b"bar".to_vec(), None)
            .await
            .unwrap();

        let result = cluster_command(
            &storage,
            &[b"COUNTKEYSINSLOT".to_vec(), b"12182".to_vec()],
            Some(&mgr),
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Empty slot
        let result = cluster_command(
            &storage,
            &[b"COUNTKEYSINSLOT".to_vec(), b"0".to_vec()],
            Some(&mgr),
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_cluster_getkeysinslot() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = crate::cluster::ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let mgr = ClusterManager::new(Arc::clone(&storage), cluster_config);
        let addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
        mgr.init(addr).await;

        storage
            .set(b"foo".to_vec(), b"bar".to_vec(), None)
            .await
            .unwrap();

        let result = cluster_command(
            &storage,
            &[b"GETKEYSINSLOT".to_vec(), b"12182".to_vec(), b"10".to_vec()],
            Some(&mgr),
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(keys)) = result {
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RespValue::BulkString(Some(b"foo".to_vec())));
        } else {
            panic!("Expected Array");
        }
    }

    #[tokio::test]
    async fn test_cluster_setslot_importing_migrating() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = crate::cluster::ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let mgr = ClusterManager::new(Arc::clone(&storage), cluster_config);
        let addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
        mgr.init(addr).await;

        // Meet another node first
        mgr.meet("127.0.0.1", 6380).await.unwrap();

        // Get the other node's ID
        let state = mgr.state().read().await;
        let my_id = state.my_id.clone();
        let other_id = state.nodes.keys().find(|k| **k != my_id).unwrap().clone();
        drop(state);

        // Add slot 100 to ourselves first
        mgr.add_slots(&[100]).await.unwrap();

        // Set slot as MIGRATING to other node
        let result = cluster_command(
            &storage,
            &[
                b"SETSLOT".to_vec(),
                b"100".to_vec(),
                b"MIGRATING".to_vec(),
                other_id.as_bytes().to_vec(),
            ],
            Some(&mgr),
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Set slot as STABLE
        let result = cluster_command(
            &storage,
            &[b"SETSLOT".to_vec(), b"100".to_vec(), b"STABLE".to_vec()],
            Some(&mgr),
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_cluster_reset_hard() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = crate::cluster::ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let mgr = ClusterManager::new(Arc::clone(&storage), cluster_config);
        let addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
        mgr.init(addr).await;

        let old_id = mgr.my_id().await;

        let result = cluster_command(&storage, &[b"RESET".to_vec(), b"HARD".to_vec()], Some(&mgr))
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let new_id = mgr.my_id().await;
        assert_ne!(old_id, new_id);
    }

    #[tokio::test]
    async fn test_cluster_slots_output() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = crate::cluster::ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let mgr = ClusterManager::new(Arc::clone(&storage), cluster_config);
        let addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
        mgr.init(addr).await;

        // Add some slots
        let slots: Vec<u16> = (0..100).collect();
        mgr.add_slots(&slots).await.unwrap();

        let result = cluster_command(&storage, &[b"SLOTS".to_vec()], Some(&mgr))
            .await
            .unwrap();
        if let RespValue::Array(Some(entries)) = result {
            assert!(!entries.is_empty());
        } else {
            panic!("Expected Array for CLUSTER SLOTS");
        }
    }

    #[tokio::test]
    async fn test_cluster_set_config_epoch() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = crate::cluster::ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let mgr = ClusterManager::new(Arc::clone(&storage), cluster_config);
        let addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
        mgr.init(addr).await;

        let result = cluster_command(
            &storage,
            &[b"SET-CONFIG-EPOCH".to_vec(), b"5".to_vec()],
            Some(&mgr),
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Second call should fail (epoch already set)
        let result = cluster_command(
            &storage,
            &[b"SET-CONFIG-EPOCH".to_vec(), b"10".to_vec()],
            Some(&mgr),
        )
        .await;
        assert!(result.is_err() || matches!(result, Ok(RespValue::Error(_))));
    }
}
