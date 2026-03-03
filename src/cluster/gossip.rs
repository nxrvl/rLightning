use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use super::{ClusterManager, LinkState, NodeRole};

/// Message types for the cluster bus protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ClusterMessageType {
    Ping = 0,
    Pong = 1,
    Meet = 2,
    Fail = 3,
    Publish = 4,
    Failover = 5,
    Update = 6,
}

impl ClusterMessageType {
    fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(Self::Ping),
            1 => Some(Self::Pong),
            2 => Some(Self::Meet),
            3 => Some(Self::Fail),
            4 => Some(Self::Publish),
            5 => Some(Self::Failover),
            6 => Some(Self::Update),
            _ => None,
        }
    }
}

/// A simplified cluster bus message
#[derive(Debug, Clone)]
pub struct ClusterMessage {
    pub msg_type: ClusterMessageType,
    pub sender_id: String,
    pub sender_addr: String,
    pub sender_port: u16,
    pub sender_bus_port: u16,
    pub sender_role: NodeRole,
    pub current_epoch: u64,
    pub config_epoch: u64,
    /// Gossip entries about other nodes
    pub gossip: Vec<GossipEntry>,
}

/// Gossip information about a node
#[derive(Debug, Clone)]
pub struct GossipEntry {
    pub node_id: String,
    pub addr: String,
    pub port: u16,
    pub bus_port: u16,
    pub flags: u16,
    pub ping_sent: u64,
    pub pong_received: u64,
}

impl ClusterMessage {
    /// Serialize message to bytes (simplified protocol)
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();

        // Header: message type (1 byte)
        data.push(self.msg_type as u8);

        // Sender ID (40 bytes, padded, truncated if longer)
        let id_bytes = self.sender_id.as_bytes();
        let id_len = id_bytes.len().min(40);
        data.extend_from_slice(&id_bytes[..id_len]);
        data.extend(std::iter::repeat_n(0u8, 40 - id_len));

        // Sender address (variable length, null-terminated)
        data.extend_from_slice(self.sender_addr.as_bytes());
        data.push(0);

        // Sender port (2 bytes, big-endian)
        data.extend_from_slice(&self.sender_port.to_be_bytes());

        // Sender bus port (2 bytes, big-endian)
        data.extend_from_slice(&self.sender_bus_port.to_be_bytes());

        // Sender role (1 byte)
        data.push(match self.sender_role {
            NodeRole::Master => 0,
            NodeRole::Replica => 1,
        });

        // Current epoch (8 bytes, big-endian)
        data.extend_from_slice(&self.current_epoch.to_be_bytes());

        // Config epoch (8 bytes, big-endian)
        data.extend_from_slice(&self.config_epoch.to_be_bytes());

        // Gossip entries count (2 bytes, big-endian)
        data.extend_from_slice(&(self.gossip.len() as u16).to_be_bytes());

        // Gossip entries
        for entry in &self.gossip {
            let eid = entry.node_id.as_bytes();
            let eid_len = eid.len().min(40);
            data.extend_from_slice(&eid[..eid_len]);
            data.extend(std::iter::repeat_n(0u8, 40 - eid_len));
            data.extend_from_slice(entry.addr.as_bytes());
            data.push(0);
            data.extend_from_slice(&entry.port.to_be_bytes());
            data.extend_from_slice(&entry.bus_port.to_be_bytes());
            data.extend_from_slice(&entry.flags.to_be_bytes());
            data.extend_from_slice(&entry.ping_sent.to_be_bytes());
            data.extend_from_slice(&entry.pong_received.to_be_bytes());
        }

        // Prepend total length (4 bytes, big-endian)
        let len = data.len() as u32;
        let mut result = len.to_be_bytes().to_vec();
        result.extend(data);
        result
    }

    /// Deserialize message from bytes
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        let mut pos = 0;

        // Message type
        let msg_type = ClusterMessageType::from_u8(data[pos])?;
        pos += 1;

        // Sender ID
        if pos + 40 > data.len() {
            return None;
        }
        let id_end = data[pos..pos + 40]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(40);
        let sender_id = String::from_utf8_lossy(&data[pos..pos + id_end]).to_string();
        pos += 40;

        // Sender address (null-terminated, max 256 bytes)
        let search_limit = std::cmp::min(data.len() - pos, 256);
        let addr_end = data[pos..pos + search_limit].iter().position(|&b| b == 0)?;
        let sender_addr = String::from_utf8_lossy(&data[pos..pos + addr_end]).to_string();
        pos += addr_end + 1;

        // Sender port
        if pos + 2 > data.len() {
            return None;
        }
        let sender_port = u16::from_be_bytes([data[pos], data[pos + 1]]);
        pos += 2;

        // Sender bus port
        if pos + 2 > data.len() {
            return None;
        }
        let sender_bus_port = u16::from_be_bytes([data[pos], data[pos + 1]]);
        pos += 2;

        // Sender role
        if pos >= data.len() {
            return None;
        }
        let sender_role = if data[pos] == 0 {
            NodeRole::Master
        } else {
            NodeRole::Replica
        };
        pos += 1;

        // Current epoch
        if pos + 8 > data.len() {
            return None;
        }
        let current_epoch = u64::from_be_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;

        // Config epoch
        if pos + 8 > data.len() {
            return None;
        }
        let config_epoch = u64::from_be_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;

        // Gossip entries count
        if pos + 2 > data.len() {
            return None;
        }
        let gossip_count = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        let mut gossip = Vec::with_capacity(gossip_count);
        for _ in 0..gossip_count {
            // Node ID
            if pos + 40 > data.len() {
                break;
            }
            let eid_end = data[pos..pos + 40]
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(40);
            let node_id = String::from_utf8_lossy(&data[pos..pos + eid_end]).to_string();
            pos += 40;

            // Address (null-terminated, max 256 bytes)
            if pos >= data.len() {
                break;
            }
            let search_limit = std::cmp::min(data.len() - pos, 256);
            let eaddr_end = match data[pos..pos + search_limit].iter().position(|&b| b == 0) {
                Some(end) => end,
                None => break, // Malformed: no null terminator found
            };
            let addr = String::from_utf8_lossy(&data[pos..pos + eaddr_end]).to_string();
            pos += eaddr_end + 1;

            if pos + 22 > data.len() {
                break;
            }
            let port = u16::from_be_bytes([data[pos], data[pos + 1]]);
            pos += 2;
            let bus_port = u16::from_be_bytes([data[pos], data[pos + 1]]);
            pos += 2;
            let flags = u16::from_be_bytes([data[pos], data[pos + 1]]);
            pos += 2;
            let ping_sent = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap_or([0; 8]));
            pos += 8;
            let pong_received = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap_or([0; 8]));
            pos += 8;

            gossip.push(GossipEntry {
                node_id,
                addr,
                port,
                bus_port,
                flags,
                ping_sent,
                pong_received,
            });
        }

        Some(ClusterMessage {
            msg_type,
            sender_id,
            sender_addr,
            sender_port,
            sender_bus_port,
            sender_role,
            current_epoch,
            config_epoch,
            gossip,
        })
    }
}

/// Start the cluster bus listener on port+10000
pub async fn start_cluster_bus(manager: Arc<ClusterManager>, addr: SocketAddr) {
    let bus_port = addr.port() + 10000;
    let bus_addr = SocketAddr::new(addr.ip(), bus_port);

    let listener = match TcpListener::bind(bus_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind cluster bus on {}: {}", bus_addr, e);
            return;
        }
    };

    info!("Cluster bus listening on {}", bus_addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let mgr = Arc::clone(&manager);
                tokio::spawn(async move {
                    handle_cluster_bus_connection(mgr, stream, peer).await;
                });
            }
            Err(e) => {
                warn!("Cluster bus accept error: {}", e);
            }
        }
    }
}

/// Handle an incoming cluster bus connection
async fn handle_cluster_bus_connection(
    manager: Arc<ClusterManager>,
    mut stream: TcpStream,
    peer: SocketAddr,
) {
    debug!("Cluster bus connection from {}", peer);

    let mut buf = vec![0u8; 65536];

    while stream.read_exact(&mut buf[..4]).await.is_ok() {
        let msg_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if msg_len > buf.len() {
            warn!("Cluster bus message too large: {}", msg_len);
            break;
        }

        if stream.read_exact(&mut buf[..msg_len]).await.is_err() {
            break;
        }

        if let Some(msg) = ClusterMessage::deserialize(&buf[..msg_len]) {
            handle_cluster_message(&manager, &msg, &mut stream).await;
        }
    }
}

/// Process a received cluster message
async fn handle_cluster_message(
    manager: &ClusterManager,
    msg: &ClusterMessage,
    stream: &mut TcpStream,
) {
    match msg.msg_type {
        ClusterMessageType::Ping | ClusterMessageType::Meet => {
            // Update node info from the sender
            update_node_from_message(manager, msg).await;

            // Send PONG response
            let pong = create_pong_message(manager).await;
            let data = pong.serialize();
            let _ = stream.write_all(&data).await;
        }
        ClusterMessageType::Pong => {
            // Update node info from the sender
            update_node_from_message(manager, msg).await;
        }
        ClusterMessageType::Fail => {
            // Mark the reported node as FAIL
            if let Some(entry) = msg.gossip.first() {
                let mut state = manager.state().write().await;
                if let Some(node) = state.nodes.get_mut(&entry.node_id) {
                    node.flags.fail = true;
                    node.flags.pfail = false;
                    warn!(
                        "Node {} marked as FAIL (reported by {})",
                        entry.node_id, msg.sender_id
                    );
                }
            }
        }
        ClusterMessageType::Failover => {
            debug!("Received FAILOVER auth request from {}", msg.sender_id);
        }
        ClusterMessageType::Update => {
            // Config update - update epoch and slot assignments
            update_node_from_message(manager, msg).await;
        }
        ClusterMessageType::Publish => {
            // Cluster-wide pub/sub message
            debug!("Received cluster PUBLISH from {}", msg.sender_id);
        }
    }
}

/// Update node information from a received message
async fn update_node_from_message(manager: &ClusterManager, msg: &ClusterMessage) {
    let mut state = manager.state().write().await;

    // Update or add the sender node
    if let Some(node) = state.nodes.get_mut(&msg.sender_id) {
        node.last_seen = std::time::Instant::now();
        node.link_state = LinkState::Connected;
        node.flags.pfail = false;
        node.pong_received = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Update epoch if sender has higher epoch
        if msg.current_epoch > state.current_epoch {
            state.current_epoch = msg.current_epoch;
        }
    }

    // Process gossip entries about other nodes
    for entry in &msg.gossip {
        if entry.node_id == state.my_id {
            continue; // Skip gossip about ourselves
        }

        if let Some(node) = state.nodes.get_mut(&entry.node_id) {
            // Update known node
            if entry.pong_received > node.pong_received {
                node.pong_received = entry.pong_received;
            }
        }
        // We don't add unknown nodes from gossip automatically;
        // nodes must be explicitly introduced via CLUSTER MEET
    }
}

/// Create a PONG message from this node's state
async fn create_pong_message(manager: &ClusterManager) -> ClusterMessage {
    let state = manager.state().read().await;
    let my_id = state.my_id.clone();
    let my_node = state.nodes.get(&my_id);

    let (addr, port, bus_port, role, config_epoch) = if let Some(node) = my_node {
        (
            node.addr.ip().to_string(),
            node.addr.port(),
            node.bus_port,
            node.flags.role,
            node.config_epoch,
        )
    } else {
        ("0.0.0.0".to_string(), 0, 0, NodeRole::Master, 0)
    };

    // Include gossip about a few random known nodes
    let mut gossip = Vec::new();
    for (id, node) in &state.nodes {
        if id == &my_id {
            continue;
        }
        gossip.push(GossipEntry {
            node_id: id.clone(),
            addr: node.addr.ip().to_string(),
            port: node.addr.port(),
            bus_port: node.bus_port,
            flags: 0,
            ping_sent: node.ping_sent,
            pong_received: node.pong_received,
        });
        if gossip.len() >= 3 {
            break; // Limit gossip entries
        }
    }

    ClusterMessage {
        msg_type: ClusterMessageType::Pong,
        sender_id: my_id,
        sender_addr: addr,
        sender_port: port,
        sender_bus_port: bus_port,
        sender_role: role,
        current_epoch: state.current_epoch,
        config_epoch,
        gossip,
    }
}

/// Periodically send PING messages to other cluster nodes
pub async fn cluster_cron(manager: Arc<ClusterManager>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        interval.tick().await;

        // Check node timeouts
        manager.check_node_timeouts().await;

        // Try automatic failover if needed
        manager.try_automatic_failover().await;

        // Send PINGs to random nodes
        let state = manager.state().read().await;
        let my_id = state.my_id.clone();
        let nodes: Vec<(String, SocketAddr, u16)> = state
            .nodes
            .iter()
            .filter(|(id, _)| **id != my_id)
            .map(|(id, n)| (id.clone(), n.addr, n.bus_port))
            .collect();
        drop(state);

        for (node_id, addr, bus_port) in &nodes {
            let bus_addr = SocketAddr::new(addr.ip(), *bus_port);
            let mgr = Arc::clone(&manager);
            let nid = node_id.clone();
            tokio::spawn(async move {
                if let Err(e) = send_ping(&mgr, bus_addr).await {
                    debug!("Failed to PING node {}: {}", nid, e);
                }
            });
        }
    }
}

/// Send a PING message to a cluster node
async fn send_ping(
    manager: &ClusterManager,
    bus_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = manager.state().read().await;
    let my_id = state.my_id.clone();
    let my_node = state.nodes.get(&my_id);

    let (addr, port, bus_port, role, config_epoch) = if let Some(node) = my_node {
        (
            node.addr.ip().to_string(),
            node.addr.port(),
            node.bus_port,
            node.flags.role,
            node.config_epoch,
        )
    } else {
        return Ok(());
    };

    let msg = ClusterMessage {
        msg_type: ClusterMessageType::Ping,
        sender_id: my_id,
        sender_addr: addr,
        sender_port: port,
        sender_bus_port: bus_port,
        sender_role: role,
        current_epoch: state.current_epoch,
        config_epoch,
        gossip: Vec::new(),
    };

    drop(state);

    let timeout = Duration::from_millis(500);
    let mut stream = tokio::time::timeout(timeout, TcpStream::connect(bus_addr)).await??;

    let data = msg.serialize();
    tokio::time::timeout(timeout, stream.write_all(&data)).await??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_message_serialize_deserialize() {
        let msg = ClusterMessage {
            msg_type: ClusterMessageType::Ping,
            sender_id: "a".repeat(40),
            sender_addr: "127.0.0.1".to_string(),
            sender_port: 6379,
            sender_bus_port: 16379,
            sender_role: NodeRole::Master,
            current_epoch: 42,
            config_epoch: 10,
            gossip: vec![GossipEntry {
                node_id: "b".repeat(40),
                addr: "127.0.0.2".to_string(),
                port: 6380,
                bus_port: 16380,
                flags: 0,
                ping_sent: 1000,
                pong_received: 2000,
            }],
        };

        let data = msg.serialize();

        // Skip the 4-byte length prefix
        let deserialized = ClusterMessage::deserialize(&data[4..]).unwrap();

        assert_eq!(deserialized.msg_type, ClusterMessageType::Ping);
        assert_eq!(deserialized.sender_id, "a".repeat(40));
        assert_eq!(deserialized.sender_addr, "127.0.0.1");
        assert_eq!(deserialized.sender_port, 6379);
        assert_eq!(deserialized.sender_bus_port, 16379);
        assert_eq!(deserialized.current_epoch, 42);
        assert_eq!(deserialized.config_epoch, 10);
        assert_eq!(deserialized.gossip.len(), 1);
        assert_eq!(deserialized.gossip[0].node_id, "b".repeat(40));
    }

    #[test]
    fn test_message_type_from_u8() {
        assert_eq!(
            ClusterMessageType::from_u8(0),
            Some(ClusterMessageType::Ping)
        );
        assert_eq!(
            ClusterMessageType::from_u8(1),
            Some(ClusterMessageType::Pong)
        );
        assert_eq!(ClusterMessageType::from_u8(255), None);
    }
}
