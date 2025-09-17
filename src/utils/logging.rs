use std::env;
use tracing::{info, warn, error, debug, Level, enabled};

/// Log system information useful for debugging
#[allow(dead_code)]
pub fn log_system_info() {
    info!(os = %env::consts::OS, arch = %env::consts::ARCH, "System Info");
    info!(physical_cores = num_cpus::get_physical(), "CPU Info");

    // Log available memory if possible
    let mem_mb: Option<u64> = {
        #[cfg(target_os = "linux")]
        {
            if let Ok(mem_info) = std::fs::read_to_string("/proc/meminfo") {
                if let Some(line) = mem_info.lines().find(|l| l.starts_with("MemTotal:")) {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(mem_kb) = parts[1].parse::<u64>() {
                            Some(mem_kb / 1024)
                        } else { None }
                    } else { None }
                } else { None }
            } else { None }
        }
        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            if let Ok(output) = Command::new("sysctl").args(["-n", "hw.memsize"]).output() {
                if let Ok(mem_bytes) = String::from_utf8_lossy(&output.stdout).trim().parse::<u64>() {
                    Some(mem_bytes / 1024 / 1024)
                } else { None }
            } else { None }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        { None }
    };

    if let Some(mem) = mem_mb {
        info!(system_memory_mb = mem, "Memory Info");
    }
}

/// Log memory stats from the storage engine
#[allow(dead_code)]
pub fn log_memory_stats(current_memory: usize, max_memory: usize) {
    let mem_mb = current_memory as f64 / 1024.0 / 1024.0;
    let max_mem_mb = max_memory as f64 / 1024.0 / 1024.0;
    let percentage = if max_memory > 0 {
        (current_memory as f64 / max_memory as f64) * 100.0
    } else {
        0.0 // Avoid division by zero if max_memory is not set
    };

    // Use structured logging fields
    let memory_info = format!("{:.2} MB / {:.2} MB ({:.1}%)", mem_mb, max_mem_mb, percentage);

    if percentage > 80.0 {
        warn!(current_mb = mem_mb, max_mb = max_mem_mb, usage_percent = percentage, "High memory usage: {}", memory_info);
    } else {
        info!(current_mb = mem_mb, max_mb = max_mem_mb, usage_percent = percentage, "Memory usage: {}", memory_info);
    }
}

/// Create an error log message and return the error
/// Useful for logging an error and returning it in a ? operation
#[allow(dead_code)]
pub fn log_error<E: std::fmt::Display>(err: E, context: &str) -> E {
    error!(context = context, error = %err, "Error occurred");
    err
}

/// Log request-response data when debug level is enabled
pub fn log_protocol_data(client_addr: &str, direction: &str, data: &[u8], is_raw: bool) {
    // Only log when debug level is enabled
    if !enabled!(Level::DEBUG) {
        return;
    }
    
    // Determine if we should log the full content or just the size
    const MAX_LOG_SIZE: usize = 1024; // Only log up to 1KB in regular debug mode
    
    if is_raw {
        // For raw data, just log the size and first few bytes as hex
        let preview_size = std::cmp::min(data.len(), 32); // First 32 bytes
        let preview_hex = data[..preview_size]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<String>>()
            .join(" ");
        
        debug!(
            client_addr = %client_addr,
            direction = %direction,
            size = %data.len(),
            preview = %preview_hex,
            "Protocol data (raw)"
        );
    } else if data.len() > MAX_LOG_SIZE {
        // For large data, log the size and a preview
        let truncated_data = String::from_utf8_lossy(&data[..MAX_LOG_SIZE]);
        let preview = format!("{} [truncated {} bytes]", truncated_data, data.len() - MAX_LOG_SIZE);
        
        debug!(
            client_addr = %client_addr,
            direction = %direction,
            size = %data.len(),
            data = %preview,
            "Protocol data"
        );
    } else {
        // For reasonably sized data, log the full content
        let data_str = String::from_utf8_lossy(data);
        
        debug!(
            client_addr = %client_addr,
            direction = %direction,
            size = %data.len(),
            data = %data_str,
            "Protocol data"
        );
    }
} 