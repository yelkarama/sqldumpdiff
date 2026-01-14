// Simple structured logger with DEBUG/INFO/ERROR levels.
// We intentionally keep this small and dependency-free to make it easy
// to understand for Rust beginners.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// Log level values are ordered (Info < Debug) for easy comparisons.
const INFO_LEVEL: u8 = 0;
const DEBUG_LEVEL: u8 = 1;

static LOG_LEVEL: AtomicU8 = AtomicU8::new(INFO_LEVEL);

// Set the global log level based on the --debug flag.
pub fn set_debug(enabled: bool) {
    if enabled {
        LOG_LEVEL.store(DEBUG_LEVEL, Ordering::Relaxed);
    } else {
        LOG_LEVEL.store(INFO_LEVEL, Ordering::Relaxed);
    }
}

// Returns true if debug logging is enabled.
pub fn is_debug() -> bool {
    LOG_LEVEL.load(Ordering::Relaxed) >= DEBUG_LEVEL
}

// Print an INFO-level message.
#[allow(dead_code)]
pub fn info(msg: &str) {
    log_line("INFO", msg);
}

// Print a DEBUG-level message if enabled.
pub fn debug(msg: &str) {
    if is_debug() {
        log_line("DEBUG", msg);
    }
}

// Print an ERROR-level message.
pub fn error(msg: &str) {
    log_line("ERROR", msg);
}

// Helper to format log lines with a timestamp and short label.
fn log_line(level: &str, msg: &str) {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    eprintln!("[{}] {} {}", level, ts, msg);
}
