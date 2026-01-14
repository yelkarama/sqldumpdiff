// Progress bar management using indicatif.
// We keep all bars under one MultiProgress so they render on separate lines.

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::fs;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct ProgressManager {
    multi: Option<Arc<MultiProgress>>,
}

impl ProgressManager {
    // Create a new manager. If enabled=false, no bars are created.
    pub fn new(enabled: bool) -> Self {
        let multi = if enabled {
            Some(Arc::new(MultiProgress::new()))
        } else {
            None
        };
        Self { multi }
    }

    // Create a bar for file-byte progress with a label.
    pub fn new_file_bar(&self, path: &str, label: &str) -> Option<ProgressBar> {
        let mp = self.multi.as_ref()?;
        let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let bar = mp.add(ProgressBar::new(size));
        bar.set_style(progress_style());
        bar.set_prefix(label.to_string());
        Some(bar)
    }

    // Helper for parsing bar (used by split step).
    pub fn new_parse_bar(&self, path: &str, label: &str) -> Option<ProgressBar> {
        self.new_file_bar(path, label)
    }

    // Create a bar for comparing table counts.
    pub fn new_table_bar(&self, total: u64) -> Option<ProgressBar> {
        let mp = self.multi.as_ref()?;
        let bar = mp.add(ProgressBar::new(total));
        bar.set_style(table_style());
        bar.set_prefix("Comparing tables".to_string());
        Some(bar)
    }
}

fn progress_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{prefix:20} {bytes:>10}/{total_bytes:<10} [{bar:67}] {percent:>3}%",
    )
    .unwrap()
    .progress_chars("█ ")
}

// Alternative style for non-byte counters (table counts).
fn table_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{prefix:20} {pos:>5}/{len:<5} [{bar:67}] {percent:>3}%",
    )
    .unwrap()
    .progress_chars("█ ")
}

// Convenience for labels using filename only.
#[allow(dead_code)]
fn basename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(path)
        .to_string()
}
