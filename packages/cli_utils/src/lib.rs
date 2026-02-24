#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Shared CLI utilities for the crime map toolchain.
//!
//! Provides `indicatif`-backed progress bars behind the [`ProgressCallback`]
//! trait, plus [`init_logger`] which sets up `indicatif-log-bridge` so that
//! `log::info!` and friends are suspended while progress bars redraw.
//!
//! Any binary that calls [`init_logger()`] at startup gets full progress bar
//! support for free.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crime_map_source::progress::ProgressCallback;
use indicatif::{ProgressBar, ProgressStyle};

pub use indicatif::MultiProgress;

/// Formats a duration in seconds into a compact human-readable string.
///
/// Examples: `"45s"`, `"12m30s"`, `"2h15m"`, `"1d3h"`.
fn format_eta(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        let m = secs / 60;
        let s = secs % 60;
        if s == 0 {
            format!("{m}m")
        } else {
            format!("{m}m{s:02}s")
        }
    } else if secs < 86400 {
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        if m == 0 {
            format!("{h}h")
        } else {
            format!("{h}h{m:02}m")
        }
    } else {
        let d = secs / 86400;
        let h = (secs % 86400) / 3600;
        if h == 0 {
            format!("{d}d")
        } else {
            format!("{d}d{h}h")
        }
    }
}

/// An `indicatif` [`ProgressBar`] that implements [`ProgressCallback`].
///
/// Uses a simple linear ETA projection (`elapsed * remaining / completed`)
/// instead of `indicatif`'s built-in exponential weighted moving average,
/// which produces wildly unstable estimates for slow paginated API fetches.
pub struct IndicatifProgress {
    bar: ProgressBar,
    /// Style to switch to once `set_total()` provides a known length.
    bar_style: ProgressStyle,
    /// When real progress started (set on `set_total()`).
    start: Mutex<Option<Instant>>,
    /// The original message (without ETA suffix), so we can re-append ETA
    /// on each `inc()` without accumulating suffixes.
    base_message: Mutex<String>,
}

impl IndicatifProgress {
    /// Creates a progress bar that starts as a spinner (no total known)
    /// and transitions to a full bar with percentage/ETA once
    /// [`ProgressCallback::set_total()`] is called.
    #[must_use]
    pub fn records_bar(multi: &MultiProgress, message: &str) -> Arc<dyn ProgressCallback> {
        let bar = multi.add(ProgressBar::new_spinner());
        bar.enable_steady_tick(Duration::from_millis(100));
        bar.set_style(
            ProgressStyle::with_template("{spinner:.cyan} {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        bar.set_message(message.to_string());

        let bar_style = ProgressStyle::with_template(
            "  {msg} {wide_bar:.cyan/dim} {pos}/{len} {percent}% [{elapsed_precise}]",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("##-");

        Arc::new(Self {
            bar,
            bar_style,
            start: Mutex::new(None),
            base_message: Mutex::new(message.to_string()),
        })
    }

    /// Creates a progress bar for step-level progress (e.g., sources 1/7).
    /// Total is known up front so this starts as a bar immediately.
    #[must_use]
    pub fn steps_bar(
        multi: &MultiProgress,
        message: &str,
        total: u64,
    ) -> Arc<dyn ProgressCallback> {
        let bar = multi.add(ProgressBar::new(total));
        bar.set_style(
            ProgressStyle::with_template(
                "{msg} {wide_bar:.green/dim} {pos}/{len} [{elapsed_precise}]",
            )
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("##-"),
        );
        bar.set_message(message.to_string());

        let bar_style = bar.style();

        Arc::new(Self {
            bar,
            bar_style,
            start: Mutex::new(Some(Instant::now())),
            base_message: Mutex::new(message.to_string()),
        })
    }

    /// Creates a progress bar for batch operations where total may or may
    /// not be known up front (e.g., attribution, geocoding, generation).
    /// Starts as a spinner and transitions on `set_total()`.
    #[must_use]
    pub fn batch_bar(multi: &MultiProgress, message: &str) -> Arc<dyn ProgressCallback> {
        let bar = multi.add(ProgressBar::new_spinner());
        bar.enable_steady_tick(Duration::from_millis(100));
        bar.set_style(
            ProgressStyle::with_template("{spinner:.yellow} {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        bar.set_message(message.to_string());

        let bar_style = ProgressStyle::with_template(
            "  {msg} {wide_bar:.yellow/dim} {pos}/{len} {percent}% [{elapsed_precise}]",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("##-");

        Arc::new(Self {
            bar,
            bar_style,
            start: Mutex::new(None),
            base_message: Mutex::new(message.to_string()),
        })
    }

    /// Computes a linear ETA and appends it to the bar's message.
    fn update_eta(&self) {
        let start = *self.start.lock().unwrap();
        let Some(start) = start else { return };

        let pos = self.bar.position();
        let len = self.bar.length().unwrap_or(0);
        if pos == 0 || len == 0 {
            return;
        }

        let elapsed = start.elapsed().as_secs_f64();
        #[allow(clippy::cast_precision_loss)]
        let rate = pos as f64 / elapsed;
        let remaining = len.saturating_sub(pos);
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let eta_secs = (remaining as f64 / rate) as u64;
        let eta_str = format_eta(eta_secs);

        let base = self.base_message.lock().unwrap().clone();
        self.bar
            .set_message(format!("{base} [~{eta_str} remaining]"));
    }
}

impl ProgressCallback for IndicatifProgress {
    fn set_total(&self, total: u64) {
        self.bar.set_length(total);
        self.bar.reset();
        *self.start.lock().unwrap() = Some(Instant::now());
        // Switch from spinner to bar style now that we know the total.
        self.bar.set_style(self.bar_style.clone());
    }

    fn set_position(&self, pos: u64) {
        self.bar.set_position(pos);
        self.update_eta();
    }

    fn inc(&self, delta: u64) {
        self.bar.inc(delta);
        self.update_eta();
    }

    fn set_message(&self, msg: String) {
        self.base_message.lock().unwrap().clone_from(&msg);
        self.bar.set_message(msg);
    }

    fn finish(&self, msg: String) {
        self.bar.finish_with_message(msg);
    }

    fn finish_and_clear(&self) {
        self.bar.finish_and_clear();
    }
}

/// Initializes the global logger wrapped in `indicatif-log-bridge` so that
/// `log::info!` and friends are suspended while progress bars redraw.
///
/// Returns the [`MultiProgress`] that all progress bars must be added to.
#[must_use]
pub fn init_logger() -> MultiProgress {
    let multi = MultiProgress::new();

    // Build the pretty-env-logger logger manually so we can wrap it.
    let logger = pretty_env_logger::formatted_builder()
        .parse_env("RUST_LOG")
        .build();
    let level = logger.filter();

    indicatif_log_bridge::LogWrapper::new(multi.clone(), logger)
        .try_init()
        .ok(); // Ignore error if logger was already set (e.g., in tests)

    log::set_max_level(level);

    multi
}

// ── Shared interactive prompts ──────────────────────────────────

/// Presents a multi-select checkbox list of all configured crime data
/// sources (formatted as `"source_id — Source Name"`).
///
/// Returns the selected source IDs as strings. If the user selects
/// nothing, returns an empty `Vec`.
///
/// # Errors
///
/// Returns an error if the terminal interaction fails.
pub fn prompt_source_multiselect(prompt: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let sources = crime_map_source::registry::all_sources();
    if sources.is_empty() {
        return Ok(Vec::new());
    }

    let labels: Vec<String> = sources
        .iter()
        .map(|s| format!("{} \u{2014} {}", s.id(), s.name()))
        .collect();

    let selected = dialoguer::MultiSelect::new()
        .with_prompt(prompt)
        .items(&labels)
        .max_length(20)
        .interact()?;

    Ok(selected
        .into_iter()
        .map(|i| sources[i].id().to_string())
        .collect())
}

/// Prompts the user for an optional `u64` value.
///
/// Returns `None` if the input is empty.
///
/// # Errors
///
/// Returns an error if the terminal interaction or parsing fails.
pub fn prompt_optional_u64(prompt: &str) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let input: String = dialoguer::Input::new()
        .with_prompt(prompt)
        .allow_empty(true)
        .interact_text()?;

    if input.trim().is_empty() {
        Ok(None)
    } else {
        Ok(Some(input.trim().parse()?))
    }
}
