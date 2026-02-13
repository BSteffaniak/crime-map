//! `indicatif`-backed progress bar implementation.
//!
//! Wraps [`indicatif::ProgressBar`] behind the [`ProgressCallback`] trait
//! so that progress reporting stays decoupled from the rendering backend
//! throughout the pipeline.

use std::sync::Arc;

use crime_map_source::progress::ProgressCallback;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

/// An `indicatif` [`ProgressBar`] that implements [`ProgressCallback`].
pub struct IndicatifProgress {
    bar: ProgressBar,
    /// Style to switch to once `set_total()` provides a known length.
    bar_style: ProgressStyle,
}

impl IndicatifProgress {
    /// Creates a progress bar that starts as a spinner (no total known)
    /// and transitions to a full bar with percentage/ETA once
    /// [`ProgressCallback::set_total()`] is called.
    #[must_use]
    pub fn records_bar(multi: &MultiProgress, message: &str) -> Arc<dyn ProgressCallback> {
        let bar = multi.add(ProgressBar::new_spinner());
        bar.set_style(
            ProgressStyle::with_template("{spinner:.cyan} {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        bar.set_message(message.to_string());

        let bar_style = ProgressStyle::with_template(
            "  {msg} {wide_bar:.cyan/dim} {pos}/{len} {percent}% [{eta}]",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("##-");

        Arc::new(Self { bar, bar_style })
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

        Arc::new(Self { bar, bar_style })
    }

    /// Creates a progress bar for batch operations where total may or may
    /// not be known up front (e.g., attribution, geocoding, generation).
    /// Starts as a spinner and transitions on `set_total()`.
    #[must_use]
    pub fn batch_bar(multi: &MultiProgress, message: &str) -> Arc<dyn ProgressCallback> {
        let bar = multi.add(ProgressBar::new_spinner());
        bar.set_style(
            ProgressStyle::with_template("{spinner:.yellow} {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        bar.set_message(message.to_string());

        let bar_style = ProgressStyle::with_template(
            "  {msg} {wide_bar:.yellow/dim} {pos}/{len} {percent}% [{eta}]",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("##-");

        Arc::new(Self { bar, bar_style })
    }
}

impl ProgressCallback for IndicatifProgress {
    fn set_total(&self, total: u64) {
        self.bar.set_length(total);
        self.bar.set_position(0);
        // Switch from spinner to bar style now that we know the total.
        self.bar.set_style(self.bar_style.clone());
    }

    fn set_position(&self, pos: u64) {
        self.bar.set_position(pos);
    }

    fn inc(&self, delta: u64) {
        self.bar.inc(delta);
    }

    fn set_message(&self, msg: String) {
        self.bar.set_message(msg);
    }

    fn finish(&self, msg: String) {
        self.bar.finish_with_message(msg);
    }

    fn finish_and_clear(&self) {
        self.bar.finish_and_clear();
    }
}
