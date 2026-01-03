use std::time::Duration;

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

use crate::batch::BatchMode;

pub struct ProgressIndicator {
    bar: Option<ProgressBar>,
    finished: bool,
}

impl ProgressIndicator {
    pub fn new(
        batch_mode: &BatchMode,
        explicit_progress: bool,
        quiet: bool,
        message: impl Into<String>,
    ) -> Self {
        if quiet || !batch_mode.should_show_progress(explicit_progress) {
            return Self {
                bar: None,
                finished: false,
            };
        }

        let bar = ProgressBar::new_spinner();
        bar.set_draw_target(ProgressDrawTarget::stderr());
        let style = ProgressStyle::with_template("{spinner} {msg}")
            .unwrap()
            .tick_strings(&["-", "\\", "|", "/"]);
        bar.set_style(style);
        bar.set_message(message.into());
        bar.enable_steady_tick(Duration::from_millis(120));

        Self {
            bar: Some(bar),
            finished: false,
        }
    }

    pub fn finish_with_message(&mut self, message: impl Into<String>) {
        if let Some(bar) = &self.bar {
            bar.finish_with_message(message.into());
            self.finished = true;
        }
    }

    pub fn finish_and_clear(&mut self) {
        if let Some(bar) = &self.bar {
            bar.finish_and_clear();
            self.finished = true;
        }
    }
}

impl Drop for ProgressIndicator {
    fn drop(&mut self) {
        if self.bar.is_some() && !self.finished {
            self.finish_and_clear();
        }
    }
}
