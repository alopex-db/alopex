//! バックプレッシャ制御用のコンパクション負債トラッカー。

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// 書き込みスロットリング設定。
#[derive(Debug, Clone)]
pub struct WriteThrottleConfig {
    /// ペンディングセクション数上限。
    pub max_pending_sections: usize,
    /// ペンディングバイト数上限。
    pub max_pending_bytes: u64,
    /// スロットリング開始閾値（0.0-1.0）。
    pub throttle_threshold: f64,
}

impl Default for WriteThrottleConfig {
    fn default() -> Self {
        Self {
            max_pending_sections: 10,
            max_pending_bytes: 64 * 1024 * 1024, // 64MB
            throttle_threshold: 0.7,
        }
    }
}

/// コンパクション負債を追跡する。
#[derive(Debug)]
pub struct CompactionDebtTracker {
    pending_sections: AtomicUsize,
    pending_bytes: AtomicU64,
    config: WriteThrottleConfig,
}

impl CompactionDebtTracker {
    /// 新規トラッカーを作成する。
    pub fn new(config: WriteThrottleConfig) -> Self {
        Self {
            pending_sections: AtomicUsize::new(0),
            pending_bytes: AtomicU64::new(0),
            config,
        }
    }

    /// ペンディングセクションを追加する。
    pub fn add_pending(&self, bytes: u64) {
        self.pending_sections.fetch_add(1, Ordering::Relaxed);
        self.pending_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// ペンディングセクションを完了する。
    pub fn complete_pending(&self, bytes: u64) {
        self.pending_sections.fetch_sub(1, Ordering::Relaxed);
        self.pending_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// スロットリングが必要か判定する。
    pub fn should_throttle(&self) -> bool {
        self.debt_ratio() >= self.config.throttle_threshold
    }

    /// 現在の負債率を返す（0.0-1.0超の可能性あり）。
    pub fn debt_ratio(&self) -> f64 {
        let sections = self.pending_sections.load(Ordering::Relaxed) as f64;
        let bytes = self.pending_bytes.load(Ordering::Relaxed) as f64;
        let section_ratio = sections / self.config.max_pending_sections as f64;
        let bytes_ratio = bytes / self.config.max_pending_bytes as f64;
        section_ratio.max(bytes_ratio)
    }

    /// スロットリング待機時間を返す（指数バックオフ）。
    pub fn throttle_duration(&self) -> Duration {
        let ratio = self.debt_ratio();
        if ratio <= self.config.throttle_threshold {
            return Duration::from_millis(0);
        }
        // 閾値超過分に比例させ、最大2秒にクリップ。
        let over = ratio - self.config.throttle_threshold;
        let millis = (over * 1000.0).clamp(10.0, 2000.0);
        Duration::from_millis(millis as u64)
    }
}
