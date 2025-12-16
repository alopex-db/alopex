//! SIMD ベースの距離カーネル群。
//!
//! - DistanceKernel: 共通インターフェイス（Send + Sync）
//! - ScalarKernel: 参照実装（unsafe なし）
//! - Avx2Kernel: x86_64 AVX2 実装（条件コンパイル）
//! - NeonKernel: aarch64 NEON 実装（条件コンパイル）
//! - select_kernel: 実行時に最適カーネルを選択

use crate::vector::Metric;

/// 距離カーネルの共通インターフェイス。
pub trait DistanceKernel: Send + Sync {
    /// コサイン類似度（0.0〜1.0、ゼロノルム時は0.0）。
    fn cosine(&self, query: &[f32], vector: &[f32]) -> f32;
    /// L2 距離（負値、大きいほど近い）。
    fn l2(&self, query: &[f32], vector: &[f32]) -> f32;
    /// 内積。
    fn inner_product(&self, query: &[f32], vector: &[f32]) -> f32;
    /// バッチスコアリング。`vectors` は `dimension * n` の連続配列。
    fn batch_score(
        &self,
        metric: Metric,
        query: &[f32],
        vectors: &[f32],
        dimension: usize,
        scores: &mut [f32],
    );
}

/// スカラーカーネル（リファレンス実装）。
#[derive(Debug, Default)]
pub struct ScalarKernel;

impl ScalarKernel {
    #[inline]
    fn dot(query: &[f32], vector: &[f32]) -> f32 {
        query
            .iter()
            .zip(vector.iter())
            .map(|(a, b)| a * b)
            .sum::<f32>()
    }

    #[inline]
    fn norm(v: &[f32]) -> f32 {
        v.iter().map(|x| x * x).sum::<f32>().sqrt()
    }
}

impl DistanceKernel for ScalarKernel {
    fn cosine(&self, query: &[f32], vector: &[f32]) -> f32 {
        let dot = Self::dot(query, vector);
        let q_norm = Self::norm(query);
        let v_norm = Self::norm(vector);
        if q_norm == 0.0 || v_norm == 0.0 {
            0.0
        } else {
            dot / (q_norm * v_norm)
        }
    }

    fn l2(&self, query: &[f32], vector: &[f32]) -> f32 {
        let dist = query
            .iter()
            .zip(vector.iter())
            .map(|(a, b)| {
                let d = a - b;
                d * d
            })
            .sum::<f32>()
            .sqrt();
        -dist
    }

    fn inner_product(&self, query: &[f32], vector: &[f32]) -> f32 {
        Self::dot(query, vector)
    }

    fn batch_score(
        &self,
        metric: Metric,
        query: &[f32],
        vectors: &[f32],
        dimension: usize,
        scores: &mut [f32],
    ) {
        for (i, chunk) in vectors.chunks(dimension).enumerate() {
            if i >= scores.len() {
                break;
            }
            scores[i] = match metric {
                Metric::Cosine => self.cosine(query, chunk),
                Metric::L2 => self.l2(query, chunk),
                Metric::InnerProduct => self.inner_product(query, chunk),
            };
        }
    }
}

// ============================================================================
// AVX2 実装 (x86_64)
// ============================================================================

#[cfg(target_arch = "x86_64")]
mod avx2 {
    use super::{DistanceKernel, Metric};
    use std::arch::x86_64::*;

    #[derive(Debug, Default)]
    pub struct Avx2Kernel;

    #[inline]
    fn horizontal_sum_ps(v: __m256) -> f32 {
        unsafe {
            // Reduce 8 lanes -> 4 -> 2 -> 1 using horizontal add.
            let lo = _mm256_castps256_ps128(v);
            let hi = _mm256_extractf128_ps(v, 1);
            let sum128 = _mm_add_ps(lo, hi); // [a0+a4, a1+a5, a2+a6, a3+a7]
            let sum64 = _mm_hadd_ps(sum128, sum128); // [a0+a4+a1+a5, a2+a6+a3+a7, ...]
            let sum32 = _mm_hadd_ps(sum64, sum64); // [total, total, ...]
            _mm_cvtss_f32(sum32)
        }
    }

    impl Avx2Kernel {
        #[inline]
        unsafe fn dot(query: &[f32], vector: &[f32]) -> f32 {
            let mut acc = _mm256_setzero_ps();
            let mut i = 0;
            while i + 8 <= query.len() {
                let q = _mm256_loadu_ps(query.as_ptr().add(i));
                let v = _mm256_loadu_ps(vector.as_ptr().add(i));
                // AVX2+FMA 前提: FMA 非搭載AVX2はサポート対象外（設計方針）。
                acc = _mm256_fmadd_ps(q, v, acc);
                i += 8;
            }
            let mut sum = horizontal_sum_ps(acc);
            for j in i..query.len() {
                sum += *query.get_unchecked(j) * *vector.get_unchecked(j);
            }
            sum
        }

        #[inline]
        unsafe fn norm(v: &[f32]) -> f32 {
            let mut acc = _mm256_setzero_ps();
            let mut i = 0;
            while i + 8 <= v.len() {
                let x = _mm256_loadu_ps(v.as_ptr().add(i));
                acc = _mm256_fmadd_ps(x, x, acc);
                i += 8;
            }
            let mut sum = horizontal_sum_ps(acc);
            for j in i..v.len() {
                let x = *v.get_unchecked(j);
                sum += x * x;
            }
            sum.sqrt()
        }

        #[inline]
        unsafe fn cosine_impl(&self, query: &[f32], vector: &[f32]) -> f32 {
            let dot = Self::dot(query, vector);
            let q_norm = Self::norm(query);
            let v_norm = Self::norm(vector);
            if q_norm == 0.0 || v_norm == 0.0 {
                0.0
            } else {
                dot / (q_norm * v_norm)
            }
        }

        #[inline]
        unsafe fn l2_impl(&self, query: &[f32], vector: &[f32]) -> f32 {
            let mut acc = _mm256_setzero_ps();
            let mut i = 0;
            while i + 8 <= query.len() {
                let q = _mm256_loadu_ps(query.as_ptr().add(i));
                let v = _mm256_loadu_ps(vector.as_ptr().add(i));
                let diff = _mm256_sub_ps(q, v);
                acc = _mm256_fmadd_ps(diff, diff, acc);
                i += 8;
            }
            let mut sum = horizontal_sum_ps(acc);
            for j in i..query.len() {
                let d = *query.get_unchecked(j) - *vector.get_unchecked(j);
                sum += d * d;
            }
            -sum.sqrt()
        }
    }

    impl DistanceKernel for Avx2Kernel {
        fn cosine(&self, query: &[f32], vector: &[f32]) -> f32 {
            unsafe { self.cosine_impl(query, vector) }
        }

        fn l2(&self, query: &[f32], vector: &[f32]) -> f32 {
            unsafe { self.l2_impl(query, vector) }
        }

        fn inner_product(&self, query: &[f32], vector: &[f32]) -> f32 {
            unsafe { Self::dot(query, vector) }
        }

        fn batch_score(
            &self,
            metric: Metric,
            query: &[f32],
            vectors: &[f32],
            dimension: usize,
            scores: &mut [f32],
        ) {
            for (i, chunk) in vectors.chunks(dimension).enumerate() {
                if i >= scores.len() {
                    break;
                }
                scores[i] = match metric {
                    Metric::Cosine => unsafe { self.cosine_impl(query, chunk) },
                    Metric::L2 => unsafe { self.l2_impl(query, chunk) },
                    Metric::InnerProduct => unsafe { Self::dot(query, chunk) },
                };
            }
        }
    }

    pub fn create() -> Box<dyn DistanceKernel> {
        Box::new(Avx2Kernel)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn horizontal_sum_correct_for_ones() {
            if !std::is_x86_feature_detected!("avx2") {
                return;
            }
            unsafe {
                let v = _mm256_set1_ps(1.0);
                let total = horizontal_sum_ps(v);
                assert!((total - 8.0).abs() < 1e-6);
            }
        }
    }
}

// ============================================================================
// NEON 実装 (aarch64)
// ============================================================================

#[cfg(target_arch = "aarch64")]
mod neon {
    use super::{DistanceKernel, Metric};
    use core::arch::aarch64::*;

    #[derive(Debug, Default)]
    pub struct NeonKernel;

    #[inline]
    unsafe fn horizontal_sum(v: float32x4_t) -> f32 {
        let pair_sum = vadd_f32(vget_low_f32(v), vget_high_f32(v));
        let sum = vpadd_f32(pair_sum, pair_sum);
        vget_lane_f32(sum, 0)
    }

    #[inline]
    unsafe fn dot(query: &[f32], vector: &[f32]) -> f32 {
        let mut acc = vdupq_n_f32(0.0);
        let mut i = 0;
        while i + 4 <= query.len() {
            let q = vld1q_f32(query.as_ptr().add(i));
            let v = vld1q_f32(vector.as_ptr().add(i));
            acc = vfmaq_f32(acc, q, v);
            i += 4;
        }
        let mut sum = horizontal_sum(acc);
        for j in i..query.len() {
            sum += *query.get_unchecked(j) * *vector.get_unchecked(j);
        }
        sum
    }

    #[inline]
    unsafe fn norm(v: &[f32]) -> f32 {
        let mut acc = vdupq_n_f32(0.0);
        let mut i = 0;
        while i + 4 <= v.len() {
            let x = vld1q_f32(v.as_ptr().add(i));
            acc = vfmaq_f32(acc, x, x);
            i += 4;
        }
        let mut sum = horizontal_sum(acc);
        for j in i..v.len() {
            let x = *v.get_unchecked(j);
            sum += x * x;
        }
        sum.sqrt()
    }

    impl DistanceKernel for NeonKernel {
        fn cosine(&self, query: &[f32], vector: &[f32]) -> f32 {
            unsafe {
                let dot = dot(query, vector);
                let q_norm = norm(query);
                let v_norm = norm(vector);
                if q_norm == 0.0 || v_norm == 0.0 {
                    0.0
                } else {
                    dot / (q_norm * v_norm)
                }
            }
        }

        fn l2(&self, query: &[f32], vector: &[f32]) -> f32 {
            unsafe {
                let mut acc = vdupq_n_f32(0.0);
                let mut i = 0;
                while i + 4 <= query.len() {
                    let q = vld1q_f32(query.as_ptr().add(i));
                    let v = vld1q_f32(vector.as_ptr().add(i));
                    let diff = vsubq_f32(q, v);
                    acc = vfmaq_f32(acc, diff, diff);
                    i += 4;
                }
                let mut sum = horizontal_sum(acc);
                for j in i..query.len() {
                    let d = *query.get_unchecked(j) - *vector.get_unchecked(j);
                    sum += d * d;
                }
                -sum.sqrt()
            }
        }

        fn inner_product(&self, query: &[f32], vector: &[f32]) -> f32 {
            unsafe { dot(query, vector) }
        }

        fn batch_score(
            &self,
            metric: Metric,
            query: &[f32],
            vectors: &[f32],
            dimension: usize,
            scores: &mut [f32],
        ) {
            for (i, chunk) in vectors.chunks(dimension).enumerate() {
                if i >= scores.len() {
                    break;
                }
                scores[i] = match metric {
                    Metric::Cosine => self.cosine(query, chunk),
                    Metric::L2 => self.l2(query, chunk),
                    Metric::InnerProduct => self.inner_product(query, chunk),
                };
            }
        }
    }

    pub fn create() -> Box<dyn DistanceKernel> {
        Box::new(NeonKernel)
    }
}

/// 実行時に最適なカーネルを選択する。
pub fn select_kernel() -> Box<dyn DistanceKernel> {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            return avx2::create();
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return neon::create();
        }
    }

    Box::new(ScalarKernel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::score;

    #[test]
    fn scalar_matches_reference() {
        let k = ScalarKernel;
        let q = [1.0, 2.0, 3.0, 4.0];
        let v = [4.0, 3.0, 2.0, 1.0];
        let metrics = [Metric::Cosine, Metric::L2, Metric::InnerProduct];
        for &m in &metrics {
            let ref_score = score(m, &q, &v).unwrap();
            let k_score = match m {
                Metric::Cosine => k.cosine(&q, &v),
                Metric::L2 => k.l2(&q, &v),
                Metric::InnerProduct => k.inner_product(&q, &v),
            };
            assert!((ref_score - k_score).abs() < 1e-6);
        }
    }

    #[test]
    fn scalar_cosine_zero_norm_returns_zero() {
        let k = ScalarKernel;
        let q = [0.0, 0.0, 0.0];
        let v = [1.0, 2.0, 3.0];
        assert_eq!(k.cosine(&q, &v), 0.0);
    }

    #[test]
    fn batch_score_populates_all() {
        let k = ScalarKernel;
        let q = [1.0, 0.0];
        let vectors = [1.0, 0.0, 0.0, 1.0];
        let mut scores = [0.0f32; 2];
        k.batch_score(Metric::InnerProduct, &q, &vectors, 2, &mut scores);
        assert_eq!(scores[0], 1.0);
        assert_eq!(scores[1], 0.0);
    }

    #[test]
    fn select_kernel_returns_any() {
        let k = select_kernel();
        let q = [1.0, 2.0];
        let v = [2.0, 1.0];
        let s = k.inner_product(&q, &v);
        assert!((s - 4.0).abs() < 1e-6);
    }

    #[test]
    fn select_kernel_matches_scalar_for_all_metrics() {
        let kernel = select_kernel();
        let scalar = ScalarKernel;
        let q = vec![1.0f32, 2.0, 3.0, 4.0];
        let v1 = vec![4.0f32, 3.0, 2.0, 1.0];
        let v2 = vec![1.0f32, 1.0, 1.0, 1.0];

        let metrics = [Metric::Cosine, Metric::L2, Metric::InnerProduct];
        for &m in &metrics {
            let s1 = match m {
                Metric::Cosine => scalar.cosine(&q, &v1),
                Metric::L2 => scalar.l2(&q, &v1),
                Metric::InnerProduct => scalar.inner_product(&q, &v1),
            };
            let k1 = match m {
                Metric::Cosine => kernel.cosine(&q, &v1),
                Metric::L2 => kernel.l2(&q, &v1),
                Metric::InnerProduct => kernel.inner_product(&q, &v1),
            };
            assert!((s1 - k1).abs() < 1e-6);

            let s2 = match m {
                Metric::Cosine => scalar.cosine(&q, &v2),
                Metric::L2 => scalar.l2(&q, &v2),
                Metric::InnerProduct => scalar.inner_product(&q, &v2),
            };
            let k2 = match m {
                Metric::Cosine => kernel.cosine(&q, &v2),
                Metric::L2 => kernel.l2(&q, &v2),
                Metric::InnerProduct => kernel.inner_product(&q, &v2),
            };
            assert!((s2 - k2).abs() < 1e-6);
        }
    }

    fn assert_same_f32(a: f32, b: f32) {
        if a.is_nan() && b.is_nan() {
            return;
        }
        if a.is_infinite() && b.is_infinite() {
            assert_eq!(a.is_sign_positive(), b.is_sign_positive());
            return;
        }
        assert!((a - b).abs() < 1e-5, "a={a}, b={b}");
    }

    #[test]
    fn kernel_handles_nan_and_inf_like_scalar() {
        let kernel = select_kernel();
        let scalar = ScalarKernel;
        let cases = vec![
            (
                Metric::Cosine,
                vec![f32::NAN, 1.0, 2.0],
                vec![1.0, 2.0, 3.0],
            ),
            (
                Metric::InnerProduct,
                vec![f32::INFINITY, 1.0],
                vec![1.0, 2.0],
            ),
            (Metric::L2, vec![f32::INFINITY, 0.0], vec![1.0, 0.0]),
        ];

        for (metric, q, v) in cases {
            let s = match metric {
                Metric::Cosine => scalar.cosine(&q, &v),
                Metric::L2 => scalar.l2(&q, &v),
                Metric::InnerProduct => scalar.inner_product(&q, &v),
            };
            let k = match metric {
                Metric::Cosine => kernel.cosine(&q, &v),
                Metric::L2 => kernel.l2(&q, &v),
                Metric::InnerProduct => kernel.inner_product(&q, &v),
            };
            assert_same_f32(s, k);
        }
    }

    #[test]
    fn cosine_with_nan_matches_scalar() {
        let kernel = select_kernel();
        let scalar = ScalarKernel;
        let q = [f32::NAN, 1.0, 2.0, 3.0];
        let v = [1.0, 2.0, 3.0, 4.0];
        let s = scalar.cosine(&q, &v);
        let k = kernel.cosine(&q, &v);
        assert_same_f32(s, k);
    }

    #[test]
    fn l2_with_inf_matches_scalar() {
        let kernel = select_kernel();
        let scalar = ScalarKernel;
        let q = [f32::INFINITY, 0.0, 1.0];
        let v = [1.0, 0.0, 1.0];
        let s = scalar.l2(&q, &v);
        let k = kernel.l2(&q, &v);
        assert_same_f32(s, k);
    }

    #[test]
    fn inner_product_with_nan_matches_scalar() {
        let kernel = select_kernel();
        let scalar = ScalarKernel;
        let q = [1.0, f32::NAN];
        let v = [2.0, 3.0];
        let s = scalar.inner_product(&q, &v);
        let k = kernel.inner_product(&q, &v);
        assert_same_f32(s, k);
    }

    #[test]
    fn batch_score_propagates_nan_inf_like_scalar() {
        let kernel = select_kernel();
        let scalar = ScalarKernel;
        let q = [1.0, f32::NAN];
        let vectors = [2.0, 3.0, f32::INFINITY, 0.0];
        let mut scores_kernel = [0.0f32; 2];
        let mut scores_scalar = [0.0f32; 2];
        kernel.batch_score(Metric::InnerProduct, &q, &vectors, 2, &mut scores_kernel);
        scalar.batch_score(Metric::InnerProduct, &q, &vectors, 2, &mut scores_scalar);
        for (a, b) in scores_scalar.iter().zip(scores_kernel.iter()) {
            assert_same_f32(*a, *b);
        }
    }
}
