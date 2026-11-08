//! Cosine scoring with the HNSW four-lane f64 accumulation order.

/// Selected once per graph; the function pointer never escapes this module.
#[derive(Clone, Copy)]
pub(super) struct CosineKernel {
    #[cfg(target_arch = "x86_64")]
    score: unsafe fn(&[f32], &[f32], f64, f64) -> f64,
}

impl CosineKernel {
    pub(super) fn select() -> Self {
        #[cfg(target_arch = "x86_64")]
        if std::arch::is_x86_feature_detected!("avx") {
            return Self { score: avx };
        }
        Self {
            #[cfg(target_arch = "x86_64")]
            score: scalar,
        }
    }

    #[inline]
    pub(super) fn score(self, left: &[f32], right: &[f32], ln: f64, rn: f64) -> f64 {
        // SAFETY: select is the only constructor and verifies the target feature
        // before storing avx. Both implementations accept arbitrary slice lengths.
        #[cfg(target_arch = "x86_64")]
        unsafe {
            (self.score)(left, right, ln, rn)
        }
        #[cfg(not(target_arch = "x86_64"))]
        scalar(left, right, ln, rn)
    }
}

pub(super) fn scalar(left: &[f32], right: &[f32], left_norm: f64, right_norm: f64) -> f64 {
    if left_norm == 0.0 || right_norm == 0.0 {
        return 0.0;
    }
    let length = left.len().min(right.len());
    let (left_chunks, left_remainder) = left[..length].as_chunks::<4>();
    let (right_chunks, right_remainder) = right[..length].as_chunks::<4>();
    let (mut dot0, mut dot1, mut dot2, mut dot3) = (0.0, 0.0, 0.0, 0.0);
    for (left, right) in left_chunks.iter().zip(right_chunks) {
        dot0 += f64::from(left[0]) * f64::from(right[0]);
        dot1 += f64::from(left[1]) * f64::from(right[1]);
        dot2 += f64::from(left[2]) * f64::from(right[2]);
        dot3 += f64::from(left[3]) * f64::from(right[3]);
    }
    let mut dot = dot0 + dot1 + dot2 + dot3;
    for (&a, &b) in left_remainder.iter().zip(right_remainder) {
        dot += f64::from(a) * f64::from(b);
    }
    (dot / (left_norm * right_norm)).clamp(-1.0, 1.0)
}

/// Requires AVX. Each load covers exactly one complete four-element chunk.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn avx(left: &[f32], right: &[f32], left_norm: f64, right_norm: f64) -> f64 {
    use std::arch::x86_64::*;
    if left_norm == 0.0 || right_norm == 0.0 {
        return 0.0;
    }
    let length = left.len().min(right.len());
    let (left_chunks, left_remainder) = left[..length].as_chunks::<4>();
    let (right_chunks, right_remainder) = right[..length].as_chunks::<4>();
    let mut lanes = _mm256_setzero_pd();
    for (left, right) in left_chunks.iter().zip(right_chunks) {
        // SAFETY: each array has four initialized f32 values. loadu does not
        // require alignment, and neither load extends beyond its array.
        let (left, right) = unsafe {
            (
                _mm256_cvtps_pd(_mm_loadu_ps(left.as_ptr())),
                _mm256_cvtps_pd(_mm_loadu_ps(right.as_ptr())),
            )
        };
        // Keep multiplication and addition separate: FMA would change rounding.
        lanes = _mm256_add_pd(lanes, _mm256_mul_pd(left, right));
    }
    let mut sums = [0.0; 4];
    // SAFETY: sums has room for all four f64 lanes; storeu needs no alignment.
    unsafe { _mm256_storeu_pd(sums.as_mut_ptr(), lanes) };
    let mut dot = sums[0] + sums[1] + sums[2] + sums[3];
    for (&a, &b) in left_remainder.iter().zip(right_remainder) {
        dot += f64::from(a) * f64::from(b);
    }
    (dot / (left_norm * right_norm)).clamp(-1.0, 1.0)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn assert_matches(left: &[f32], right: &[f32], ln: f64, rn: f64) {
        let expected = scalar(left, right, ln, rn).to_bits();
        assert_eq!(
            CosineKernel::select().score(left, right, ln, rn).to_bits(),
            expected
        );
    }

    #[test]
    fn known_scores_preserve_reduction_order_tails_and_clamping() {
        #[cfg(target_arch = "x86_64")]
        {
            let available = std::arch::is_x86_feature_detected!("avx");
            let expected: unsafe fn(&[f32], &[f32], f64, f64) -> f64 =
                if available { avx } else { scalar };
            assert!(std::ptr::fn_addr_eq(CosineKernel::select().score, expected));
            eprintln!(
                "HNSW_COSINE_KERNEL={}",
                if available { "avx" } else { "scalar" }
            );
        }
        #[cfg(not(target_arch = "x86_64"))]
        eprintln!("HNSW_COSINE_KERNEL=scalar");
        let large = 2.0_f32.powi(60);
        type ScoreCase<'a> = (&'a [f32], &'a [f32], f64, f64, f64);
        let cases: &[ScoreCase<'_>] = &[
            (&[], &[], 1.0, 1.0, 0.0),
            (&[1.0], &[1.0], 0.0, 1.0, 0.0),
            (&[1.0], &[1.0], 1.0, 0.0, 0.0),
            (&[2.0], &[1.0], 1.0, 1.0, 1.0),
            (&[-2.0], &[1.0], 1.0, 1.0, -1.0),
            (&[large, 1.0, -large, 1.0], &[1.0; 4], 1.0, 2.0, 0.5),
            (
                &[
                    large, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, -large, 0.0, 0.0, 0.0, 1.0,
                ],
                &[1.0; 13],
                1.0,
                2.0,
                0.5,
            ),
        ];
        for &(a, b, an, bn, expected) in cases {
            assert_eq!(scalar(a, b, an, bn).to_bits(), expected.to_bits());
            assert_eq!(
                CosineKernel::select().score(a, b, an, bn).to_bits(),
                expected.to_bits()
            );
        }
        for length in 1..=7 {
            let a = &([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0][..length]);
            let b = [1.0; 8];
            let expected = (length * (length + 1) / 2) as f64 / 32.0;
            for (a, b) in [(a, b.as_slice()), (b.as_slice(), a)] {
                assert_eq!(scalar(a, b, 2.0, 16.0).to_bits(), expected.to_bits());
                assert_eq!(
                    CosineKernel::select().score(a, b, 2.0, 16.0).to_bits(),
                    expected.to_bits()
                );
            }
        }
    }

    #[test]
    fn tails_unaligned_slices_and_zero_norm_preserve_bits() {
        let values = [
            0.0,
            -0.0,
            f32::MIN_POSITIVE,
            f32::from_bits(1),
            f32::MAX,
            -f32::MAX,
            1.0,
            -1.0,
        ];
        let left: Vec<_> = values.into_iter().cycle().take(35).collect();
        let right: Vec<_> = values.into_iter().rev().cycle().take(35).collect();
        for length in 0..=32 {
            for tail in 0..=2 {
                let a = &left[1..1 + length];
                let b = &right[1..1 + length + tail];
                let an = crate::vector::hnsw::vector_norm(a);
                let bn = crate::vector::hnsw::vector_norm(b);
                assert_matches(a, b, an, bn);
                assert_matches(b, a, bn, an);
                assert_matches(a, b, 0.0, bn);
                assert_matches(a, b, an, 0.0);
                // Exercise both clamp limits without relying on normalized inputs.
                assert_matches(a, b, 1.0, 1.0);
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn finite_vectors_match_scalar_bits(
            left in prop::collection::vec(any::<u32>(), 0..34),
            right in prop::collection::vec(any::<u32>(), 0..34),
        ) {
            // Clear one exponent bit to retain signs, subnormals and a broad
            // exponent range without allowing NaN or infinity.
            let left: Vec<_> = left.into_iter().map(|bits| f32::from_bits(bits & 0xfeff_ffff)).collect();
            let right: Vec<_> = right.into_iter().map(|bits| f32::from_bits(bits & 0xfeff_ffff)).collect();
            assert_matches(&left, &right, crate::vector::hnsw::vector_norm(&left), crate::vector::hnsw::vector_norm(&right));
        }
    }
}
