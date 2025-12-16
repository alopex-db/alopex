use crate::error::ParserError;

pub const DEFAULT_RECURSION_LIMIT: usize = 50;

/// Prevents runaway recursion during parsing.
#[derive(Debug, Clone)]
pub struct RecursionCounter {
    max_depth: usize,
    remaining_depth: std::cell::Cell<usize>,
}

impl RecursionCounter {
    pub fn new(max_depth: usize) -> Self {
        Self {
            max_depth,
            remaining_depth: std::cell::Cell::new(max_depth),
        }
    }

    pub fn try_decrease(&self) -> Result<DepthGuard, ParserError> {
        let remaining = self.remaining_depth.get();
        if remaining == 0 {
            Err(ParserError::RecursionLimitExceeded {
                depth: self.max_depth + 1, // actual depth attempted
            })
        } else {
            self.remaining_depth.set(remaining - 1);
            Ok(DepthGuard {
                counter: self as *const RecursionCounter,
            })
        }
    }

    pub fn current_depth(&self) -> usize {
        self.max_depth - self.remaining_depth.get()
    }
}

#[derive(Debug)]
pub struct DepthGuard {
    counter: *const RecursionCounter,
}

impl Drop for DepthGuard {
    fn drop(&mut self) {
        // Safety: counter points to self.recursion inside Parser; guard does not outlive Parser.
        let counter = unsafe { &*self.counter };
        let old_value = counter.remaining_depth.get();
        counter.remaining_depth.set(old_value + 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_overflow_depth_as_limit_plus_one() {
        let counter = RecursionCounter::new(2);
        let _g1 = counter.try_decrease().unwrap();
        let _g2 = counter.try_decrease().unwrap();
        let err = counter.try_decrease().unwrap_err();
        match err {
            ParserError::RecursionLimitExceeded { depth } => assert_eq!(depth, 3),
            other => panic!("expected recursion limit error, got {:?}", other),
        }
    }
}
