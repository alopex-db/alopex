use rand::prelude::*;
use rand::rngs::StdRng;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, OnceLock};

static SEED_OVERRIDE: OnceLock<Option<u64>> = OnceLock::new();
static DETERMINISTIC_RNG: OnceLock<Mutex<StdRng>> = OnceLock::new();

fn parse_seed_env() -> Option<u64> {
    std::env::var("STRESS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| {
            std::env::var("STRESS_REPLAY_SEED")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
        })
}

pub fn seed_override() -> Option<u64> {
    *SEED_OVERRIDE.get_or_init(parse_seed_env)
}

pub fn deterministic_mode() -> bool {
    seed_override().is_some()
        || std::env::var("STRESS_REPLAY")
            .ok()
            .map(|v| v != "0")
            .unwrap_or(false)
}

pub fn effective_seed(default_seed: u64) -> u64 {
    seed_override().unwrap_or(default_seed)
}

pub fn seed_for_name(name: &str, default_seed: u64) -> u64 {
    if let Some(seed) = seed_override() {
        return seed;
    }
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    default_seed ^ hasher.finish()
}

fn with_rng<T>(f: impl FnOnce(&mut StdRng) -> T) -> Option<T> {
    let seed = seed_override()?;
    let rng = DETERMINISTIC_RNG.get_or_init(|| Mutex::new(StdRng::seed_from_u64(seed)));
    let mut guard = rng.lock().unwrap();
    Some(f(&mut guard))
}

pub fn gen_f64() -> f64 {
    with_rng(|rng| rng.gen::<f64>()).unwrap_or_else(|| rand::thread_rng().gen::<f64>())
}

pub fn gen_u32() -> u32 {
    with_rng(|rng| rng.gen::<u32>()).unwrap_or_else(|| rand::thread_rng().gen::<u32>())
}

pub fn gen_u8() -> u8 {
    with_rng(|rng| rng.gen::<u8>()).unwrap_or_else(|| rand::thread_rng().gen::<u8>())
}

pub fn gen_range_usize(range: std::ops::Range<usize>) -> usize {
    with_rng(|rng| rng.gen_range(range.clone()))
        .unwrap_or_else(|| rand::thread_rng().gen_range(range))
}
