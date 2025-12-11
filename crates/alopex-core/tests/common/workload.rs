use rand::prelude::*;
use rand::rngs::StdRng;

/// ワークロード操作。
#[derive(Clone, Debug)]
pub enum Operation {
    Get(Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Scan(Vec<u8>),
}

/// ワークロード設定。
#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    pub operation_count: usize,
    pub key_space_size: usize,
    pub value_size: usize,
    pub seed: u64,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            operation_count: 100,
            key_space_size: 1000,
            value_size: 64,
            seed: 7,
        }
    }
}

/// 決定論的ワークロードジェネレーター。
pub struct WorkloadGenerator {
    cfg: WorkloadConfig,
    rng: StdRng,
}

impl WorkloadGenerator {
    pub fn new(cfg: WorkloadConfig) -> Self {
        Self {
            rng: StdRng::seed_from_u64(cfg.seed),
            cfg,
        }
    }

    pub fn generate_batch(&mut self) -> Vec<Operation> {
        let mut ops = Vec::with_capacity(self.cfg.operation_count);
        for _ in 0..self.cfg.operation_count {
            let choice = self.rng.gen_range(0..4);
            let key = self.random_key();
            let op = match choice {
                0 => Operation::Get(key),
                1 => {
                    let val = self.random_value();
                    Operation::Put(key, val)
                }
                2 => Operation::Delete(key),
                _ => Operation::Scan(self.random_prefix()),
            };
            ops.push(op);
        }
        ops
    }

    fn random_key(&mut self) -> Vec<u8> {
        let k = self.rng.gen_range(0..self.cfg.key_space_size);
        format!("key_{:08}", k).into_bytes()
    }

    fn random_prefix(&mut self) -> Vec<u8> {
        let k = self.rng.gen_range(0..self.cfg.key_space_size);
        format!("key_{:04}", k % 100).into_bytes()
    }

    fn random_value(&mut self) -> Vec<u8> {
        (0..self.cfg.value_size).map(|_| self.rng.gen()).collect()
    }
}
