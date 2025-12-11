use alopex_core::types::Value;
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

    pub fn next_operation(&mut self) -> Operation {
        let choice = self.rng.gen_range(0..4);
        let key = self.random_key();
        match choice {
            0 => Operation::Get(key),
            1 => {
                let val = self.random_value();
                Operation::Put(key, val)
            }
            2 => Operation::Delete(key),
            _ => Operation::Scan(self.random_prefix()),
        }
    }

    pub fn generate_batch(&mut self) -> Vec<Operation> {
        let mut ops = Vec::with_capacity(self.cfg.operation_count);
        for _ in 0..self.cfg.operation_count {
            ops.push(self.next_operation());
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

/// モデル比率設定。
#[derive(Clone, Debug)]
pub struct ModelMix {
    pub kv: f64,
    pub sql: f64,
    pub vector: f64,
    pub columnar: f64,
}

impl ModelMix {
    pub fn balanced() -> Self {
        Self {
            kv: 0.25,
            sql: 0.25,
            vector: 0.25,
            columnar: 0.25,
        }
    }

    fn weights(&self) -> [f64; 4] {
        let mut w = [self.kv, self.sql, self.vector, self.columnar];
        let total: f64 = w.iter().sum();
        if total <= f64::EPSILON {
            w = [1.0, 1.0, 1.0, 1.0];
        }
        w
    }
}

/// マルチモデルワークロード設定。
#[derive(Clone, Debug)]
pub struct MultiModelWorkloadConfig {
    pub model_mix: ModelMix,
    pub workload: WorkloadConfig,
    pub vector_dim: usize,
    pub columnar_width: usize,
}

impl Default for MultiModelWorkloadConfig {
    fn default() -> Self {
        Self {
            model_mix: ModelMix::balanced(),
            workload: WorkloadConfig::default(),
            vector_dim: 16,
            columnar_width: 4,
        }
    }
}

/// SQL操作。
#[derive(Clone, Debug)]
pub enum SqlOperation {
    Insert {
        table: String,
        row: Vec<(String, Value)>,
    },
    Select {
        table: String,
        filter: Option<String>,
    },
    Update {
        table: String,
        set: Vec<(String, Value)>,
        filter: Option<String>,
    },
    Delete {
        table: String,
        filter: Option<String>,
    },
}

/// ベクトル操作。
#[derive(Clone, Debug)]
pub enum VectorOperation {
    Insert {
        id: u64,
        vector: Vec<f32>,
        metadata: Option<Value>,
    },
    Search {
        query: Vec<f32>,
        k: usize,
    },
    Delete {
        id: u64,
    },
}

/// カラムナー操作。
#[derive(Clone, Debug)]
pub enum ColumnarOperation {
    BatchInsert {
        columns: Vec<Column>,
    },
    Scan {
        filter: Option<String>,
        projection: Vec<String>,
    },
}

/// カラムナーの列データ。
#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub values: Vec<Value>,
}

/// マルチモデルの操作。
#[derive(Clone, Debug)]
pub enum MultiModelOperation {
    Kv(Operation),
    Sql(SqlOperation),
    Vector(VectorOperation),
    Columnar(ColumnarOperation),
}

/// マルチモデルワークロードジェネレーター。
pub struct MultiModelWorkloadGenerator {
    cfg: MultiModelWorkloadConfig,
    rng: StdRng,
    kv_gen: WorkloadGenerator,
}

impl MultiModelWorkloadGenerator {
    pub fn new(cfg: MultiModelWorkloadConfig) -> Self {
        let kv_gen = WorkloadGenerator::new(cfg.workload.clone());
        Self {
            rng: StdRng::seed_from_u64(cfg.workload.seed),
            cfg,
            kv_gen,
        }
    }

    pub fn next_operation(&mut self) -> MultiModelOperation {
        match self.pick_model() {
            0 => MultiModelOperation::Kv(self.kv_gen.next_operation()),
            1 => MultiModelOperation::Sql(self.random_sql_op()),
            2 => MultiModelOperation::Vector(self.random_vector_op()),
            _ => MultiModelOperation::Columnar(self.random_columnar_op()),
        }
    }

    pub fn generate_batch(&mut self, count: usize) -> Vec<MultiModelOperation> {
        (0..count).map(|_| self.next_operation()).collect()
    }

    fn pick_model(&mut self) -> usize {
        let w = self.cfg.model_mix.weights();
        let total: f64 = w.iter().sum();
        let r = self.rng.gen::<f64>() * total;
        let mut acc = 0.0;
        for (idx, weight) in w.iter().enumerate() {
            acc += weight;
            if r <= acc {
                return idx;
            }
        }
        w.len() - 1
    }

    fn random_sql_op(&mut self) -> SqlOperation {
        let choice = self.rng.gen_range(0..4);
        let table = if self.rng.gen_bool(0.5) {
            "users".to_string()
        } else {
            "items".to_string()
        };
        match choice {
            0 => SqlOperation::Insert {
                table,
                row: vec![
                    ("id".to_string(), self.random_value(8)),
                    ("name".to_string(), self.random_value(12)),
                ],
            },
            1 => SqlOperation::Select {
                table,
                filter: Some("id > 10".into()),
            },
            2 => SqlOperation::Update {
                table,
                set: vec![("name".to_string(), self.random_value(10))],
                filter: Some("id = 1".into()),
            },
            _ => SqlOperation::Delete {
                table,
                filter: Some("id < 5".into()),
            },
        }
    }

    fn random_vector_op(&mut self) -> VectorOperation {
        let choice = self.rng.gen_range(0..3);
        match choice {
            0 => VectorOperation::Insert {
                id: self.rng.gen_range(0..10_000),
                vector: self.random_vector(self.cfg.vector_dim),
                metadata: Some(self.random_value(16)),
            },
            1 => VectorOperation::Search {
                query: self.random_vector(self.cfg.vector_dim),
                k: 10,
            },
            _ => VectorOperation::Delete {
                id: self.rng.gen_range(0..10_000),
            },
        }
    }

    fn random_columnar_op(&mut self) -> ColumnarOperation {
        if self.rng.gen_bool(0.5) {
            let mut cols = Vec::with_capacity(self.cfg.columnar_width);
            for idx in 0..self.cfg.columnar_width {
                let name = format!("c{idx}");
                let mut values = Vec::with_capacity(16);
                for _ in 0..16 {
                    values.push(self.random_value(8));
                }
                cols.push(Column { name, values });
            }
            ColumnarOperation::BatchInsert { columns: cols }
        } else {
            let projection: Vec<String> = (0..self.cfg.columnar_width.min(4))
                .map(|i| format!("c{i}"))
                .collect();
            ColumnarOperation::Scan {
                filter: Some("c0 > 0".into()),
                projection,
            }
        }
    }

    fn random_value(&mut self, len: usize) -> Value {
        (0..len).map(|_| self.rng.gen()).collect()
    }

    fn random_vector(&mut self, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| self.rng.gen_range(0.0..1.0)).collect()
    }
}
