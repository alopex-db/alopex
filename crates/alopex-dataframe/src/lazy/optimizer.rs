use crate::lazy::LogicalPlan;

pub struct Optimizer;

impl Optimizer {
    pub fn optimize(plan: &LogicalPlan) -> LogicalPlan {
        plan.clone()
    }
}
