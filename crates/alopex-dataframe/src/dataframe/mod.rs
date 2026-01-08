#[allow(clippy::module_inception)]
mod dataframe;
mod series;

pub use dataframe::{DataFrame, GroupBy};
pub use series::Series;
