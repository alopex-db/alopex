#[allow(clippy::module_inception)]
mod dataframe;
mod series;

pub use dataframe::DataFrame;
pub use series::Series;
