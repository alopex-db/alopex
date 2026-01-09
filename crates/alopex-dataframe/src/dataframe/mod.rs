#[allow(clippy::module_inception)]
mod dataframe;
mod series;

/// Eager `DataFrame` and `GroupBy`.
pub use dataframe::{DataFrame, GroupBy};
/// A named, chunked Arrow array.
pub use series::Series;
