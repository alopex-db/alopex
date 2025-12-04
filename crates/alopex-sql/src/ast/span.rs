/// A location in source text (1-based line/column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Location {
    /// Line number (1-based). Zero represents an unknown location.
    pub line: u64,
    /// Column number (1-based). Zero represents an unknown location.
    pub column: u64,
}

impl Location {
    /// Returns an unknown/empty location.
    pub const fn empty() -> Self {
        Self { line: 0, column: 0 }
    }

    /// Creates a new location with the given line/column.
    pub const fn new(line: u64, column: u64) -> Self {
        Self { line, column }
    }
}

/// A span covering a start and end location (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span {
    pub start: Location,
    pub end: Location,
}

impl Span {
    /// Returns an unknown/empty span.
    pub const fn empty() -> Self {
        Self {
            start: Location::empty(),
            end: Location::empty(),
        }
    }

    /// Creates a new span from start and end locations.
    pub const fn new(start: Location, end: Location) -> Self {
        Self { start, end }
    }

    /// Returns the minimal span covering both spans.
    pub fn union(&self, other: &Span) -> Span {
        if self.start.line == 0 {
            return *other;
        }
        if other.start.line == 0 {
            return *self;
        }
        Span {
            start: core::cmp::min(self.start, other.start),
            end: core::cmp::max(self.end, other.end),
        }
    }
}

/// Trait for AST nodes that can report their source span.
pub trait Spanned {
    fn span(&self) -> Span;
}
