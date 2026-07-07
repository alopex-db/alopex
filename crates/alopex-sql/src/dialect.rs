/// SQL方言ごとのカスタマイズポイントを提供するトrait。
pub trait Dialect: std::fmt::Debug {
    /// 識別子の先頭として利用可能な文字かを判定する。
    fn is_identifier_start(&self, ch: char) -> bool;

    /// 識別子の残りの文字として利用可能な文字かを判定する。
    fn is_identifier_part(&self, ch: char) -> bool;

    /// 方言名。
    fn name(&self) -> &'static str;
}

/// Alopex標準のSQL方言。
#[derive(Debug, Default, Clone)]
pub struct AlopexDialect;

impl Dialect for AlopexDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch == '_' || ch.is_ascii_alphabetic()
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch == '_' || ch.is_ascii_alphanumeric()
    }

    fn name(&self) -> &'static str {
        "alopex"
    }
}
