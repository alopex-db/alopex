#[derive(Debug, Clone, Copy)]
pub enum Precedence {
    Period,
    MulDivMod,
    PlusMinus,
    StringConcat,
    Comparison,
    Between,
    Like,
    Is,
    UnaryNot,
    And,
    Or,
}

impl Precedence {
    pub fn value(self) -> u8 {
        match self {
            Precedence::Period => 100,
            Precedence::MulDivMod => 40,
            Precedence::PlusMinus => 30,
            Precedence::StringConcat => 25,
            Precedence::Comparison => 20,
            Precedence::Between => 20,
            Precedence::Like => 19,
            Precedence::Is => 17,
            Precedence::UnaryNot => 15,
            Precedence::And => 10,
            Precedence::Or => 5,
        }
    }
}

pub const PREC_UNKNOWN: u8 = 0;
