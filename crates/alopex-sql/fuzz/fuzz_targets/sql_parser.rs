#![cfg_attr(not(feature = "honggfuzz"), no_main)]

use alopex_sql::{AlopexDialect, Parser};

fn run(data: &[u8]) {
    let dialect = AlopexDialect;
    let input = String::from_utf8_lossy(data);
    let _ = Parser::parse_sql(&dialect, &input);
}

#[cfg(feature = "honggfuzz")]
fn main() {
    loop {
        honggfuzz::fuzz!(|data: &[u8]| {
            run(data);
        });
    }
}

#[cfg(not(feature = "honggfuzz"))]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    run(data);
});
