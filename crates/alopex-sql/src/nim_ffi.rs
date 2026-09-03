use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::slice;
use std::sync::Once;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ParseResultKind {
    Ok = 0,
    Error = 1,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CParseResult {
    pub kind: ParseResultKind,
    pub buffer_ptr: *mut c_void,
    pub buffer_len: c_int,
    pub error_ptr: *mut c_char,
    pub error_len: c_int,
}

#[link(name = "alopex_sql_parser")]
unsafe extern "C" {
    fn alopex_parser_init();
    fn alopex_parse_sql(input: *const c_char, length: c_int) -> CParseResult;
    fn alopex_free_buffer(p: *mut c_void);
    fn alopex_parser_version() -> *const c_char;
}

static INIT: Once = Once::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParseInputError {
    InteriorNul,
    LengthOutOfRange,
}

fn checked_c_int_len(len: usize) -> Result<c_int, ParseInputError> {
    c_int::try_from(len).map_err(|_| ParseInputError::LengthOutOfRange)
}

fn prepare_sql_input(sql: &str) -> Result<(CString, c_int), ParseInputError> {
    let len = checked_c_int_len(sql.len())?;
    if sql.as_bytes().contains(&0) {
        return Err(ParseInputError::InteriorNul);
    }
    let input = CString::new(sql).map_err(|_| ParseInputError::InteriorNul)?;
    Ok((input, len))
}

pub fn parse_sql(sql: &str) -> Result<CParseResult, ParseInputError> {
    let (input, len) = prepare_sql_input(sql)?;
    INIT.call_once(|| unsafe {
        alopex_parser_init();
    });

    Ok(unsafe { alopex_parse_sql(input.as_ptr(), len) })
}

pub fn parser_contract_version() -> String {
    INIT.call_once(|| unsafe {
        alopex_parser_init();
    });

    let version = unsafe { alopex_parser_version() };
    if version.is_null() {
        return String::new();
    }
    unsafe { CStr::from_ptr(version) }
        .to_string_lossy()
        .into_owned()
}

pub struct OwnedBuffer {
    ptr: *mut c_void,
    len: usize,
}

impl OwnedBuffer {
    pub fn new(ptr: *mut c_void, len: c_int) -> Self {
        Self {
            ptr,
            len: usize::try_from(len).unwrap_or(0),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        if self.ptr.is_null() || self.len == 0 {
            return &[];
        }
        unsafe { slice::from_raw_parts(self.ptr.cast::<u8>(), self.len) }
    }
}

impl Drop for OwnedBuffer {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { alopex_free_buffer(self.ptr) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostile_length_is_fallible_without_allocating_a_huge_string() {
        let max = usize::try_from(c_int::MAX).expect("c_int::MAX fits usize");
        assert_eq!(checked_c_int_len(max), Ok(c_int::MAX));
        assert_eq!(
            checked_c_int_len(max + 1),
            Err(ParseInputError::LengthOutOfRange)
        );
        assert_eq!(
            checked_c_int_len(usize::MAX),
            Err(ParseInputError::LengthOutOfRange)
        );
    }

    #[test]
    fn low_level_adapter_rejects_nul_without_panicking() {
        assert_eq!(
            prepare_sql_input("SELECT \0 1").map(|_| ()),
            Err(ParseInputError::InteriorNul)
        );
    }
}
