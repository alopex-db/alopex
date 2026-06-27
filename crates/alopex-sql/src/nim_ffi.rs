use std::ffi::{CString, c_char, c_int, c_void};
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

unsafe extern "C" {
    fn alopex_parser_init();
    fn alopex_parse_sql(input: *const c_char, length: c_int) -> CParseResult;
    fn alopex_free_buffer(p: *mut c_void);
}

static INIT: Once = Once::new();

pub fn parse_sql(sql: &str) -> CParseResult {
    INIT.call_once(|| unsafe {
        alopex_parser_init();
    });

    let input = CString::new(sql).expect("SQL input contains an interior NUL byte");
    let len = c_int::try_from(sql.len()).expect("SQL input is too large for Nim parser FFI");
    unsafe { alopex_parse_sql(input.as_ptr(), len) }
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
