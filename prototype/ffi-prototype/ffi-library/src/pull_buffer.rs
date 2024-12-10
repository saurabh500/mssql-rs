// This code is called from ..\..\ffi-app\PullBuffer.cpp

use super::{parse_all, Parse};
use bytes::BytesMut;
use libc::{c_int, c_uchar, c_uint, c_ushort};

type ParseCallback = unsafe extern "C" fn(ptr: *mut ParseReader) -> c_int;

pub struct ParseReader {
    buffer: BytesMut,
}

impl ParseReader {
    fn new(buffer: BytesMut) -> Self {
        Self { buffer }
    }

    pub fn get_buffer(&mut self, size: c_ushort, ptr: *mut c_uchar) -> c_uint {
        if self.buffer.len() < size as usize {
            return 1;
        }

        let buf = self.buffer.split_to(size as usize);
        unsafe {
            std::ptr::copy(buf.as_ptr().cast(), ptr, size as usize);
        }

        0
    }
}

#[no_mangle]
pub extern "C" fn get_buffer(reader_ptr: *mut ParseReader, size: c_ushort, ptr: *mut c_uchar) {
    let reader = unsafe {
        assert!(!reader_ptr.is_null());
        &mut *reader_ptr
    };

    reader.get_buffer(size, ptr);
}

pub struct PullParser {
    pub call_back: Option<ParseCallback>,
}

impl PullParser {
    fn new() -> Self {
        Self { call_back: None }
    }
}

impl Parse for PullParser {
    fn parse(&self, buf: BytesMut) -> crate::Result<()> {
        let mut readed = ParseReader::new(buf);

        if let Some(cb) = self.call_back {
            unsafe {
                cb(&mut readed as *mut ParseReader);
            }
        }

        Ok(())
    }
}

#[no_mangle]
pub extern "C" fn register_parse_callback(ptr: *mut PullParser, cb: Option<ParseCallback>) {
    let parser = unsafe {
        assert!(!ptr.is_null());
        &mut *ptr
    };

    parser.call_back = cb;
}

#[no_mangle]
pub extern "C" fn pull_parser_new() -> *mut PullParser {
    Box::into_raw(Box::new(PullParser::new()))
}

#[no_mangle]
pub extern "C" fn pull_parse_token(ptr: *const PullParser) -> i32 {
    let parser = unsafe {
        assert!(!ptr.is_null());
        &*ptr
    };

    let result = parse_all(parser);
    match result {
        Ok(()) => 0,
        Err(_) => 1,
    }
}

#[no_mangle]
pub extern "C" fn pull_parser_free(ptr: *mut PullParser) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        _ = Box::from_raw(ptr);
    }
}
