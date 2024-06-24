// This code is called from ..\..\ffi-app\PushBuffer.cpp

use libc::{c_int,c_uchar,c_ushort};
use bytes::{BytesMut,Buf};
use super::{parse_all,Parse};

type EnvCallback = unsafe extern "C" fn(size: c_ushort, buf: *const c_uchar) -> c_int;

pub struct PushParser {
    pub call_back: Option<EnvCallback>
}

impl PushParser {
    fn new() -> Self{
        Self {
            call_back: None,
        }
    }
}

impl Parse for PushParser {
    fn parse(&self, buf: BytesMut) -> crate::Result<()> {
        let mut buf = buf;
        let token_size = buf.get_u16_le();
        println!("Token {} buf {}", token_size, buf.len());
        assert!((token_size as usize) >= buf.len());

        if let Some(cb) = self.call_back {
            unsafe {
                cb(token_size, buf.as_ptr());
            }        
        }
    
        Ok(())    
    }
}

#[no_mangle]
pub extern "C" fn register_env_callback(ptr: *mut PushParser, cb: Option<EnvCallback>) {
    let parser = unsafe {
        assert!(!ptr.is_null());
        &mut *ptr
    };

    parser.call_back = cb;
}

#[no_mangle]
pub extern "C" fn push_parser_new() -> *mut PushParser {
    Box::into_raw(Box::new(PushParser::new()))
}

#[no_mangle]
pub extern "C" fn push_parse_token(ptr: *const PushParser) -> i32 {
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
pub extern "C" fn push_parser_free(ptr: *mut PushParser) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        _ = Box::from_raw(ptr);
    }
}
