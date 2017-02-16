use std;
use goauth::error::GOErr as go_err;
use serde_json::Error as serde_err;
use std::str::Utf8Error as utf8_err;
use std::ffi::NulError as null_err;
use bt::error::BTErr as bt_err;
use std::io::Error as io_err;
use rustc_serialize::base64::FromBase64Error as b64_err;
use std::ffi::CString;
use std::error::Error;


macro_rules! impl_from {
    ($type_: ident, $enum_ty: ident) => {
        impl From<$type_> for BTErr {
            fn from(e: $type_) -> BTErr {
                BTErr::$enum_ty(e)
            }
        }
    }
}

#[derive(Debug)]
pub enum BTErr {
    GOErr(go_err),
    SerdeErr(serde_err),
    UTF8Err(utf8_err),
    NullErr(null_err),
    BTEr(bt_err),
    B64Err(b64_err),
    IOErr(io_err)
}

impl_from!(go_err, GOErr);
impl_from!(serde_err, SerdeErr);
impl_from!(utf8_err, UTF8Err);
impl_from!(null_err, NullErr);
impl_from!(bt_err, BTEr);
impl_from!(b64_err, B64Err);
impl_from!(io_err, IOErr);

impl BTErr {
    fn core<'a>(&self) -> &'a str {
        match *self {
            BTErr::GOErr(_) => "Auth Err",
            BTErr::SerdeErr(_) => "Serde Err",
            BTErr::UTF8Err(_) => "UTF8 Err",
            BTErr::NullErr(_) => "NULL Err",
            BTErr::BTEr(_) => "Bigtable Err",
            BTErr::B64Err(_) => "Base64 Err",
            BTErr::IOErr(_) => "IO Err",
        }
    }
}

impl std::fmt::Display for BTErr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            BTErr::GOErr(ref e) => e.fmt(f),
            BTErr::SerdeErr(ref e) => e.fmt(f),
            BTErr::UTF8Err(ref e) => e.fmt(f),
            BTErr::NullErr(ref e) => e.fmt(f),
            BTErr::BTEr(ref e) => e.fmt(f),
            BTErr::B64Err(ref e) => e.fmt(f),
            BTErr::IOErr(ref e) => e.fmt(f),
        }
    }
}

impl std::error::Error for BTErr {
    fn description(&self) -> &str {
        match *self {
            BTErr::GOErr(ref e) => e.description(),
            BTErr::SerdeErr(ref e) => e.description(),
            BTErr::UTF8Err(ref e) => e.description(),
            BTErr::NullErr(ref e) => e.description(),
            BTErr::BTEr(ref e) => e.description(),
            BTErr::B64Err(ref e) => e.description(),
            BTErr::IOErr(ref e) => e.description(),
        }
    }
}

impl Into<CString> for BTErr {
    fn into(self) -> CString {
        CString::new(format!("{}:{} - {}", self.core(), self.description(), self)).unwrap()
    }
}