extern crate libc;
extern crate rpgffi as pg;
extern crate bigtable as bt;
extern crate serde_json;
extern crate protobuf;
extern crate goauth;
extern crate rustc_serialize;

use rustc_serialize::base64::{STANDARD, ToBase64, FromBase64};
use goauth::auth::Token;
use goauth::credentials::Credentials;
use std::io::prelude::*;
use std::fs::File;
use std::ffi::{CString, CStr};
use libc::c_char;
use bt::wraps;
use bt::utils::*;
use bt::support::{Project, Instance, Table};
use serde_json::Value;

mod error;

use error::BTErr;


fn get_ptr<'a>(p: *const c_char, l: usize) -> &'a [u8] {
    unsafe { &CStr::from_ptr(p).to_bytes()[0..l] }
}

fn str_from_ptr<'a>(p: *const c_char, l: usize) -> Result<&'a str, BTErr> {
    Ok(std::str::from_utf8(&get_ptr(p, l))?)
}

fn from_file(fp: &str) -> Result<Vec<u8>, BTErr> {
    let mut f = File::open(fp)?;
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer)?;
    Ok(buffer)
}

fn pg_return(c: CString) -> *mut pg::text {
    unsafe {
        pg::cstring_to_text(
            c.as_ptr()
        )
    }
}

fn credentials_from_db(ptr: *const c_char, l: usize) -> Result<Credentials, BTErr> {
    Ok(Credentials::from_str(
        std::str::from_utf8(
            &str_from_ptr(ptr, l)?
                .from_base64()?[..]
        )?
    )?)
}

fn ptr_to_cstring(ptr: *const c_char, l: usize) -> Result<CString, BTErr> {
    let ptr_str = str_from_ptr(ptr, l)?;
    let buffer = from_file(ptr_str)?;
    Ok(CString::new(buffer.to_base64(STANDARD))?)
}

fn token_from_credential_ptr(credentials: *const c_char, l: usize) -> Result<Token, BTErr> {
    let credentials = str_from_ptr(credentials, l)?.from_base64()?;
    Ok(get_auth_token(std::str::from_utf8(&credentials[..])?, false)?)
}

fn read_rows(table: Result<Table, BTErr>, token: Result<Token, BTErr>, lim: Option<i64>) -> Result<CString, BTErr> {
    let rows = wraps::read_rows(table?, &token?, lim)?;
    let r = serde_json::to_string(&rows)?;
    Ok(CString::new(r)?)
}

fn format_input_data(rows: Result<&str, BTErr>, split_array: bool) -> Result<Vec<String>, BTErr> {
    let mut data: Vec<String> = Vec::new();
    let rows = rows?;
    if split_array {
        let obj = serde_json::from_str::<Value>(rows)?;
        match obj.as_array() {
            Some(arr) => {
                for val in arr {
                    data.push(serde_json::to_string(&val)?)
                }
            },
            None => data.push(String::from(rows))
        }
    } else {
        data.push(String::from(rows))
    }

    Ok(data)
}

fn write_rows(data: Result<Vec<String>, BTErr>,
              family: Result<&str, BTErr>,
              qualifier: Result<&str, BTErr>,
              token: Result<Token, BTErr>,
              table: Result<Table, BTErr>) -> Result<CString, BTErr> {
    let data = data?;
    let l = data.len();
    let qualifier = qualifier?;
    let family = family?;
    let table = table?;
    let table_name = table.name.clone();

    let _ = wraps::bulk_write_rows(data, family, qualifier, None, &token?, table)?;
    Ok(CString::new(format!("Wrote {} row(s) to {}, cf: {}, cq: {}",
                            l, table_name, family, qualifier))?)
}

#[no_mangle]
pub extern "C" fn bt_rust_set_credentials(credentials: *const c_char, l: usize) -> *mut pg::text {
    match ptr_to_cstring(credentials, l) {
        Ok(x) => pg_return(x),
        Err(e) => pg_return(e.into())
    }
}

#[no_mangle]
pub extern "C" fn bt_rust_read_rows(lim: i64,
                                    credentials: *const c_char, c_l: usize,
                                    instance_ptr: *const c_char, i_l: usize,
                                    table_ptr: *const c_char, t_l: usize) -> *mut pg::text {
    let token = token_from_credential_ptr(credentials, c_l);
    println!("{}", token_from_credential_ptr(credentials, c_l).unwrap());
    let credentials = credentials_from_db(credentials, c_l);
    let instance_name = str_from_ptr(instance_ptr, i_l);
    let table_name = str_from_ptr(table_ptr, t_l);
    let table = bt_table(credentials, instance_name, table_name);

    match read_rows(table, token, Some(lim)) {
        Ok(x) => pg_return(x),
        Err(e) => pg_return(e.into())
    }
}

#[no_mangle]
pub extern "C" fn bt_rust_write_rows(c_family: *const c_char, f_l: usize,
                                     c_qualifier: *const c_char, q_l: usize,
                                     c_rows: *const c_char, r_l: usize,
                                     credentials: *const c_char, c_l: usize,
                                     instance_ptr: *const c_char, i_l: usize,
                                     table_ptr: *const c_char, t_l: usize,
                                     split_array: bool) -> *mut pg::text {
    let familiy = str_from_ptr(c_family, f_l);
    let qualifier = str_from_ptr(c_qualifier, q_l);
    let rows = str_from_ptr(c_rows, r_l);

    let token = token_from_credential_ptr(credentials, c_l);
    let credentials = credentials_from_db(credentials, c_l);

    let instance_name = str_from_ptr(instance_ptr, i_l);
    let table_name = str_from_ptr(table_ptr, t_l);

    let table = bt_table(credentials, instance_name, table_name);

    let data = format_input_data(rows, split_array);

    match write_rows(data, familiy, qualifier, token, table) {
        Ok(x) => pg_return(x),
        Err(e) => pg_return(e.into())
    }
}

fn bt_table(credentials: Result<Credentials, BTErr>,
            instance_name: Result<&str, BTErr>,
            table_name: Result<&str, BTErr>)
            -> Result<Table, BTErr> {
    let project = Project { name: credentials?.project() };
    let instance = Instance { project: project, name: String::from(instance_name?) };
    Ok(Table { instance: instance, name: String::from(table_name?) })
}
