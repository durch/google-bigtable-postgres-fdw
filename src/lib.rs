extern crate libc;
//extern crate pgffi as pg;
#[macro_use]
extern crate serde_derive;
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
use bt::method::SampleRowKeys;
use bt::request::BTRequest;
use bt::utils::*;
use bt::support::{Project, Instance, Table};
use serde_json::Value;

mod error;
mod pg;

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

fn read_rows(table: Result<Table, BTErr>, token: &Token, lim: Option<i64>) -> Result<CString, BTErr> {
    let rows = wraps::read_rows(table?, token, lim)?;
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
              row_key: Option<&str>,
              token: &Token,
              table: Result<Table, BTErr>) -> Result<CString, BTErr> {
    let data = data?;
    let l = data.len();
    let qualifier = qualifier?;
    let family = family?;
    let table = table?;
    let table_name = table.name.clone();

    let _ = wraps::bulk_write_rows(data, family, qualifier, row_key, token, table)?;
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
    let credentials = credentials_from_db(credentials, c_l);
    let instance_name = str_from_ptr(instance_ptr, i_l);
    let table_name = str_from_ptr(table_ptr, t_l);
    let table = bt_table(credentials, instance_name, table_name);

    match read_rows(table, &token.unwrap(), Some(lim)) {
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

    match write_rows(data, familiy, qualifier, None, &token.unwrap(), table) {
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

// Extension stuff

#[derive(Debug)]
pub struct BtFdwState {
    token: Result<Token, BTErr>,
    user: FdwUser
}

impl BtFdwState {
    fn project(&self) -> Project {
        Project {
            name: get_option("project", &self.user.server.options.as_slice()).unwrap().value
        }
    }

    fn instance(&self) -> Instance {
        Instance {
            name: get_option("instance", &self.user.server.options.as_slice()).unwrap().value,
            project: self.project()
        }
    }

    fn table(&self) -> Table {
        Table {
            name: get_option("name", &self.user.server.table.options.as_slice()).unwrap().value,
            instance: self.instance()
        }
    }
}

#[derive(Debug)]
struct FdwUser {
    id: pg::Oid,
    server: FdwServer,
    credentials: Option<Credentials>,
    options: Vec<FdwOpt>,
    valid_options: Vec<String>
}

impl FdwUser {
    fn from(srv: FdwServer, curruser: pg::Oid) -> Self {
        let valid_options = vec!(String::from("credentials_path"));
        let fuser = unsafe {
            let fu = pg::GetUserMapping(curruser, srv.id);
            assert!(!fu.is_null());
            *fu
        };
        let opts = extract_options(fuser.options, &valid_options.as_slice());
        let creds = Credentials::from_file(&get_option("credentials_path", &opts.as_slice()).unwrap().value).unwrap();
        FdwUser {
            id: fuser.userid,
            server: srv,
            options: opts,
            valid_options: valid_options,
            credentials: Some(creds)
        }
    }

    fn authenticate(&self) -> Result<Token, BTErr> {
        Ok(get_auth_token(&self.options.first().unwrap().value, true)?)
    }
}

#[derive(Debug)]
struct FdwTable {
    id: pg::Oid,
    options: Vec<FdwOpt>,
    valid_options: Vec<String>,
    relation: Relation,
    server_id: pg::Oid
}

impl From<Relation> for FdwTable {
    fn from(rel: Relation) -> Self {
        let valid_options = vec!(String::from("name"));
        let ftable: pg::ForeignTable = unsafe {
            let ft = pg::GetForeignTable(rel.id);
            assert!(!ft.is_null());
            *ft
        };
        FdwTable {
            id: ftable.relid,
            options: extract_options(ftable.options, &valid_options),
            valid_options: valid_options,
            relation: rel,
            server_id:
            ftable.serverid
        }
    }
}

#[derive(Debug)]
struct FdwServer {
    id: pg::Oid,
    options: Vec<FdwOpt>,
    valid_options: Vec<String>,
    table: FdwTable,
}

impl From<FdwTable> for FdwServer {
    fn from(table: FdwTable) -> Self {
        let valid_options = vec!(String::from("instance"), String::from("project"));
        let fserver: pg::ForeignServer = unsafe {
            let fs = pg::GetForeignServer(table.server_id);
            assert!(!fs.is_null());
            *fs
        };
        FdwServer {
            id: fserver.serverid,
            options: extract_options(fserver.options, &valid_options),
            valid_options: valid_options,
            table: table
        }
    }
}

fn get_option(target: &str, options: &[FdwOpt]) -> Option<FdwOpt> {
    for option in options {
        if &option.name == target {
            return Some(option.clone())
        }
    }
    None
}

#[derive(Debug, Clone)]
struct FdwOpt {
    name: String,
    value: String
}

impl FdwOpt {
    fn name(&self) -> &str {
        &self.name
    }
}

impl From<*mut pg::DefElem> for FdwOpt {
    fn from(def: *mut pg::DefElem) -> Self {
        let defname;
        let str_val;
        unsafe {
            assert!(!def.is_null());
            let def = *def;
            defname = CStr::from_ptr(def.defname);
            let val = def.arg as *mut pg::Value;
            assert!(!val.is_null());
            let str_ptr = *(*val).val.str.as_mut();
            assert!(!str_ptr.is_null());
            str_val = CStr::from_ptr(str_ptr);
        }
        FdwOpt { name: defname.to_str().unwrap().to_string(), value: str_val.to_str().unwrap().to_string() }
    }
}

#[derive(Debug)]
struct Relation {
    id: pg::Oid,
    pg_rel: pg::RelationData
}

impl From<*mut pg::RelationData> for Relation {
    fn from(rd: *mut pg::RelationData) -> Self {
        let rd = unsafe {
            assert!(!rd.is_null());
            *rd
        };
        Relation {
            id: rd.rd_id,
            pg_rel: rd
        }
    }
}

#[derive(Debug)]
struct Node {
    relation: Relation
}

impl From<*mut pg::ForeignScanState> for Node {
    fn from(fss: *mut pg::ForeignScanState) -> Self {
        let relation = unsafe {
            assert!(!fss.is_null());
            Relation::from((*fss).ss.ss_currentRelation)
        };
        Node { relation: relation }
    }
}

impl From<*mut pg::ResultRelInfo> for Node {
    fn from(rri: *mut pg::ResultRelInfo) -> Self {
        let relation = unsafe {
            assert!(!rri.is_null());
            Relation::from((*rri).ri_RelationDesc)
        };
        Node { relation: relation }
    }
}

#[no_mangle]
pub extern fn bt_fdw_state_free(ptr: *mut BtFdwState) {
    if ptr.is_null() { return }
    unsafe { Box::from_raw(ptr); }
}

fn extract_options(opts: *mut pg::List, opts_to_get: &[String]) -> Vec<FdwOpt> {
    let mut out_opts = Vec::new();
    let opts = unsafe {
        assert!(!opts.is_null());
        *opts
    };
    let l = opts.length;
    for opt in 0..l {
        let cell;
        let def;
        unsafe {
            let cell_ptr = pg::list_nth_cell(&opts, opt);
            assert!(!cell_ptr.is_null());
            cell = (*cell_ptr).data.ptr_value.as_mut();
            def = *cell as *mut pg::DefElem;
        };
        let opt = FdwOpt::from(def);
        for opt_to_get in opts_to_get {
            if opt_to_get == opt.name() {
                out_opts.push(opt);
                break;
            }
        }
    }
    out_opts
}

// Generics are hard in C :)
#[no_mangle]
pub extern "C" fn bt_fdw_state_from_fss(curruser: pg::Oid, fss: *mut pg::ForeignScanState) -> *mut BtFdwState {
    bt_fdw_state_new(curruser, fss)
}

#[no_mangle]
pub extern "C" fn bt_fdw_state_from_relinfo(curruser: pg::Oid, rinfo: *mut pg::ResultRelInfo) -> *mut BtFdwState {
    bt_fdw_state_new(curruser, rinfo)
}

fn bt_fdw_state_new<T>(curruser: pg::Oid, node: T) -> *mut BtFdwState
    where Node: From<T> {
    let node = Node::from(node);
    let ftable = FdwTable::from(node.relation);
    let fserver = FdwServer::from(ftable);
    let fuser = FdwUser::from(fserver, curruser);

    Box::into_raw(Box::new(BtFdwState { token: fuser.authenticate(), user: fuser }))
}


#[derive(Debug, Serialize, Deserialize)]
struct FdwInsData {
    row_key: String,
    column: String,
    column_qualifier: String,
    data: Vec<serde_json::Value>
}

#[no_mangle]
pub extern "C" fn bt_fdw_exec_foreign_insert(state: *mut BtFdwState, slot: *mut pg::TupleTableSlot, data: *const c_char) {
    let bt_fdw_state = unsafe {
        assert!(!state.is_null());
        &*state
    };
    let tuple_slot = unsafe {
        assert!(!slot.is_null());
        &*slot
    };
    let fdw_data: FdwInsData = unsafe {
        assert!(!data.is_null());
        serde_json::from_str(CStr::from_ptr(data).to_str().unwrap()).unwrap()
    };
    let t_data: Vec<String> = fdw_data.data.into_iter().map(|ref x| serde_json::to_string(x).unwrap()).collect();
    let token = match bt_fdw_state.token {
        Ok(ref x) => x,
        Err(ref e) => panic!(e)
    };
     match write_rows(
        Ok(t_data),
        Ok(&fdw_data.column),
        Ok(&fdw_data.column_qualifier),
        Some(&fdw_data.row_key),
        token,
        Ok(bt_fdw_state.table())) {
         Ok(x) => println!("{:?}",x),
         Err(e) => println!("{:?}", e)
     }


}

#[no_mangle]
pub extern "C" fn bt_fdw_iterate_foreign_scan(state: *mut BtFdwState, node: *mut pg::ForeignScanState) {
    let bt_fdw_state = unsafe {
        assert!(!state.is_null());
        &*state
    };
    let att;
    let slot;
    let token = match bt_fdw_state.token {
        Ok(ref x) => x,
        Err(ref e) => panic!(e)
    };
    println!("{:?}", sample_row_keys(&token, bt_fdw_state.table()));
    let row = read_rows(
        Ok(bt_fdw_state.table()),
        &token,
        Some(100)
    ).unwrap();
    unsafe {
        let relation = (*node).ss.ss_currentRelation;
        att = (*relation).rd_att;
        slot = (*node).ss.ss_ScanTupleSlot;
        pg::ExecClearTuple(slot);
    }
    unsafe {
        let attinmeta = pg::TupleDescGetAttInMetadata(att);
        let tuple = pg::BuildTupleFromCStrings(attinmeta, &mut row.into_raw());
        pg::ExecStoreTuple(tuple, slot, 0, 0);
    }
}

fn sample_row_keys(token: &Token, table: Table) -> Result<serde_json::Value, BTErr> {
    let req = BTRequest {
        base: None,
        table: table,
        method: SampleRowKeys::new()
    };
    let response = req.execute(token)?;
    Ok(response)
}


