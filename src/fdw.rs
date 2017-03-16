use bt::support::Table;
use bt::wraps;
use fdw_error::Result;
use goauth::auth::Token;
use libc::c_char;
use pg;
use serde_json;
use std::ffi::{CString, CStr};
use structs::*;

use super::LIMIT;


pub fn _exec_foreign_insert(state: *mut BtFdwState,
                            data: *const c_char) -> Result<()> {
    let bt_fdw_state = unsafe {
        assert!(!state.is_null());
        &*state
    };
    let fdw_data: FdwInsData = unsafe {
        assert!(!data.is_null());
        serde_json::from_str(CStr::from_ptr(data).to_str()?)?
    };
    let mut err_cnt = 0;
    let t_data: Vec<String> = fdw_data.data.into_iter().map(
        |ref x| match serde_json::to_string(x) {
            Ok(x) => x,
            Err(_) => {
                err_cnt += 1;
                String::from("")
            }
        }).collect();
    if err_cnt > 0 { bail!(("Failed to read all rows")) }
    let token = match bt_fdw_state.token {
        Ok(ref x) => x,
        Err(_) => bail!("Invalid token")
    };
    write_rows(
        Ok(t_data),
        Ok(&fdw_data.column),
        Ok(&fdw_data.column_qualifier),
        Some(&fdw_data.row_key),
        token,
        Ok(bt_fdw_state.table()?)
    )?;
    Ok(())
}

pub fn _iterate_foreign_scan(state: *mut BtFdwState,
                             node: *mut pg::ForeignScanState) -> Result<Option<FdwRow>> {
    let mut bt_fdw_state = unsafe {
        assert!(!state.is_null());
        &mut *state
    };
    let token = match bt_fdw_state.token {
        Ok(ref x) => x,
        Err(_) => panic!("Invalid token")
    };
    if !bt_fdw_state.has_data {
        let l = unsafe { LIMIT };
        let data = wraps::read_rows(bt_fdw_state.table()?, token, l)?;
        bt_fdw_state.data = FdwSelectData::from(data)?;
        bt_fdw_state.has_data = true
    }

    let row = bt_fdw_state.data.chunks.pop();

    let node = Node::from(node);
    unsafe {
        pg::ExecClearTuple(node.slot.expect("Expected TupleTableSlot, got None"));
    }
    match row {
        Some(r) => {
            Ok(Some(FdwRow::from(r)?))
        },
        // leave slot empty, signal postgres everything is fetched
        None => Ok(None)
    }
}

pub fn bt_fdw_state_new<T>(curruser: pg::Oid, node: T) -> *mut BtFdwState
    where Node: From<T> {
    let node = Node::from(node);
    let ftable = FdwTable::from(node.relation);
    let fserver = FdwServer::from(ftable);
    let fuser = FdwUser::from(fserver, curruser);

    Box::into_raw(Box::new(BtFdwState {
        token: fuser.authenticate(),
        user: fuser,
        has_data: false,
        data: FdwSelectData { chunks: Vec::new() }
    }))
}

pub fn write_rows(data: Result<Vec<String>>,
                  family: Result<&str>,
                  qualifier: Result<&str>,
                  row_key: Option<&str>,
                  token: &Token,
                  table: Result<Table>) -> Result<CString> {
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
