use bt::support::Table;
use bt::wraps;
use fdw_error::Result;
use goauth::auth::Token;
use libc::c_char;
use pg;
use serde_json;
use std::ffi::{CString, CStr};
use structs::*;

use bt::method::SampleRowKeys;
use bt::request::BTRequest;


use super::LIMIT;


pub fn _exec_foreign_insert(state: *mut BtFdwState,
                            data: *const c_char) -> Result<()> {
    let bt_fdw_state = unsafe {
        assert!(!state.is_null());
        &*state
    };
    let fdw_data: Vec<wraps::Row> = unsafe {
        assert!(!data.is_null());
        serde_json::from_str(CStr::from_ptr(data).to_str()?)?
    };
    let token = match bt_fdw_state.token {
        Ok(ref x) => x,
        Err(_) => bail!("Invalid token")
    };
    write_rows(
        Ok(fdw_data),
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
//    let _ = sample_row_keys(token, bt_fdw_state.table()?);
    match row {
        Some(r) => {
            Ok(Some(FdwRow::from(r)?))
        },
        // leave slot empty, signal postgres everything is fetched
        None => Ok(None)
    }
}

pub fn bt_fdw_state_new<T>(node: T) -> *mut BtFdwState
    where Node: From<T> {
    //    unsafe {assert!(pg::GetUserId() == curruser)};
    let node = Node::from(node);
    let ftable = FdwTable::from(node.relation);
    let fserver = FdwServer::from(ftable);
    let fuser = unsafe { FdwUser::from(fserver, pg::GetUserId()) };

    Box::into_raw(Box::new(BtFdwState {
        token: fuser.authenticate(),
        user: fuser,
        has_data: false,
        data: FdwSelectData { chunks: Vec::new() }
    }))
}

pub fn write_rows(data: Result<Vec<wraps::Row>>,
                  token: &Token,
                  table: Result<Table>) -> Result<CString> {
    let mut data = data?;
    let l = data.len();
    let table = table?;

    let _ = wraps::bulk_write_rows(&mut data, token, table)?;
    Ok(CString::new(format!("Wrote {} row(s)", l))?)
}

fn sample_row_keys(token: &Token, table: Table) -> Result<serde_json::Value> {
    let mut req = BTRequest {
        base: None,
        table: table.clone(),
        method: SampleRowKeys::new()
    };

    req.method.payload.set_table_name(format!("projects/{}/instances/{}/tables/{}",
                                              table.instance.project.name,
                                              table.instance.name,
                                              table.name));
    println!("{:?}", req.method.payload.get_table_name());
    let response = req.execute(token)?;
    println!("{:?}", response);
    Ok(response)
}
