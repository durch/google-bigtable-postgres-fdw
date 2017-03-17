use fdw::{_exec_foreign_insert, bt_fdw_state_new, _iterate_foreign_scan};
use libc::{c_char};
use pg;
use serde_json;
use structs::{BtFdwState, Node};

use super::LIMIT;

// Generics are hard in C :)
#[no_mangle]
pub extern "C" fn bt_fdw_state_from_fss(fss: *mut pg::ForeignScanState) -> *mut BtFdwState {
    bt_fdw_state_new(fss)
}

#[no_mangle]
pub extern "C" fn bt_fdw_state_from_relinfo(rinfo: *mut pg::ResultRelInfo) -> *mut BtFdwState {
    bt_fdw_state_new(rinfo)
}

#[no_mangle]
pub unsafe extern "C" fn get_limit(plan_info: *mut pg::PlannerInfo) {
    let limit = (*plan_info).limit_tuples;
    LIMIT = {
        if limit == -1. {
            None
        } else {
            Some(limit as i64)
        }
    };
}

#[no_mangle]
pub extern "C" fn bt_fdw_exec_foreign_insert(state: *mut BtFdwState, data: *const c_char) {
    let _ = _exec_foreign_insert(state, data);
}

#[no_mangle]
pub extern "C" fn bt_fdw_iterate_foreign_scan(state: *mut BtFdwState, node: *mut pg::ForeignScanState) {
    match _iterate_foreign_scan(state, node) {
        Ok(x) => match x {
            Some(row) => Node::from(node).assign_slot(
                match serde_json::to_string(&row) {
                    Ok(x) => x,
                    Err(e) => panic!(e)
                }
            ),
            None => {}
        },
        Err(e) => panic!(e)
    }
}