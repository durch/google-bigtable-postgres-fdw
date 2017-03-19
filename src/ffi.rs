use fdw::{_exec_foreign_insert, bt_fdw_state_new, _iterate_foreign_scan};
use pg::*;
use serde_json;
use structs::{BtFdwState, Node};
use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;
use std::error::Error;

use super::LIMIT;

#[no_mangle]
pub unsafe extern "C" fn fdw_assign_callbacks(fdw_routine: *mut FdwRoutine) {
    (*fdw_routine).GetForeignRelSize = Some(bt_fdw_get_foreign_rel_size);
    (*fdw_routine).GetForeignPaths = Some(bt_fdw_get_foreign_paths);
    (*fdw_routine).GetForeignPlan = Some(bt_fdw_get_foreign_plan);
    (*fdw_routine).ExplainForeignScan = Some(bt_fdw_explain_foreign_scan);
    (*fdw_routine).BeginForeignScan = Some(bt_fdw_begin_foreign_scan);
    (*fdw_routine).IterateForeignScan = Some(bt_fdw_iterate_foreign_scan);
    (*fdw_routine).ReScanForeignScan = Some(bt_fdw_rescan_foreign_scan);
    (*fdw_routine).EndForeignScan = Some(bt_fdw_end_foreign_scan);
    (*fdw_routine).AnalyzeForeignTable = None;
    (*fdw_routine).IsForeignRelUpdatable = Some(bt_is_foreign_rel_updatable);
    (*fdw_routine).AddForeignUpdateTargets = Some(bt_fdw_add_foreign_update_targets);
    (*fdw_routine).PlanForeignModify = Some(bt_fdw_plan_foreign_modify);
    (*fdw_routine).BeginForeignModify = Some(bt_fdw_begin_foreign_modify);
    (*fdw_routine).ExecForeignInsert = Some(bt_fdw_exec_foreign_insert);
    (*fdw_routine).ExecForeignUpdate = Some(bt_fdw_exec_foreign_update);
    (*fdw_routine).ExecForeignDelete = Some(bt_fdw_exec_foreign_delete);
    (*fdw_routine).EndForeignModify = Some(bt_fdw_end_foreign_modify);
}

#[no_mangle]
pub unsafe extern "C" fn bt_is_foreign_rel_updatable(rel: Relation) -> i32 {
    (0 << CmdType::CMD_UPDATE as u8) | (1 << CmdType::CMD_INSERT as u8) | (0 << CmdType::CMD_DELETE as u8)
}

#[no_mangle]
pub unsafe extern "C" fn bt_fdw_end_foreign_modify(estate: *mut EState,
                                                   rinfo: *mut ResultRelInfo) {}


#[no_mangle]
pub unsafe extern "C" fn bt_fdw_exec_foreign_delete(estate: *mut EState,
                                                    rinfo: *mut ResultRelInfo,
                                                    slot: *mut TupleTableSlot,
                                                    plan_slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    unimplemented!();
}


#[no_mangle]
pub unsafe extern "C" fn bt_fdw_exec_foreign_update(estate: *mut EState,
                                                    rinfo: *mut ResultRelInfo,
                                                    slot: *mut TupleTableSlot,
                                                    plan_slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    unimplemented!();
}

#[no_mangle]
pub unsafe extern "C" fn bt_fdw_plan_foreign_modify(root: *mut PlannerInfo,
                                                    plan: *mut ModifyTable,
                                                    result_relation: Index,
                                                    subplan_index: i32) -> *mut List {
    let l: *mut List = ptr::null_mut();
    l
}


#[no_mangle]
pub unsafe extern "C" fn bt_fdw_add_foreign_update_targets(parsetree: *mut Query,
                                                           target_rte: *mut RangeTblEntry,
                                                           target_relation: *mut RelationData) {}

#[no_mangle]
pub unsafe extern "C" fn bt_fdw_begin_foreign_modify(mtable: *mut ModifyTableState,
                                                     rinfo: *mut ResultRelInfo,
                                                     fdw_private: *mut List,
                                                     subplan_index: i32,
                                                     eflags: i32) {
    if (eflags & 0x0001) == 0x0001 {
        return
    }

    (*rinfo).ri_FdwState = bt_fdw_state_new(rinfo) as *mut c_void;
}

#[no_mangle]
pub extern "C" fn bt_fdw_rescan_foreign_scan(node: *mut ForeignScanState) {}

#[no_mangle]
pub extern "C" fn bt_fdw_end_foreign_scan(node: *mut ForeignScanState) {}

#[no_mangle]
pub unsafe extern "C" fn bt_fdw_get_foreign_rel_size(root: *mut PlannerInfo,
                                                     baserel: *mut RelOptInfo,
                                                     foreigntableid: Oid) {
    (*baserel).rows = 1.;
}

#[no_mangle]
pub unsafe extern "C" fn bt_fdw_get_foreign_paths(root: *mut PlannerInfo,
                                                  baserel: *mut RelOptInfo,
                                                  foreigntableid: Oid) {
    let target: *mut PathTarget = ptr::null_mut();
    let relids: *mut Bitmapset = ptr::null_mut();
    let path: *mut Path = ptr::null_mut();
    let null: *mut List = ptr::null_mut();

    let fs_path = create_foreignscan_path(root, baserel, target,
                                          (*baserel).rows, 0., 0., null, relids, path, null);

    add_path(baserel, fs_path as *mut Path)
}

#[no_mangle]
pub unsafe extern "C" fn bt_fdw_get_foreign_plan(root: *mut PlannerInfo,
                                                 baserel: *mut RelOptInfo,
                                                 foreigntableid: Oid,
                                                 best_path: *mut ForeignPath,
                                                 tlist: *mut List,
                                                 scan_clauses: *mut List,
                                                 outer_plan: *mut Plan) -> *mut ForeignScan {
    let scan_relid = (*baserel).relid;
    get_limit(root);
    let scan_clauses = extract_actual_clauses(scan_clauses, 0);

    let null: *mut List = ptr::null_mut();

    make_foreignscan(tlist,
                     scan_clauses,
                     scan_relid,
                     null, /* no expressions to evaluate */
                     null, /* no private state either */
                     null, /* no custom tlist */
                     null, /* no remote quals */
                     outer_plan)
}

#[no_mangle]
pub unsafe extern "C" fn bt_fdw_explain_foreign_scan(node: *mut ForeignScanState, es: *mut ExplainState) {
    ExplainPropertyText(CString::new("foreign table").unwrap().as_ptr(),
                        CString::new("bt").unwrap().as_ptr(),
                        es)
}

#[no_mangle]
pub unsafe extern "C" fn bt_fdw_begin_foreign_scan(node: *mut ForeignScanState, eflags: i32) {
    if (eflags & 0x0001) == 0x0001 {
        return
    }
    (*node).fdw_state = bt_fdw_state_new(node) as *mut c_void;
}

// Generics are hard in C :)
#[no_mangle]
pub extern "C" fn bt_fdw_state_from_fss(fss: *mut ForeignScanState) -> *mut BtFdwState {
    bt_fdw_state_new(fss)
}

#[no_mangle]
pub extern "C" fn bt_fdw_state_from_relinfo(rinfo: *mut ResultRelInfo) -> *mut BtFdwState {
    bt_fdw_state_new(rinfo)
}

#[no_mangle]
pub unsafe extern "C" fn get_limit(plan_info: *mut PlannerInfo) {
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
pub unsafe extern "C" fn bt_fdw_exec_foreign_insert(estate: *mut EState,
                                                    rinfo: *mut ResultRelInfo,
                                                    slot: *mut TupleTableSlot,
                                                    plan_slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    let mut isnull: bool_ = 0;
    let val = slot_getattr(slot, 1, &mut isnull);
    let data = text_to_cstring(val as *const varlena);
    let _ = _exec_foreign_insert((*rinfo).ri_FdwState as *mut BtFdwState, data);
    slot
}

//#[no_mangle]
//pub extern "C" fn bt_fdw_exec_foreign_insert2(state: *mut BtFdwState, data: *const c_char) {
//    let _ = _exec_foreign_insert(state, data);
//}

#[no_mangle]
pub extern "C" fn bt_fdw_iterate_foreign_scan(node: *mut ForeignScanState) -> *mut TupleTableSlot {
    unsafe {
        match _iterate_foreign_scan((*node).fdw_state as *mut BtFdwState, node) {
            Ok(x) => match x {
                Some(row) => Node::from(node).assign_slot(
                    match serde_json::to_string(&row) {
                        Ok(x) => x,
                        Err(e) => return_err(e)
                    }
                ),
                None => {}
            },
            Err(e) => Node::from(node).assign_slot(return_err(e))
        }
        (*node).ss.ss_ScanTupleSlot
    }
}

fn return_err<T: Error>(e: T) -> String {
    serde_json::to_string(&format!("{}: {:?}", e.description(), e.cause()))
        .unwrap_or(String::from("An unknown error has occured."))
}