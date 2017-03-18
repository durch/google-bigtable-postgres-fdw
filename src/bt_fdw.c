#include "bt_fdw.h"

PG_MODULE_MAGIC;

extern Datum bt_fdw_handler(PG_FUNCTION_ARGS);
extern Datum bt_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bt_fdw_handler);
PG_FUNCTION_INFO_V1(bt_fdw_validator);

Datum
bt_fdw_handler(PG_FUNCTION_ARGS) {
    FdwRoutine *fdw_routine = makeNode(FdwRoutine);
    fdw_routine->GetForeignRelSize = bt_fdw_get_foreign_rel_size;
    fdw_routine->GetForeignPaths = bt_fdw_get_foreign_paths;
    fdw_routine->GetForeignPlan = bt_fdw_get_foreign_plan;
    fdw_routine->ExplainForeignScan = bt_fdw_explain_foreign_scan;
    fdw_routine->BeginForeignScan = bt_fdw_begin_foreign_scan;
    fdw_routine->IterateForeignScan = bt_fdw_iterate_foreign_scan;
    fdw_routine->ReScanForeignScan = bt_fdw_rescan_foreign_scan;
    fdw_routine->EndForeignScan = bt_fdw_end_foreign_scan;
    fdw_routine->AnalyzeForeignTable = NULL;
    fdw_routine->IsForeignRelUpdatable = bt_is_foreign_rel_updatable;
    fdw_routine->AddForeignUpdateTargets = bt_fdw_add_foreign_update_targets;
    fdw_routine->PlanForeignModify = bt_fdw_plan_foreign_modify;
    fdw_routine->BeginForeignModify = bt_fdw_begin_foreign_modify;
    fdw_routine->ExecForeignInsert = bt_fdw_exec_foreign_insert;
    fdw_routine->ExecForeignUpdate = bt_fdw_exec_foreign_update;
    fdw_routine->ExecForeignDelete = bt_fdw_exec_foreign_delete;
    fdw_routine->EndForeignModify = bt_fdw_end_foreign_modify;

    PG_RETURN_POINTER(fdw_routine);
}

Datum
bt_fdw_validator(PG_FUNCTION_ARGS) {
    PG_RETURN_BOOL(true);
}