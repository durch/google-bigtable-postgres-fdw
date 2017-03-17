#include "bt_fdw.h"

PG_MODULE_MAGIC;

/*
 * Handler and Validator functions
 */
extern Datum bt_fdw_handler(PG_FUNCTION_ARGS);
extern Datum bt_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bt_fdw_handler);
PG_FUNCTION_INFO_V1(bt_fdw_validator);

/*
 * FDW functions implementation
 */

Datum
bt_fdw_handler(PG_FUNCTION_ARGS) {
    FdwRoutine *fdw_routine = makeNode(FdwRoutine);
    fdw_routine->GetForeignRelSize = btGetForeignRelSize;
    fdw_routine->GetForeignPaths = btGetForeignPaths;
    fdw_routine->GetForeignPlan = btGetForeignPlan;
    fdw_routine->ExplainForeignScan = btExplainForeignScan;
    fdw_routine->BeginForeignScan = btFdwBeginForeignScan;
    fdw_routine->IterateForeignScan = btFdWIterateForeignScan;
    fdw_routine->ReScanForeignScan = btReScanForeignScan;
    fdw_routine->EndForeignScan = btEndForeignScan;
    fdw_routine->AnalyzeForeignTable = NULL;
    fdw_routine->IsForeignRelUpdatable = btIsForeignRelUpdatable;
    fdw_routine->AddForeignUpdateTargets = btAddForeignUpdateTargets;        /* U D */
    fdw_routine->PlanForeignModify = btPlanForeignModify; /* I U D */
    fdw_routine->BeginForeignModify = btBeginForeignModify;        /* I U D */
    fdw_routine->ExecForeignInsert = btExecForeignInsert; /* I */
    fdw_routine->ExecForeignUpdate = btExecForeignUpdate; /* U */
    fdw_routine->ExecForeignDelete = btExecForeignDelete; /* D */
    fdw_routine->EndForeignModify = btEndForeignModify;    /* I U D */

    PG_RETURN_POINTER(fdw_routine);
}

Datum
bt_fdw_validator(PG_FUNCTION_ARGS) {
    PG_RETURN_BOOL(true);
}

static void
btGetForeignRelSize(PlannerInfo *root,
                    RelOptInfo *baserel,
                    Oid foreigntableid) {
    elog(LOG, "entering function %s", __func__);

    baserel->rows = 1;
}

static void
btGetForeignPaths(PlannerInfo *root,
                  RelOptInfo *baserel,
                  Oid foreigntableid) {
    Cost total_cost, startup_cost;
    elog(LOG, "entering function %s", __func__);
    startup_cost = 10;
    total_cost = startup_cost + baserel->rows;

    /* Create a ForeignPath node and add it as only possible path */
    add_path(baserel, (Path *)
            create_foreignscan_path(root, baserel,
                                    NULL,      /* default pathtarget */
                                    baserel->rows,
                                    startup_cost,
                                    total_cost,
                                    NIL,        /* no pathkeys */
                                    NULL,        /* no outer rel either */
                                    NULL,      /* no extra plan */
                                    NIL));        /* no fdw_private data */
}

static ForeignScan *
btGetForeignPlan(PlannerInfo *root,
                 RelOptInfo *baserel,
                 Oid foreigntableid,
                 ForeignPath *best_path,
                 List *tlist,
                 List *scan_clauses,
                 Plan *outer_plan) {

    elog(LOG, "entering function %s", __func__);

    Index scan_relid = baserel->relid;

    get_limit(root);

    scan_clauses = extract_actual_clauses(scan_clauses, false);

    return make_foreignscan(tlist,
                            scan_clauses,
                            scan_relid,
                            NIL,    /* no expressions to evaluate */
                            NIL,    /* no private state either */
                            NIL,    /* no custom tlist */
                            NIL,    /* no remote quals */
                            outer_plan);
}

static void
btExplainForeignScan(ForeignScanState *node, ExplainState *es) {
    /* TODO: calculate real values */
    ExplainPropertyText("Foreign Bigtable", "bt", es);

    if (es->costs) {
        ExplainPropertyLong("Foreign Bigtable, costs and row estiamtes are meaningless", 0, es);
    }
}

static void
btFdwBeginForeignScan(ForeignScanState *node, int eflags) {
    bt_fdw_state_t *st = bt_fdw_state_from_fss(node);

//    elog(LOG, "entering function %s", __func__);

    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
        return;
    }

    node->fdw_state = st;
}

static TupleTableSlot *
btFdWIterateForeignScan(ForeignScanState *node) {

    bt_fdw_state_t *st;
    st = node->fdw_state;

    elog(LOG, "entering function %s", __func__);

    bt_fdw_iterate_foreign_scan(st, node);
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    return slot;
}

static void
btReScanForeignScan(ForeignScanState *node) {
    elog(LOG, "entering function %s", __func__);
}

static void
btEndForeignScan(ForeignScanState *node) {
    elog(LOG, "entering function %s", __func__);

}

static void
btAddForeignUpdateTargets(Query *parsetree,
                          RangeTblEntry *target_rte,
                          Relation target_relation) {

    elog(DEBUG1, "entering function %s", __func__);

}


static List *
btPlanForeignModify(PlannerInfo *root,
                    ModifyTable *plan,
                    Index resultRelation,
                    int subplan_index) {

    elog(DEBUG1, "entering function %s", __func__);

    return NULL;
}


static void
btBeginForeignModify(ModifyTableState *mtstate,
                     ResultRelInfo *rinfo,
                     List *fdw_private,
                     int subplan_index,
                     int eflags) {

    bt_fdw_state_t *st = bt_fdw_state_from_relinfo(rinfo);

    elog(LOG, "entering function %s", __func__);

    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
        return;
    }

    rinfo->ri_FdwState = st;

}


static TupleTableSlot *
btExecForeignInsert(EState *estate,
                    ResultRelInfo *rinfo,
                    TupleTableSlot *slot,
                    TupleTableSlot *planSlot) {

    bt_fdw_state_t *st;
    st = rinfo->ri_FdwState;

    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    Form_pg_attribute attr = tupdesc->attrs[1];
    bool isnull;

    Datum val = slot_getattr(slot, 1, &isnull);

    char *res = TextDatumGetCString(val);

    bt_fdw_exec_foreign_insert(st, res);

    elog(DEBUG1, "entering function %s", __func__);

    return slot;
}


static TupleTableSlot *
btExecForeignUpdate(EState *estate,
                    ResultRelInfo *rinfo,
                    TupleTableSlot *slot,
                    TupleTableSlot *planSlot) {


    elog(DEBUG1, "entering function %s", __func__);

    return slot;
}


static TupleTableSlot *
btExecForeignDelete(EState *estate,
                    ResultRelInfo *rinfo,
                    TupleTableSlot *slot,
                    TupleTableSlot *planSlot) {

    elog(DEBUG1, "entering function %s", __func__);

    return slot;
}


static void
btEndForeignModify(EState *estate,
                   ResultRelInfo *rinfo) {

    elog(DEBUG1, "entering function %s", __func__);

}

static int
btIsForeignRelUpdatable(Relation rel) {

    elog(DEBUG1, "entering function %s", __func__);

    return (0 << CMD_UPDATE) | (1 << CMD_INSERT) | (0 << CMD_DELETE);
}
