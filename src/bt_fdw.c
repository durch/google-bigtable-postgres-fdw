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

    baserel->rows = 500;
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
    ExplainPropertyText("Foreign BT", "bt", es);

    if (es->costs) {
        ExplainPropertyLong("Foreign BT cost", 10, es);
    }
}

static void
btFdwBeginForeignScan(ForeignScanState *node, int eflags) {
    bt_fdw_state_t *st = bt_fdw_state_from_fss(GetUserId(), node);

//    elog(LOG, "entering function %s", __func__);

    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
        return;
    }

    node->fdw_state = st;
}

static TupleTableSlot *
btFdWIterateForeignScan(ForeignScanState *node) {
//    btFdwExecutionState *festate = (btFdwExecutionState *) node->fdw_state;
//    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
//    AttInMetadata *attInMetadata = TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);
//    HeapTuple tuple;
//    char **values;

    bt_fdw_state_t *st;
    st = node->fdw_state;

//    node->fdw_state = state_incr(st);

//    ExecClearTuple(slot);

    elog(LOG, "entering function %s", __func__);

//    values = (char **) palloc(sizeof(char *) * 2);
//    values[0] = "lala";
//
//
//    state_incr(st, node);
//    tuple = BuildTupleFromCStrings(attInMetadata, values);

//    ExecStoreTuple(tuple, slot, InvalidBuffer, false);

//    ExecStoreVirtualTuple(slot);
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
    /*
     * UPDATE and DELETE operations are performed against rows previously
     * fetched by the table-scanning functions. The FDW may need extra
     * information, such as a row ID or the values of primary-key columns, to
     * ensure that it can identify the exact row to update or delete. To
     * support that, this function can add extra hidden, or "junk", target
     * columns to the list of columns that are to be retrieved from the
     * foreign table during an UPDATE or DELETE.
     *
     * To do that, add TargetEntry items to parsetree->targetList, containing
     * expressions for the extra values to be fetched. Each such entry must be
     * marked resjunk = true, and must have a distinct resname that will
     * identify it at execution time. Avoid using names matching ctidN or
     * wholerowN, as the core system can generate junk columns of these names.
     *
     * This function is called in the rewriter, not the planner, so the
     * information available is a bit different from that available to the
     * planning routines. parsetree is the parse tree for the UPDATE or DELETE
     * command, while target_rte and target_relation describe the target
     * foreign table.
     *
     * If the AddForeignUpdateTargets pointer is set to NULL, no extra target
     * expressions are added. (This will make it impossible to implement
     * DELETE operations, though UPDATE may still be feasible if the FDW
     * relies on an unchanging primary key to identify rows.)
     */

    elog(DEBUG1, "entering function %s", __func__);

}


static List *
btPlanForeignModify(PlannerInfo *root,
                    ModifyTable *plan,
                    Index resultRelation,
                    int subplan_index) {
    /*
     * Perform any additional planning actions needed for an insert, update,
     * or delete on a foreign table. This function generates the FDW-private
     * information that will be attached to the ModifyTable plan node that
     * performs the update action. This private information must have the form
     * of a List, and will be delivered to BeginForeignModify during the
     * execution stage.
     *
     * root is the planner's global information about the query. plan is the
     * ModifyTable plan node, which is complete except for the fdwPrivLists
     * field. resultRelation identifies the target foreign table by its
     * rangetable index. subplan_index identifies which target of the
     * ModifyTable plan node this is, counting from zero; use this if you want
     * to index into plan->plans or other substructure of the plan node.
     *
     * If the PlanForeignModify pointer is set to NULL, no additional
     * plan-time actions are taken, and the fdw_private list delivered to
     * BeginForeignModify will be NIL.
     */

    elog(DEBUG1, "entering function %s", __func__);

    return NULL;
}


static void
btBeginForeignModify(ModifyTableState *mtstate,
                     ResultRelInfo *rinfo,
                     List *fdw_private,
                     int subplan_index,
                     int eflags) {
    /*
     * Begin executing a foreign table modification operation. This routine is
     * called during executor startup. It should perform any initialization
     * needed prior to the actual table modifications. Subsequently,
     * ExecForeignInsert, ExecForeignUpdate or ExecForeignDelete will be
     * called for each tuple to be inserted, updated, or deleted.
     *
     * mtstate is the overall state of the ModifyTable plan node being
     * executed; global data about the plan and execution state is available
     * via this structure. rinfo is the ResultRelInfo struct describing the
     * target foreign table. (The ri_FdwState field of ResultRelInfo is
     * available for the FDW to store any private state it needs for this
     * operation.) fdw_private contains the private data generated by
     * PlanForeignModify, if any. subplan_index identifies which target of the
     * ModifyTable plan node this is. eflags contains flag bits describing the
     * executor's operating mode for this plan node.
     *
     * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
     * should not perform any externally-visible actions; it should only do
     * the minimum required to make the node state valid for
     * ExplainForeignModify and EndForeignModify.
     *
     * If the BeginForeignModify pointer is set to NULL, no action is taken
     * during executor startup.
     */

//    BlackholeFdwModifyState *modify_state =
//            palloc0(sizeof(BlackholeFdwModifyState));
//    rinfo->ri_FdwState = modify_state;

    bt_fdw_state_t *st = bt_fdw_state_from_relinfo(GetUserId(), rinfo);

//    elog(LOG, "entering function %s", __func__);

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
    /*
     * Insert one tuple into the foreign table. estate is global execution
     * state for the query. rinfo is the ResultRelInfo struct describing the
     * target foreign table. slot contains the tuple to be inserted; it will
     * match the rowtype definition of the foreign table. planSlot contains
     * the tuple that was generated by the ModifyTable plan node's subplan; it
     * differs from slot in possibly containing additional "junk" columns.
     * (The planSlot is typically of little interest for INSERT cases, but is
     * provided for completeness.)
     *
     * The return value is either a slot containing the data that was actually
     * inserted (this might differ from the data supplied, for example as a
     * result of trigger actions), or NULL if no row was actually inserted
     * (again, typically as a result of triggers). The passed-in slot can be
     * re-used for this purpose.
     *
     * The data in the returned slot is used only if the INSERT query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away
     * returning some or all columns depending on the contents of the
     * RETURNING clause. However, some slot must be returned to indicate
     * success, or the query's reported rowcount will be wrong.
     *
     * If the ExecForeignInsert pointer is set to NULL, attempts to insert
     * into the foreign table will fail with an error message.
     *
     */

    /* ----
     * BlackholeFdwModifyState *modify_state =
     *	 (BlackholeFdwModifyState *) rinfo->ri_FdwState;
     * ----
     */

    bt_fdw_state_t *st;
    st = rinfo->ri_FdwState;

    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    Form_pg_attribute attr = tupdesc->attrs[1];
    bool isnull;

    Datum val = slot_getattr(slot, 1, &isnull);

    char *res = TextDatumGetCString(val);

//    text *json = cstring_to_text((char *) val);

    bt_fdw_exec_foreign_insert(st, slot, res);

    elog(DEBUG1, "entering function %s", __func__);

    return slot;
}


static TupleTableSlot *
btExecForeignUpdate(EState *estate,
                    ResultRelInfo *rinfo,
                    TupleTableSlot *slot,
                    TupleTableSlot *planSlot) {
    /*
     * Update one tuple in the foreign table. estate is global execution state
     * for the query. rinfo is the ResultRelInfo struct describing the target
     * foreign table. slot contains the new data for the tuple; it will match
     * the rowtype definition of the foreign table. planSlot contains the
     * tuple that was generated by the ModifyTable plan node's subplan; it
     * differs from slot in possibly containing additional "junk" columns. In
     * particular, any junk columns that were requested by
     * AddForeignUpdateTargets will be available from this slot.
     *
     * The return value is either a slot containing the row as it was actually
     * updated (this might differ from the data supplied, for example as a
     * result of trigger actions), or NULL if no row was actually updated
     * (again, typically as a result of triggers). The passed-in slot can be
     * re-used for this purpose.
     *
     * The data in the returned slot is used only if the UPDATE query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away
     * returning some or all columns depending on the contents of the
     * RETURNING clause. However, some slot must be returned to indicate
     * success, or the query's reported rowcount will be wrong.
     *
     * If the ExecForeignUpdate pointer is set to NULL, attempts to update the
     * foreign table will fail with an error message.
     *
     */

    /* ----
     * BlackholeFdwModifyState *modify_state =
     *	 (BlackholeFdwModifyState *) rinfo->ri_FdwState;
     * ----
     */

    elog(DEBUG1, "entering function %s", __func__);

    return slot;
}


static TupleTableSlot *
btExecForeignDelete(EState *estate,
                    ResultRelInfo *rinfo,
                    TupleTableSlot *slot,
                    TupleTableSlot *planSlot) {
    /*
     * Delete one tuple from the foreign table. estate is global execution
     * state for the query. rinfo is the ResultRelInfo struct describing the
     * target foreign table. slot contains nothing useful upon call, but can
     * be used to hold the returned tuple. planSlot contains the tuple that
     * was generated by the ModifyTable plan node's subplan; in particular, it
     * will carry any junk columns that were requested by
     * AddForeignUpdateTargets. The junk column(s) must be used to identify
     * the tuple to be deleted.
     *
     * The return value is either a slot containing the row that was deleted,
     * or NULL if no row was deleted (typically as a result of triggers). The
     * passed-in slot can be used to hold the tuple to be returned.
     *
     * The data in the returned slot is used only if the DELETE query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away
     * returning some or all columns depending on the contents of the
     * RETURNING clause. However, some slot must be returned to indicate
     * success, or the query's reported rowcount will be wrong.
     *
     * If the ExecForeignDelete pointer is set to NULL, attempts to delete
     * from the foreign table will fail with an error message.
     */

    /* ----
     * BlackholeFdwModifyState *modify_state =
     *	 (BlackholeFdwModifyState *) rinfo->ri_FdwState;
     * ----
     */

    elog(DEBUG1, "entering function %s", __func__);

    return slot;
}


static void
btEndForeignModify(EState *estate,
                   ResultRelInfo *rinfo) {
    /*
     * End the table update and release resources. It is normally not
     * important to release palloc'd memory, but for example open files and
     * connections to remote servers should be cleaned up.
     *
     * If the EndForeignModify pointer is set to NULL, no action is taken
     * during executor shutdown.
     */

    /* ----
     * BlackholeFdwModifyState *modify_state =
     *	 (BlackholeFdwModifyState *) rinfo->ri_FdwState;
     * ----
     */

    elog(DEBUG1, "entering function %s", __func__);

}

static int
btIsForeignRelUpdatable(Relation rel) {
    /*
     * Report which update operations the specified foreign table supports.
     * The return value should be a bit mask of rule event numbers indicating
     * which operations are supported by the foreign table, using the CmdType
     * enumeration; that is, (1 << CMD_UPDATE) = 4 for UPDATE, (1 <<
     * CMD_INSERT) = 8 for INSERT, and (1 << CMD_DELETE) = 16 for DELETE.
     *
     * If the IsForeignRelUpdatable pointer is set to NULL, foreign tables are
     * assumed to be insertable, updatable, or deletable if the FDW provides
     * ExecForeignInsert, ExecForeignUpdate, or ExecForeignDelete
     * respectively. This function is only needed if the FDW supports some
     * tables that are updatable and some that are not. (Even then, it's
     * permissible to throw an error in the execution routine instead of
     * checking in this function. However, this function is used to determine
     * updatability for display in the information_schema views.)
     */

    elog(DEBUG1, "entering function %s", __func__);

    return (0 << CMD_UPDATE) | (1 << CMD_INSERT) | (0 << CMD_DELETE);
}
