/*-------------------------------------------------------------------------
 *
 *      foreign-data wrapper for LDAP
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Dickson S. Guedes <guedes@guedesoft.net>
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#if PG_VERSION_NUM < 90600
#error wrong Postgresql version, 9.6.x is required
#endif

//#include "funcapi.h"
//#include "access/reloptions.h"
//#include "catalog/pg_foreign_table.h"
//#include "catalog/pg_foreign_server.h"
//#include "catalog/pg_user_mapping.h"
//#include "commands/explain.h"
#include "foreign/fdwapi.h"
//#include "foreign/foreign.h"
//#include "miscadmin.h"
//#include "optimizer/cost.h"
//#include "optimizer/pathnode.h"
//#include "optimizer/planmain.h"
//#include "optimizer/restrictinfo.h"
//#include "utils/builtins.h"
//#include "utils/rel.h"

// Rust stuff

extern void
fdw_assign_callbacks(FdwRoutine *);

//extern void
//bt_fdw_get_foreign_rel_size(PlannerInfo *root,
//                            RelOptInfo *baserel,
//                            Oid foreigntableid);
//
//
//extern void
//bt_fdw_get_foreign_paths(PlannerInfo *root,
//                         RelOptInfo *baserel,
//                         Oid foreigntableid);
//
//
//extern ForeignScan *
//bt_fdw_get_foreign_plan(PlannerInfo *,
//                        RelOptInfo *,
//                        Oid,
//                        ForeignPath *,
//                        List *,
//                        List *,
//                        Plan *);
//
//
//extern void
//bt_fdw_explain_foreign_scan(ForeignScanState *, ExplainState *);
//
//extern void
//bt_fdw_begin_foreign_scan(ForeignScanState *, int);
//
//extern TupleTableSlot *
//bt_fdw_iterate_foreign_scan(ForeignScanState *);
//
//extern void
//bt_fdw_rescan_foreign_scan(ForeignScanState *node);
//
//extern void
//bt_fdw_end_foreign_scan(ForeignScanState *node);
//
//extern void
//bt_fdw_begin_foreign_modify(ModifyTableState *mtstate,
//                            ResultRelInfo *rinfo,
//                            List *fdw_private,
//                            int subplan_index,
//                            int eflags);
//
//
//extern TupleTableSlot *
//bt_fdw_exec_foreign_insert(EState *estate,
//                           ResultRelInfo *rinfo,
//                           TupleTableSlot *slot,
//                           TupleTableSlot *planSlot);
//
//
//extern void *
//get_limit(PlannerInfo *);
//
//
//static void btExplainForeignModify(ModifyTableState *mtstate,
//                                   ResultRelInfo *rinfo,
//                                   List *fdw_private,
//                                   int subplan_index,
//                                   struct ExplainState *es);
//
//extern void
//bt_fdw_add_foreign_update_targets(Query *parsetree,
//                                  RangeTblEntry *target_rte,
//                                  Relation target_relation);
//
//extern List *
//bt_fdw_plan_foreign_modify(PlannerInfo *root,
//                           ModifyTable *plan,
//                           Index resultRelation,
//                           int subplan_index);
//
//extern TupleTableSlot *
//bt_fdw_exec_foreign_update(EState *estate,
//                           ResultRelInfo *rinfo,
//                           TupleTableSlot *slot,
//                           TupleTableSlot *planSlot);
//
//extern TupleTableSlot *
//bt_fdw_exec_foreign_delete(EState *estate,
//                           ResultRelInfo *rinfo,
//                           TupleTableSlot *slot,
//                           TupleTableSlot *planSlot);
//
//extern void
//bt_fdw_end_foreign_modify(EState *estate,
//                          ResultRelInfo *rinfo);
//
//extern int
//bt_is_foreign_rel_updatable(Relation rel);