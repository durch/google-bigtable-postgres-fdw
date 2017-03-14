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

#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
/*
  #include "catalog/pg_type.h"
*/
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
/*
  #include "mb/pg_wchar.h"
  #include "nodes/makefuncs.h"
*/
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
/*
  #include "storage/fd.h"
  #include "utils/array.h"
*/
#include "utils/builtins.h"
#include "utils/rel.h"

#define LENGTH(x) VARSIZE(x) - VARHDRSZ

/*
 * Valid options that could be used by
 * this wrapper
 */

typedef struct btFdwOption {
    const char *option_name;
    Oid option_context;
} btFdwOption;

static struct btFdwOption valid_options[] =
        {
                {"instance",    ForeignServerRelationId},
                {"credentials", UserMappingRelationId},
                {"table",       ForeignTableRelationId},
                {NULL,          InvalidOid}
        };

typedef struct {
    text *instance;
    text *table;
    text *credentials;
} btFdwConfiguration;

///*
// * Stores the FDW execution state
// */
typedef struct {
    int not_row;
    int row;
//    btFdwConfiguration *config;

} btFdwExecutionState;

// Rust stuff

typedef struct bt_fdw_state_S bt_fdw_state_t;

extern bt_fdw_state_t *
bt_fdw_state_from_fss(Oid, ForeignScanState *);

extern bt_fdw_state_t *
bt_fdw_state_from_relinfo(Oid, ResultRelInfo *);

extern void *
bt_fdw_iterate_foreign_scan(bt_fdw_state_t *, ForeignScanState *);

extern void *
bt_fdw_exec_foreign_insert(bt_fdw_state_t *, TupleTableSlot *, char *);

extern void *
get_limit(PlannerInfo *);

/*
 * FDW functions declarations
 */

static void
btGetForeignRelSize(PlannerInfo *root,
                    RelOptInfo *baserel,
                    Oid foreigntableid);

static void
btGetForeignPaths(PlannerInfo *root,
                  RelOptInfo *baserel,
                  Oid foreigntableid);

static ForeignScan *
btGetForeignPlan(PlannerInfo *root,
                 RelOptInfo *baserel,
                 Oid foreigntableid,
                 ForeignPath *best_path,
                 List *tlist,
                 List *scan_clauses,
                 Plan *outer_plan);

static void
btExplainForeignScan(ForeignScanState *node,
                     ExplainState *es);

static void
btFdwBeginForeignScan(ForeignScanState *node,
                      int eflags);

static TupleTableSlot *
btFdWIterateForeignScan(ForeignScanState *node);

static void
btReScanForeignScan(ForeignScanState *node);

static void
btEndForeignScan(ForeignScanState *node);

/*
static bool
ldapAnalyzeForeignTable(Relation relation,
                    AcquireSampleRowsFunc *func,
                    BlockNumber *totalpages);
*/

/*
 * Helper functions
 */
//static void _get_str_attributes(char *attributes[], Relation);
//static int  _name_str_case_cmp(Name, const char *);
//static bool _is_valid_option(const char *, Oid);
//static void _ldap_get_options(Oid, LdapFdwConfiguration *);
//static void _ldap_check_quals(Node *, TupleDesc, char **, char **, bool *);
//static char ** _string_to_array(char *);

static void btExplainForeignModify(ModifyTableState *mtstate,
                                   ResultRelInfo *rinfo,
                                   List *fdw_private,
                                   int subplan_index,
                                   struct ExplainState *es);

static void btAddForeignUpdateTargets(Query *parsetree,
                                      RangeTblEntry *target_rte,
                                      Relation target_relation);

static List *btPlanForeignModify(PlannerInfo *root,
                                 ModifyTable *plan,
                                 Index resultRelation,
                                 int subplan_index);

static void btBeginForeignModify(ModifyTableState *mtstate,
                                 ResultRelInfo *rinfo,
                                 List *fdw_private,
                                 int subplan_index,
                                 int eflags);

static TupleTableSlot *btExecForeignInsert(EState *estate,
                                           ResultRelInfo *rinfo,
                                           TupleTableSlot *slot,
                                           TupleTableSlot *planSlot);

static TupleTableSlot *btExecForeignUpdate(EState *estate,
                                           ResultRelInfo *rinfo,
                                           TupleTableSlot *slot,
                                           TupleTableSlot *planSlot);

static TupleTableSlot *btExecForeignDelete(EState *estate,
                                           ResultRelInfo *rinfo,
                                           TupleTableSlot *slot,
                                           TupleTableSlot *planSlot);

static void btEndForeignModify(EState *estate,
                               ResultRelInfo *rinfo);

static int btIsForeignRelUpdatable(Relation rel);