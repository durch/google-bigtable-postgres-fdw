#include "postgres.h"

#if PG_VERSION_NUM < 90600
#error wrong Postgresql version, 9.6.x is required
#endif

#include "foreign/fdwapi.h"

extern void
fdw_assign_callbacks(FdwRoutine *);

PG_MODULE_MAGIC;

extern Datum bt_fdw_handler(PG_FUNCTION_ARGS);
extern Datum bt_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bt_fdw_handler);
PG_FUNCTION_INFO_V1(bt_fdw_validator);

Datum
bt_fdw_handler(PG_FUNCTION_ARGS) {
    FdwRoutine *fdw_routine = makeNode(FdwRoutine);
    fdw_assign_callbacks(fdw_routine);

    PG_RETURN_POINTER(fdw_routine);
}

Datum
bt_fdw_validator(PG_FUNCTION_ARGS) {
    PG_RETURN_BOOL(true);
}