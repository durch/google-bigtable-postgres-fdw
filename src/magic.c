/* Gain access to Pg_magic_func() */

#include "postgres.h" // includes most of the basic stuff needed for interfacing with Postgres. This line needs to be included in every C-File that declares Postgres functions.
#include "fmgr.h" // needs to be included to make use of PG_GETARG_XXX and PG_RETURN_XXX macros. While this is valid for C we just need bindings and can write our own macros as you can see in lib.rs

PG_MODULE_MAGIC; // the “magic block” needed as of PostgreSQL 8.2 in one (and only one) of the module source files after including the header fmgr.h.
PG_FUNCTION_INFO_V1(_bt_read_rows); // introduces the function to Postges as Version 1 Calling Convention, and is only needed if you want the function to interface with Postgres.
PG_FUNCTION_INFO_V1(_bt_write);
PG_FUNCTION_INFO_V1(_bt_set_credentials);

#define LENGTH(x)  VARSIZE(x) - VARHDRSZ

Datum _bt_set_credentials(PG_FUNCTION_ARGS) {
    text *credentials, *ret;

    credentials = PG_GETARG_TEXT_P(0);

    text* bt_rust_set_credentials(const char* credentials, int l);

    ret = bt_rust_set_credentials(VARDATA(credentials), LENGTH(credentials));

    PG_RETURN_TEXT_P(ret);
}

Datum _bt_read_rows(PG_FUNCTION_ARGS) {

    text *credentials, *ret, *instance, *tb;
    int lim;

    text* bt_rust_read_rows(int lim,
                            const char* credentials, int c_l,
                            const char* instance_ptr, int i_l,
                            const char* table_ptr, int t_l);

    lim = PG_GETARG_INT64(0);
    credentials = PG_GETARG_TEXT_P(1);
    instance = PG_GETARG_TEXT_P(2);
    tb = PG_GETARG_TEXT_P(3);

    ret = bt_rust_read_rows(lim,
                            VARDATA(credentials), LENGTH(credentials),
                            VARDATA(instance), LENGTH(instance),
                            VARDATA(tb), LENGTH(tb));

    PG_RETURN_TEXT_P(ret);
}

Datum _bt_write(PG_FUNCTION_ARGS) {

    text *c_family, *c_qualifier, *c_rows, *ret, *credentials, *instance, *tb;
    bool split_array;

    text* bt_rust_write_rows(const char* c_family, int f_l,
                     const char* c_qualifier, int q_l,
                     const char* c_rows, int r_l,
                     const char* credentials, int c_l,
                     const char* instance, int i_l,
                     const char* tb, int t_l,
                     bool split_array);

    c_family = PG_GETARG_TEXT_P(0);
    c_qualifier = PG_GETARG_TEXT_P(1);
    c_rows = PG_GETARG_TEXT_P(2);
    credentials = PG_GETARG_TEXT_P(3);
    instance = PG_GETARG_TEXT_P(4);
    tb = PG_GETARG_TEXT_P(5);
    split_array = PG_GETARG_BOOL(6);

    ret = bt_rust_write_rows(VARDATA(c_family), LENGTH(c_family),
                            VARDATA(c_qualifier), LENGTH(c_qualifier),
                            VARDATA(c_rows), LENGTH(c_rows),
                            VARDATA(credentials), LENGTH(credentials),
                            VARDATA(instance), LENGTH(instance),
                            VARDATA(tb), LENGTH(tb),
                            split_array);

    PG_RETURN_TEXT_P(ret);
}