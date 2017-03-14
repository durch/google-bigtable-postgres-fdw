-- -- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bigtable" to load this file. \quit

CREATE FUNCTION bt_fdw_handler()
RETURNS fdw_handler
AS '$libdir/bigtable'
LANGUAGE C STRICT;

CREATE FUNCTION bt_fdw_validator(text[], oid)
RETURNS void
AS '$libdir/bigtable'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER bigtable
  HANDLER bt_fdw_handler
  VALIDATOR bt_fdw_validator;