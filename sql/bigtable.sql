-- drop extension bigtable cascade; create extension bigtable; create server test foreign data wrapper bt_fdw options (instance 'testinst', project 'drazens-bigtable-testing'); create foreign table test(bt json) server test options (name 'test'); create user mapping for postgres server test options (credentials_path '/tmp/code/drazens-bigtable-testing-9f32bd2aa193.json');
--
--

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

CREATE FOREIGN DATA WRAPPER bt_fdw
  HANDLER bt_fdw_handler
  VALIDATOR bt_fdw_validator;