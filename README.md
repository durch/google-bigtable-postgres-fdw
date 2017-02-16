[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/durch/rpg_base36/blob/master/LICENSE.md)

# Google Bigtable Rust PostgreSQL Extension

[Rust](https://www.rust-lang.org/en-US/) [PostgreSQL](https://www.postgresql.org/) extension for interfacing with [Google Cloud Bigtable](https://cloud.google.com/bigtable/), as well as other API compatible databases ([HBase](https://hbase.apache.org/) should work with some effort).

While logic is contained in `Rust`, it leverages `PostgreSQL ` `C` macros for passing parameters around as well returning values, and for ease of use it is all wrapped in `PL/pgSQL`.

At the moment **reading** and **writing** form/to `Bigtable` is supported with *deleting* and *updating* on the roadmap, as well as a few other more Bigtable oriented features, and encrypted credential storage. At present it is more of an exercise in writing a `PostgreSQL` extension in `Rust` than anything else.


## Installation
+ `PostgreSQL 9.3+`
+ `Stable Rust 1.15+`, get it using [rustup](https://www.rustup.rs/).

```bash
git clone https://github.com/durch/google-bigtable-postgres-extension.git
cd google-bigtable-postgres-extension
make install
psql -U postgres
```

Once inside the DB

```sql
CREATE EXTENSION bigtable;
```

The command above will also output a message about inputing your credentials, which you can get from Google Cloud Console, the intention is to work using service accounts, you'll need proper have [scopes](https://cloud.google.com/bigtable/docs/creating-compute-instance) in order to be able to use the extension. Once you have the `json` credential file, you can feed it in:

```sql
SELECT bt_set_credentials('<absolute_path_to_gcloud_json_credentials>');
```

The contents of the file are read, `base64` encoded and stored in `bt_auth_config` table, this is completely insecure and you should take care not allow bad guys access to this key, especially if it has admin scopes.

You can delete the file from the system after reading it in, it will be dumped and restored along with other `DB` tables.

## Usage

### Reading

```sql
# SIGNATURE
bt_read_rows(instance_name TEXT, table_name TEXT, limit INT) RETURNS JSON

# EXAMPLE
# Reading 10 rows from test_table in test-instance
SELECT bt_read_rows('test-instance', 'test-table', 10);

# Output will be valid json, it will fail if the table is empty
```

### Writing

#### Writing one row at a time

```sql
# SIGNATURES
bt_write_one(column_family TEXT, column_qulifier TEXT, rows TEXT, instance_name TEXT, table_name TEXT) RETURNS TEXT

bt_write_one(column_family TEXT, column_qulifier TEXT, rows JSON, instance_name TEXT, table_name TEXT) RETURNS TEXT

# EXAMPLES
SELECT bt_write_one('cf1', 't', 'Sample text row', 'test-instance','test-table');

SELECT bt_write_one('cf1', 't', '"Sample json text row"', 'test-instance','test-table');

SELECT bt_write_one('cf1', 't', '{"json_object_row": true}', 'test-instance','test-table');

SELECT bt_write_one('cf1', 't', '["json", "array", "row"]', 'test-instance','test-table');
```

#### Writing many rows at once

In the case of `bt_write_many` `json` *arrays* are unpacked into rows, all other `json` types will be written as one row, as if using `bt_write_one`. 

This should enable straightforward import of data to Bigtable as PostgreSQL has some very nice [JSON functions](https://www.postgresql.org/docs/9.6/static/functions-json.html) for formatting and converting data.

```sql
# SIGNATURE
bt_write_many(column_family TEXT, column_qulifier TEXT, rows JSON, instance_name IN TEXT, table_name) RETURNS TEXT

# EXAMPLE
SELECT bt_write_many('cf1', 't', '["this", "will", "make", 5, "rows"]', 'test-instance', 'test-table');
```
