[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/durch/rpg_base36/blob/master/LICENSE.md)

# Google Bigtable Rust PostgreSQL FDW

[Rust](https://www.rust-lang.org/en-US/) [PostgreSQL](https://www.postgresql.org/) foreign data wrapper for interfacing with [Google Cloud Bigtable](https://cloud.google.com/bigtable/), as well as other API compatible databases ([HBase](https://hbase.apache.org/) should work with some effort).

While logic is contained in `Rust`, it leverages `PostgreSQL C FDW` callbacks.

### Roadmap

- [x] `SELECT`
- [x] `SELECT LIMIT`
- [ ] `SELECT OFFSET`
- [ ] `SELECT WHERE`
- [x] `INSERT`
- [ ] `UPDATE` - update can be achived by inserting an existing key
- [ ] `DELETE`
- [ ] Support for PG 9.3+
- [ ] Useful `EXPLAIN`
- [x] Reduce `C` boilerplate

## Installation

+ `PostgreSQL 9.6+`
+ `stable Rust`, get it using [rustup](https://www.rustup.rs/).

```bash
git clone https://github.com/durch/google-bigtable-postgres-fdw.git
cd google-bigtable-postgres-fdw
make install
psql -U postgres
```

### Initial DB setup

```PLpgSQL
CREATE EXTENSION bigtable;
CREATE SERVER test FOREIGN DATA WRAPPER bigtable OPTIONS (instance '`instance_id`', project '`project_id`');
CREATE FOREIGN TABLE test(bt json) SERVER test OPTIONS (name '`table_name`');
CREATE USER MAPPING FOR postgres SERVER TEST OPTIONS (credentials_path '`path_to_service_account_json_credentials`');
```

### Usage

You can use [gen.py]( google-bigtable-postgres-fdw/gen.py ) to generate some test data. Modify `gen.py` to adjust for the number of generated records, also modify the`column` key in the generated output as this needs be a `column familly` that **exists** in your Bigtable, running `python gen.py` outputs `test.sql`, which can be fed into PG.

```
psql -U postgres < test.sql
```

#### SELECT

One Bigtable row per PG rowis returned, limit is done on the BT side, rows are returned as `json` and can be further manipulated using Postgres [json functions and operators](`https://www.postgresql.org/docs/9.6/static/functions-json.html`). `WHERE` is evaluted on the PG side so be sure to grab what you need from BT.

```PLpgSQL
SELECT * FROM test;
SELECT * FROM test LIMIT 100;

SELECT bt->'familyName', bt->'qualifier' FROM test WHERE bt->>'rowKey' ~* '.*regex.*';
SELECT bt->'familyName', bt->'qualifier' FROM test WHERE bt->>'rowKey' = 'exact';
```

#### INSERT

`INSERT` takes a `json` array of rows as defined in [core BT lib]("https://github.com/durch/rust-bigtable/blob/master/src/wraps.rs#L21"):

```json
[
    {
        "row_key": "string",
        "family": "string",
        "qualifier": "string",
        "value": "string"
    }
]
```

As you are passing in one `json` object which gets expanded, `INSERT` counter always shows one row inserted, truth can be found in PG logs.

## Environment setup

You can get up and running quickly with PG + Rust setup using `docker-compose`.

```bash
docker-compose up -d
docker exec -it `docker ps | grep btpgext_vm | awk '{print $1}'` /bin/bash
```

This will setup staticly linked Rust 1.16 and PG 9.6 in an Ubuntu Xenial image, and get you in, where you can run `make install` and start playing.