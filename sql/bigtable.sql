-- -- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bigtable" to load this file. \quit

CREATE FUNCTION bt_config_table()
  RETURNS VOID AS
$$
BEGIN
  CREATE TABLE bt_auth_config (
    credentials TEXT,
    created     TIMESTAMPTZ DEFAULT now()
  );
  RAISE WARNING 'Run "SELECT bt_set_credentials(<absolute_path_to_gcloud_json_credentials>);" to set credentials, after this you can remove the file it will be stored in bt_auth_config table ';
  PERFORM pg_catalog.pg_extension_config_dump('bt_auth_config', '');
END;
$$ LANGUAGE 'plpgsql';

SELECT bt_config_table();

CREATE FUNCTION bt_get_credentials(r OUT TEXT) AS
$$
BEGIN
  SELECT credentials
  FROM bt_auth_config
  ORDER BY created DESC
  LIMIT 1
  INTO r;
END;
$$ LANGUAGE 'plpgsql';

CREATE FUNCTION bt_read_rows(instance IN TEXT, tb IN TEXT, lim IN INT DEFAULT 10, ret OUT JSON) AS
$$
BEGIN
  SELECT _bt_read_rows(lim, bt_get_credentials(), instance, tb)
  INTO ret;
END;
$$ LANGUAGE 'plpgsql';

CREATE FUNCTION bt_write_one(family IN TEXT, qulifier IN TEXT, val IN TEXT, instance IN TEXT,
                             tb     IN TEXT, ret OUT TEXT) AS
$$
BEGIN
  SELECT _bt_write(family, qulifier, val, bt_get_credentials(), instance, tb, FALSE)
  INTO ret;
END;
$$ LANGUAGE 'plpgsql';

CREATE FUNCTION bt_write_one(family IN TEXT, qulifier IN TEXT, val IN JSON, instance IN TEXT,
                             tb     IN TEXT, ret OUT TEXT) AS
$$
DECLARE
  credentials TEXT;
BEGIN
  SELECT bt_get_credentials()
  INTO credentials;
  SELECT _bt_write(family, qulifier, val, credentials, instance, tb, FALSE)
  INTO ret;
END;
$$ LANGUAGE 'plpgsql';

CREATE FUNCTION bt_write_many(family TEXT, qulifier TEXT, val JSON, instance IN TEXT, tb IN TEXT, ret OUT TEXT) AS
$$
DECLARE
  credentials TEXT;
BEGIN
  SELECT bt_get_credentials()
  INTO credentials;
  SELECT _bt_write(family, qulifier, val, credentials, instance, tb, TRUE)
  INTO ret;
END;
$$ LANGUAGE 'plpgsql';


CREATE FUNCTION _bt_read_rows(lim INT, credentials TEXT, instance TEXT, tb TEXT)
  RETURNS JSON
AS '$libdir/bigtable'
LANGUAGE C;

CREATE FUNCTION _bt_write(family      TEXT, qulifier TEXT, val TEXT, credentials TEXT, instance TEXT, tb TEXT,
                          split_array BOOL)
  RETURNS TEXT
AS '$libdir/bigtable'
LANGUAGE C;

CREATE FUNCTION _bt_write(family      TEXT, qulifier TEXT, val JSON, credentials TEXT, instance TEXT, tb TEXT,
                          split_array BOOL)
  RETURNS TEXT
AS '$libdir/bigtable'
LANGUAGE C;

CREATE FUNCTION bt_set_credentials(fp TEXT)
  RETURNS VOID AS
$$
DECLARE
  credentials TEXT;
BEGIN
  INSERT INTO bt_auth_config (credentials) SELECT _bt_set_credentials(fp);
END;
$$ LANGUAGE 'plpgsql';

CREATE FUNCTION _bt_set_credentials(fp TEXT)
  RETURNS TEXT
AS '$libdir/bigtable'
LANGUAGE C;

