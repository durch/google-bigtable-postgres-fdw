EXTENSION    = bigtable
EXTVERSION   = 0.1.0

DATA         = sql/$(EXTENSION)--$(EXTVERSION).sql
PG_CONFIG    = pg_config

all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql

# Note that `MODULES = bigtable` implies a dependency on `bigtable.so`.
MODULES      = bigtable
PGXX        := $(shell utils/get_version.sh)
RS          := $(shell which cargo >/dev/null && echo yes || echo no)

ifeq ($(shell uname -s),Darwin)
    LINK_FLAGS   = -C link-args='-Wl,-undefined,dynamic_lookup'
endif

.PHONY: bigtable.so
bigtable.so:
	cargo rustc --release -- $(LINK_FLAGS)
	cp target/release/libbigtable.* $@

.PHONY: cargoclean
cargoclean:
	find . -name Cargo.lock -exec rm {} \;
	cargo clean

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

clean: cargoclean

all: bigtable.so
