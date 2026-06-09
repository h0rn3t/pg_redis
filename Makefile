EXTENSION   = pg_redis
MODULE_big  = pg_redis

OBJS = \
	src/pg_redis.o \
	src/kv_store.o \
	src/ttl.o \
	src/list.o \
	src/hash_value.o \
	src/persistence.o \
	src/bgworker.o \
	src/jobs.o \
	src/shmem.o \
	src/utils.o \
	src/binval.o \
	src/dirty_ring.o \
	src/shared_hash.o \
	src/shared_list.o \
	src/shared_store.o

DATA         = pg_redis--1.0.sql pg_redis--1.0--1.1.sql pg_redis--1.1.sql \
               pg_redis--1.1--1.2.sql pg_redis--1.2.sql
REGRESS      = basic ttl hashes lists persistence flush admin jobs async_table
REGRESS_OPTS = --inputdir=test --outputdir=test

PG_CFLAGS = -Wall -Wextra -Wno-unused-parameter -Isrc

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs 2>/dev/null)
ifneq ($(wildcard $(PGXS)),)
include $(PGXS)
else
$(warning $(PG_CONFIG): PGXS not found at "$(PGXS)" - only docker-* targets are available)
endif

# ----- Docker -----
PG_VERSION   ?= 18
DOCKER_IMAGE ?= pg_redis
DOCKER_TAG   ?= $(PG_VERSION)

.PHONY: docker-build docker-test docker-test-async docker-regen docker-shell docker-up docker-down docker-bench

docker-build:
	docker build \
		--target runtime \
		--build-arg PG_VERSION=$(PG_VERSION) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		.

docker-test:
	docker build \
		--target test \
		--build-arg PG_VERSION=$(PG_VERSION) \
		--progress=plain \
		.

# Async-mode tests: spins a cluster with shared_preload_libraries=pg_redis,
# storage_mode=shared, a small dirty_ring_size, and the BGW disabled, then
# runs test/async/*.sql. Used for scenarios pg_regress can't cover.
docker-test-async:
	docker build \
		--target test-async \
		--build-arg PG_VERSION=$(PG_VERSION) \
		--progress=plain \
		.

# Regenerate test/expected/*.out by running installcheck inside a builder
# image with the host's test/expected/ bind-mounted. Use after editing or
# adding test SQL files.
docker-regen:
	docker build \
		--target builder \
		--build-arg PG_VERSION=$(PG_VERSION) \
		-t pg_redis-builder:$(DOCKER_TAG) \
		.
	docker run --rm \
		--user postgres \
		-e PGREDIS_REGEN_DIR=/host_expected \
		-v "$(CURDIR)/test/expected:/host_expected" \
		pg_redis-builder:$(DOCKER_TAG) \
		/usr/local/bin/docker-test.sh

docker-shell:
	docker run --rm -it \
		-e POSTGRES_PASSWORD=postgres \
		$(DOCKER_IMAGE):$(DOCKER_TAG) bash

docker-up:
	PG_VERSION=$(PG_VERSION) docker compose up --build -d

docker-down:
	PG_VERSION=$(PG_VERSION) docker compose down -v

# Run the bench: spins up redis + pg_redis + a python container, prints the
# comparison to stdout, tears everything down.
docker-bench:
	PG_VERSION=$(PG_VERSION) BENCH_N=$${BENCH_N:-1000} \
		docker compose -f docker-compose.bench.yml up --build -d redis pg_redis
	PG_VERSION=$(PG_VERSION) BENCH_N=$${BENCH_N:-1000} \
		docker compose -f docker-compose.bench.yml run --rm --build bench
	PG_VERSION=$(PG_VERSION) \
		docker compose -f docker-compose.bench.yml down -v
