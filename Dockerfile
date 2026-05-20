# syntax=docker/dockerfile:1.6
ARG PG_VERSION=18

FROM postgres:${PG_VERSION}-bookworm AS builder
ARG PG_VERSION
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential \
        postgresql-server-dev-${PG_VERSION} \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Makefile pg_redis.control \
     pg_redis--1.0.sql pg_redis--1.0--1.1.sql pg_redis--1.1.sql ./
COPY src/ ./src/
COPY test/ ./test/
RUN make clean >/dev/null 2>&1 || true \
 && make \
 && make install
COPY scripts/docker-test.sh /usr/local/bin/docker-test.sh
COPY scripts/docker-test-async.sh /usr/local/bin/docker-test-async.sh
RUN chmod +x /usr/local/bin/docker-test.sh /usr/local/bin/docker-test-async.sh \
 && chown -R postgres:postgres /build

FROM builder AS test
USER postgres
ENV PGDATA=/tmp/pgdata
RUN /usr/local/bin/docker-test.sh

FROM builder AS test-async
USER postgres
ENV PGDATA=/tmp/pgdata-async
RUN /usr/local/bin/docker-test-async.sh

FROM postgres:${PG_VERSION}-bookworm AS runtime
ARG PG_VERSION
COPY --from=builder /usr/lib/postgresql/${PG_VERSION}/lib/pg_redis.so \
                    /usr/lib/postgresql/${PG_VERSION}/lib/pg_redis.so
COPY --from=builder /usr/share/postgresql/${PG_VERSION}/extension/pg_redis.control \
                    /usr/share/postgresql/${PG_VERSION}/extension/pg_redis.control
COPY --from=builder /usr/share/postgresql/${PG_VERSION}/extension/pg_redis--1.0.sql \
                    /usr/share/postgresql/${PG_VERSION}/extension/pg_redis--1.0.sql
COPY --from=builder /usr/share/postgresql/${PG_VERSION}/extension/pg_redis--1.0--1.1.sql \
                    /usr/share/postgresql/${PG_VERSION}/extension/pg_redis--1.0--1.1.sql
COPY --from=builder /usr/share/postgresql/${PG_VERSION}/extension/pg_redis--1.1.sql \
                    /usr/share/postgresql/${PG_VERSION}/extension/pg_redis--1.1.sql
