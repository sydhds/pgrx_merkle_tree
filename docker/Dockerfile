# Stage 1: build the pg extension
FROM ubuntu:25.10 as builder

RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    libclang-dev \
    pkg-config \
    libssl-dev \
    git \
    postgresql-server-dev-18

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.91.0
ENV PATH="/root/.cargo/bin:${PATH}"

RUN cargo install cargo-pgrx --locked

RUN cargo pgrx init --pg18 /usr/lib/postgresql/18/bin/pg_config

WORKDIR /app
COPY . .

WORKDIR /app/pg_merkle_tree
RUN cargo pgrx package --pg-config /usr/lib/postgresql/18/bin/pg_config

# Stage 2: Postgres Runtime
FROM postgres:18-bookworm

# COPY pgrx files
COPY --from=builder \
     /app/target/release/pg_merkle_tree-pg18/usr/lib/postgresql/18/lib/pg_merkle_tree.so \
     /usr/lib/postgresql/18/lib/

COPY --from=builder \
     /app/target/release/pg_merkle_tree-pg18/usr/share/postgresql/18/extension/* \
     /usr/share/postgresql/18/extension/

# COPY init script
COPY docker/init-db.sh /docker-entrypoint-initdb.d/init-db-extension.sh