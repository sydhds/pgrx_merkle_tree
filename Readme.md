# Pgrx merkle tree

A postgresql extension (using pgrx) to store a merkle tree in a Postgresql DB

# Description

* The pg extension defines a new type `PgFr` to store Fr type (a field element) efficiently in Postgresql

# Compile pg extension

* `cd pg_merkle_tree`
* `cargo build --lib --features pg18 --no-default-features`

Get a psql shell:
* `cargo pgrx run pg18`

Run tests:
* `cargo pgrx test pg18`
* `cargo pgrx test pg18 test_pgfr_is_32_bytes`