# Pgrx merkle tree

A postgresql extension (using pgrx) to store a merkle tree in a Postgresql DB

## Description

* The pg extension defines a new type `PgFr` to store Fr type (a field element from [Ark crates](https://github.com/arkworks-rs/algebra)) efficiently in Postgresql
* A merkle tree is stored in a Postgresql table (One tree per table)
  * The hashing function is the Poseidon hash function 
  * A set of postgresql functions are provided to manipulate the tree (tree initialization, get root, update leaf, get proof)
* The following example is provided to illustrate the usage of the extension using the Postgresql binary protocol:
  * sqlx_binary_protocol

## Limitations

* Performances are good enough for a merkle tree of depth <= 20 (set_leaf around ~ 10 ms)
* For huge depth, set_leaf might exceed the number of allowed postgres parameters
* Indexes in the merkle tree are passed as bigint (or i64) then converted to usize in the rust code.

### Requirements

* Table structure for the merkle tree :
  * `CREATE TABLE pgfr_mtree (index_in_mtree bigint, value pgfr);`
  * `CREATE UNIQUE INDEX pgfr_mtree_index_index_in_mtree ON pgfr_mtree (index_in_mtree);`

## Development

### Compile pg extension

* `cd pg_merkle_tree`
* `cargo build --lib --features pg18 --no-default-features`

Get a psql shell:
* `cargo pgrx run pg18`
  * `CREATE EXTENSION pg_merkle_tree;`
  * 

Run tests:
* `cargo pgrx test pg18`
* `cargo pgrx test pg18 test_pgfr_is_32_bytes`

### Helpers

psql:
* Listing data types: `\dT+`
* Listing functions: `\df+`
* Enable timing: `\timing`

## sqlx_binary_protocol

### Run

* Start the psql shell with the pg extension loaded and merkle tree initialized (depth = 20)
* `cargo run -- DB_URL`
* View queries:
    * `RUST_LOG=sqlx=debug cargo run -- DB_URL`

## zerokit_ref

Use Zerokit crate to compute reference values for the unit tests in pg_merkle_tree

