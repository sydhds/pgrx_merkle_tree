use std::str::FromStr;
use std::sync::OnceLock;
// third-party
use ark_serialize::{CanonicalSerialize, CanonicalDeserialize};
use ark_bn254::Fr;
use ark_ff::AdditiveGroup;
// sqlx
use sqlx::{postgres::{
    PgHasArrayType,
    PgTypeInfo,
    types::Oid,
    PgArgumentBuffer,
    PgValueRef,
    Postgres
}, Type, Encode, Decode, Pool};

// Cache for pgfr oid
static PGFR_OID: OnceLock<Oid> = OnceLock::new();
// Cache for pgfr array oid (type: _pgrf)
// Note:
// Postgres automatically creates an array type named with an underscore usually.
static PGFR_ARRAY_OID: OnceLock<Oid> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct PgFrStruct {
    pub inner: Fr,
}

impl Type<Postgres> for PgFrStruct {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        // sqlx::postgres::PgTypeInfo::with_name("pgfr")
        let oid = *PGFR_OID.get().expect("PGFR_OID must be initialized in main()");
        PgTypeInfo::with_oid(oid)
    }
}

impl<'q> Encode<'q, Postgres> for PgFrStruct {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        let mut temp_buf = Vec::with_capacity(32);
        self.inner.serialize_compressed(&mut temp_buf)?;
        buf.extend_from_slice(&temp_buf);
        Ok(sqlx::encode::IsNull::No)
    }
}

impl<'r> Decode<'r, Postgres> for PgFrStruct {
    fn decode(value: PgValueRef<'r>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let bytes = value.as_bytes()?;
        let fr = Fr::deserialize_compressed(bytes)?;
        Ok(PgFrStruct { inner: fr })
    }
}

impl PgHasArrayType for PgFrStruct {
    fn array_type_info() -> PgTypeInfo {
        // PgTypeInfo::with_name("_pgfr")
        let oid = *PGFR_ARRAY_OID.get().expect("PGFR_ARRAY_OID must be initialized");
        PgTypeInfo::with_oid(oid)
    }
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    env_logger::init();

    // TODO: arg
    let db_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "postgres://sydh:sydh@localhost:28818/pg_merkle_tree".to_string());
    let pool = sqlx::PgPool::connect(db_url.as_str()).await?;

    // Get pgfr oid and pgfr array oid in order to cache them for the queries
    let row: (Oid, Oid) = sqlx::query_as("SELECT oid, typarray FROM pg_type WHERE typname = 'pgfr'")
        .fetch_one(&pool)
        .await?;

    PGFR_OID.set(row.0).expect("Failed to set PGFR_OID");
    PGFR_ARRAY_OID.set(row.1).expect("Failed to set array OID");

    println!("pgfr oid: {:?} - pgfr array oid: {:?}", PGFR_OID, PGFR_ARRAY_OID);

    // Basic queries
    // Note: updating a value manually like this breaks the merkle tree (hashes not updated)
    //       pgfr_mtree_set_leaf should be used instead

    let v = PgFrStruct { inner: Fr::from(4242) };
    // Compute the 'real' leaf index
    const DEPTH: i16 = 20;
    let leaf_index_7_ = (1 << DEPTH) + 7 - 1;
    let leaf_index_7 = leaf_index_7_ as i64;

    println!("Inserting value: {v:?} at index 7...");
    sqlx::query("UPDATE pgfr_mtree SET value = $1 WHERE index_in_mtree = $2")
        .bind(&v)
        .bind(leaf_index_7)
        .execute(&pool)
        .await?;

    let row: (PgFrStruct,) = sqlx::query_as("SELECT value FROM pgfr_mtree WHERE index_in_mtree = $1")
        .bind(leaf_index_7)
        .fetch_one(&pool)
        .await?;

    println!("Value at index 7: {:?}", row.0.inner);
    assert_eq!(row.0.inner, Fr::from(4242));

    // Restore previous value
    let v2 = PgFrStruct { inner: Fr::ZERO };
    println!("Reseting value at index 7...");
    sqlx::query("UPDATE pgfr_mtree SET value = $1 WHERE index_in_mtree = $2")
        .bind(&v2)
        .bind(leaf_index_7)
        .execute(&pool)
        .await?;

    // ~Benchmarking set_leaf

    bench_set_leaf(pool.clone(), DEPTH).await?;

    // ~Benchmarking get_proof
    // TODO

    Ok(())
}

async fn bench_set_leaf(pool: Pool<Postgres>, depth: i16) -> Result<(), sqlx::Error> {

    let mut conn = pool.acquire().await?;

    // Warmup
    let v0 = PgFrStruct { inner: Fr::from(0) };
    let _res = sqlx::query("SELECT pgfr_mtree_set_leaf($1, $2, $3)")
        .bind(depth) // depth
        .bind(0i64)
        .bind(&v0)
        .execute(&mut *conn)
        .await?;
    let _res = sqlx::query("SELECT pgfr_mtree_set_leaf($1, $2, $3)")
        .bind(depth) // depth
        .bind(0i64)
        .bind(&v0)
        .execute(&mut *conn)
        .await?;

    // ~Benchmark
    let v = PgFrStruct { inner: Fr::from(2) };
    {
        let start = std::time::Instant::now();
        let res = sqlx::query("SELECT pgfr_mtree_set_leaf($1, $2, $3)")
            .bind(depth) // depth
            .bind(0i64)
            .bind(&v)
            .execute(&mut *conn)
            .await?;

        let elapsed = start.elapsed();
        println!("[set leaf] res: {:?}", res);
        println!("elapsed: {:?} secs ({} ms)", elapsed.as_secs_f64(), elapsed.as_millis());
    }

    {
        let v2 = PgFrStruct { inner: Fr::from(42) };

        let start = std::time::Instant::now();
        let res = sqlx::query("SELECT pgfr_mtree_set_leaf($1, $2, $3)")
            .bind(depth) // depth
            .bind(7i64)
            .bind(&v2)
            .execute(&mut *conn)
            .await?;

        let elapsed = start.elapsed();
        println!("[set leaf] res: {:?}", res);
        println!("elapsed: {:?} secs ({} ms)", elapsed.as_secs_f64(), elapsed.as_millis());
    }

    // Get root test
    let row: (PgFrStruct,) = sqlx::query_as("SELECT pgfr_mtree_get_root()")
        .fetch_one(&pool)
        .await?;
    println!("Root: {:?}", row.0.inner);

    assert_eq!(row.0.inner, Fr::from_str("20005511697733701318510026485221552683808692907978965709352926666824577974588").unwrap());

    Ok(())

}
