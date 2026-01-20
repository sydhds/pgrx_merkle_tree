use std::sync::OnceLock;
use sqlx::{postgres::{PgArgumentBuffer, PgValueRef, Postgres}, Type, Encode, Decode};
use ark_serialize::{CanonicalSerialize, CanonicalDeserialize};
// use ark_bls12_381::Fr; // adjust to your specific crate
use ark_bn254::Fr;
use sqlx::postgres::{PgHasArrayType, PgTypeInfo};
use sqlx::postgres::types::Oid;

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
        self.inner
            .serialize_compressed(&mut temp_buf)
            ?;
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

    let row: (Oid, Oid) = sqlx::query_as("SELECT oid, typarray FROM pg_type WHERE typname = 'pgfr'")
        .fetch_one(&pool)
        .await?;

    // Store it in the global cache
    PGFR_OID.set(row.0).expect("Failed to set PGFR_OID");
    PGFR_ARRAY_OID.set(row.1).expect("Failed to set array OID");

    println!("pgfr oid: {:?} - pgfr array oid: {:?}", PGFR_OID, PGFR_ARRAY_OID);

    // let depth: i16 = 20;
    Ok(())
}