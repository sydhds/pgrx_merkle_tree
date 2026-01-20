use std::collections::BTreeMap;
// std
use std::iter::repeat_n;
// third-party
use ark_bn254::Fr;
use pgrx::datum::DatumWithOid;
// pgrx
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiResult};
use crate::PgFr;
use crate::poseidon::poseidon_hash_;
use crate::merkle_tree_utils::{node_parent, first_child};

#[pg_extern]
fn pgfr_mtree_init(depth: i64) {

    let depth = depth as usize;

    // initialize merkle tree nodes (root, nodes & leaves)
    let mut levels = Vec::with_capacity(depth + 1);
    levels.push(Fr::default());
    (0..depth).for_each(|level_index| {
        levels.push(poseidon_hash_(&[levels[level_index]; 2]))
    });

    let nodes: Vec<Fr> = levels
        .iter()
        .rev()
        .enumerate()
        .flat_map(|(level_index, hash_value)| {
            // per level, repeat the hash value (1 for root, 2 for level 1, ...)
            repeat_n(hash_value, 1 << level_index)
        })
        .cloned()
        .collect();

    let query = "INSERT INTO pgfr_mtree (index_in_mtree, value) VALUES ($1, $2::pgfr)";
    Spi::connect_mut(|client| {
        for (index_in_mtree, node) in nodes.iter().enumerate() {
            let index_in_mtree = index_in_mtree as i64;
            client.update(
                query,
                None,
                &[
                    index_in_mtree.into(),
                    PgFr(*node).into()
                ]).unwrap();
        }
    });
}

#[pg_extern]
fn pgfr_mtree_get_root() -> Result<Option<PgFr>, pgrx::spi::Error> {

    let res: SpiResult<Option<PgFr>> = Spi::get_one_with_args(
        "SELECT value::pgfr FROM pgfr_mtree WHERE index_in_mtree = 0 LIMIT 1;",
        &[]
    );

    res
}

#[pg_extern(parallel_unsafe)]
fn pgfr_mtree_set_leaf(depth: i16, index_in_mtree: i64, leaf_value: PgFr) -> Result<(), pgrx::spi::Error> {

    let index = index_in_mtree as usize;
    let leaf_index_ = (1 << depth) + index - 1;
    let leaf_index = leaf_index_ as i64;

    let query = "UPDATE pgfr_mtree SET value = $1 WHERE index_in_mtree = $2";

    Spi::run_with_args(
        query,
        &[
            unsafe { DatumWithOid::new(leaf_value, PgFr::type_oid()) },
            leaf_index.into()
        ]
    )?;

    // Get index and new hashes to insert in tree after leaf update
    let mut to_update = BTreeMap::new();
    Spi::connect(|client| {
        mtree_get_hashes(client, leaf_index_, leaf_index_, &mut to_update);
    });

    let (to_update_indexes, to_update_values): (Vec<i64>, Vec<PgFr>) = to_update.into_iter().unzip();

    let query_2 = r#"
        UPDATE pgfr_mtree
        SET value = data.new_value
        FROM (
            SELECT * FROM UNNEST($1::bigint[], $2::pgfr[])
            AS t(i_index, new_value)
        ) AS data
        WHERE pgfr_mtree.index_in_mtree = data.i_index;
        "#;

    Spi::run_with_args(query_2,
                       &[
                           to_update_indexes.into(),
                           to_update_values.into()
                       ]
    )?;

    Ok(())
}

fn mtree_get_hashes(client: &SpiClient, start_index: usize, end_index: usize, to_update: &mut BTreeMap<i64, PgFr>) {

    let query_1 = "SELECT value::pgfr FROM pgfr_mtree WHERE index_in_mtree = $1 LIMIT 1";

    let mut start_index = start_index;
    let mut end_index = end_index;

    while let (Some(start_parent), Some(end_parent)) = (node_parent(start_index), node_parent(end_index)) {

        for parent in start_parent..=end_parent {

            // iter over parent nodes - for each parent, get left child and right child 'value' column
            let left_child_ = first_child(parent);
            let right_child_ = left_child_ + 1;

            let left_child = left_child_ as i64;
            let right_child = right_child_ as i64;

            // Get value for left child
            let left_child_value = if to_update.contains_key(&left_child) {
                to_update[&left_child]
            } else {
                let left_child_value_ = client.select(query_1, None, &[left_child.into()]);
                let left_child_value = left_child_value_
                    .unwrap() // unwrap safe: assume merkle tree table has been correctly initialized
                    .first() // SELECT query only returns 1 element
                    .get_one::<PgFr>() // SELECT query returns only column 'value'
                    .unwrap()// unwrap safe: SELECT query returns only column 'value'
                    .unwrap(); // unwrap safe: assume 'value' column is always initialized
                left_child_value
            };

            // Get value for right child
            let right_child_value = if to_update.contains_key(&right_child) {
                to_update[&right_child]
            } else {
                let right_child_value_ = client.select(query_1, None, &[right_child.into()]);
                let right_child_value = right_child_value_
                    .unwrap()
                    .first()
                    .get_one::<PgFr>()
                    .unwrap()
                    .unwrap();
                right_child_value
            };

            // Compute hash
            let value = poseidon_hash_(&[left_child_value.0, right_child_value.0]);
            let parent_ = parent as i64;

            // Store it in our hashmap (db will be updated later in bulk)
            to_update.insert(parent_, PgFr(value));

            // Loop until we reach the merkle tree root node (which has no parent)
            start_index = start_parent;
            end_index = end_parent;
        }
    }
}



#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {

    use std::str::FromStr;
    use super::*;

    #[pg_test]
    fn test_merkle_tree_init() {

        let _res = Spi::run("
            CREATE TABLE pgfr_mtree (index_in_mtree bigint, value pgfr);
            CREATE UNIQUE INDEX pgfr_mtree_index ON pgfr_mtree (index_in_mtree);
            "
        );

        pgfr_mtree_init(3);

        // Get root manually
        let root_node = Spi::get_one::<PgFr>("SELECT value FROM pgfr_mtree WHERE index_in_mtree = 0;").unwrap().unwrap();
        assert_eq!(root_node.0, Fr::from_str("11286972368698509976183087595462810875513684078608517520839298933882497716792").unwrap());
    }

    #[pg_test]
    fn test_merkle_tree_get_root() {

        let _res = Spi::run("
            CREATE TABLE pgfr_mtree (index_in_mtree bigint, value pgfr);
            CREATE UNIQUE INDEX pgfr_mtree_index ON pgfr_mtree (index_in_mtree);
            "
        );

        pgfr_mtree_init(3);

        // Get root manually
        let root_node = Spi::get_one::<PgFr>("SELECT value FROM pgfr_mtree WHERE index_in_mtree = 0;").unwrap().unwrap();

        let root_node_2 = pgfr_mtree_get_root().unwrap().unwrap();

        assert_eq!(root_node.0, Fr::from_str("11286972368698509976183087595462810875513684078608517520839298933882497716792").unwrap());
        assert_eq!(root_node.0, root_node_2.0);
    }

    #[pg_test]
    fn test_pgfr_set_leaf() {
        let _res = Spi::run("
            CREATE TABLE pgfr_mtree (index_in_mtree bigint, value pgfr);
            CREATE UNIQUE INDEX pgfr_mtree_index ON pgfr_mtree (index_in_mtree);
            "
        );
        pgfr_mtree_init(3);

        pgfr_mtree_set_leaf(3, 0, PgFr(Fr::from(2))).unwrap();
        let root = pgfr_mtree_get_root().unwrap().unwrap();
        assert_eq!(root.0, Fr::from_str("3799385896495180565562780950112041501871782716691607926126180421168246094289").unwrap());

        pgfr_mtree_set_leaf(3, 7, PgFr(Fr::from(42))).unwrap();
        let root = pgfr_mtree_get_root().unwrap().unwrap();
        assert_eq!(root.0, Fr::from_str("9164054056146260648413073295070635933539618302378139976693739565479035405901").unwrap());
    }



}

