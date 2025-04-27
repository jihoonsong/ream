use std::sync::Arc;

use alloy_primitives::B256;
use redb::{Database, Durability, MultimapTableDefinition};

use super::{
    MultimapTable, SSZEncoding
};
use crate::errors::StoreError;

/// Table definition for the Parent Root Index Multimap table
///
/// Key: ParentRoot
/// Value: BlockRoot's
pub const PARENT_ROOT_INDEX_MULTIMAP_TABLE: MultimapTableDefinition<SSZEncoding<B256>, SSZEncoding<B256>> = MultimapTableDefinition::new("parent_root_index_multimap");

pub struct ParentRootIndexMultimapTable {
    pub db: Arc<Database>,
}

impl MultimapTable for ParentRootIndexMultimapTable {
    type Key = B256;

    type GetValue = Vec<B256>;

    type InsertValue = B256;

    fn get(&self, key: Self::Key) -> Result<Option<Self::GetValue>, StoreError> {
        let read_txn = self.db.begin_read()?;

        let table = read_txn.open_multimap_table(PARENT_ROOT_INDEX_MULTIMAP_TABLE)?;
        let mut result = table.get(key)?;
        let mut values = vec![];
        while let Some(value) = result.next() {
            values.push(value?.value());
        };
        Ok(Some(values))
    }

    fn insert(&self, key: Self::Key, value: Self::InsertValue) -> Result<(), StoreError> {
        let mut write_txn = self.db.begin_write()?;
        write_txn.set_durability(Durability::Immediate);
        let mut table = write_txn.open_multimap_table(PARENT_ROOT_INDEX_MULTIMAP_TABLE)?;
        table.insert(key, value)?;
        drop(table);
        write_txn.commit()?;
        Ok(())
    }
}

pub struct ParentRootIndex<> {

}
