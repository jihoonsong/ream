use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use redb::{Builder, Database};

use crate::{
    dir,
    errors::StoreError,
    tables::{
        beacon_block::{BeaconBlockTable, BEACON_BLOCK_TABLE}, beacon_state::{BeaconStateTable, BEACON_STATE_TABLE}, blobs_and_proofs::{BlobsAndProofsTable, BLOBS_AND_PROOFS_TABLE}, block_timeliness::{BlockTimelinessTable, BLOCK_TIMELINESS_TABLE}, checkpoint_states::{CheckpointStatesTable, CHECKPOINT_STATES_TABLE}, equivocating_indices::{EquivocatingIndicesField, EQUIVOCATING_INDICES_FIELD}, finalized_checkpoint::{FinalizedCheckpointField, FINALIZED_CHECKPOINT_FIELD}, genesis_time::{GenesisTimeField, GENESIS_TIME_FIELD}, justified_checkpoint::{JustifiedCheckpointField, JUSTIFIED_CHECKPOINT_FIELD}, latest_messages::{LatestMessagesTable, LATEST_MESSAGES_TABLE}, parent_root_index::{ParentRootIndexMultimapTable, PARENT_ROOT_INDEX_MULTIMAP_TABLE}, proposer_boost_root::{ProposerBoostRootField, PROPOSER_BOOST_ROOT_FIELD}, slot_index::{SlotIndexTable, SLOT_INDEX_TABLE}, state_root_index::{StateRootIndexTable, STATE_ROOT_INDEX_TABLE}, time::{TimeField, TIME_FIELD}, unrealized_finalized_checkpoint::{
            UnrealizedFinalizedCheckpointField, UNREALIZED_FINALIZED_CHECKPOINT_FIELD
        }, unrealized_justifications::{
            UnrealizedJustificationsTable, UNREALIZED_JUSTIFICATIONS_TABLE
        }, unrealized_justified_checkpoint::{
            UnrealizedJustifiedCheckpointField, UNREALIZED_JUSTIFED_CHECKPOINT_FIELD
        }
    },
};

pub const APP_NAME: &str = "ream";

pub const REDB_FILE: &str = "ream.redb";

/// The size of the cache for the database
///
/// 1 GiB
pub const REDB_CACHE_SIZE: usize = 1_024 * 1_024 * 1_024;

#[derive(Clone, Debug)]
pub struct ReamDB {
    pub db: Arc<Database>,
}

#[allow(clippy::result_large_err)]
impl ReamDB {
    pub fn new(data_dir: Option<PathBuf>, ephemeral: bool) -> Result<Self, StoreError> {
        let ream_dir =
            dir::setup_data_dir(APP_NAME, data_dir, ephemeral).map_err(StoreError::Io)?;

        let ream_file = ream_dir.join(REDB_FILE);

        let db = Builder::new()
            .set_cache_size(REDB_CACHE_SIZE)
            .create(&ream_file)
            .map_err(|err| StoreError::Database(err.into()))?;

        let write_txn = db.begin_write()?;
        write_txn.open_table(BEACON_BLOCK_TABLE)?;
        write_txn.open_table(BEACON_STATE_TABLE)?;
        write_txn.open_table(BLOBS_AND_PROOFS_TABLE)?;
        write_txn.open_table(BLOCK_TIMELINESS_TABLE)?;
        write_txn.open_table(CHECKPOINT_STATES_TABLE)?;
        write_txn.open_table(EQUIVOCATING_INDICES_FIELD)?;
        write_txn.open_table(FINALIZED_CHECKPOINT_FIELD)?;
        write_txn.open_table(GENESIS_TIME_FIELD)?;
        write_txn.open_table(JUSTIFIED_CHECKPOINT_FIELD)?;
        write_txn.open_table(LATEST_MESSAGES_TABLE)?;
        write_txn.open_multimap_table(PARENT_ROOT_INDEX_MULTIMAP_TABLE)?;
        write_txn.open_table(PROPOSER_BOOST_ROOT_FIELD)?;
        write_txn.open_table(SLOT_INDEX_TABLE)?;
        write_txn.open_table(STATE_ROOT_INDEX_TABLE)?;
        write_txn.open_table(TIME_FIELD)?;
        write_txn.open_table(UNREALIZED_FINALIZED_CHECKPOINT_FIELD)?;
        write_txn.open_table(UNREALIZED_JUSTIFICATIONS_TABLE)?;
        write_txn.open_table(UNREALIZED_JUSTIFED_CHECKPOINT_FIELD)?;
        write_txn.commit()?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn beacon_block_provider(&self) -> BeaconBlockTable {
        BeaconBlockTable {
            db: self.db.clone(),
        }
    }

    pub fn beacon_state_provider(&self) -> BeaconStateTable {
        BeaconStateTable {
            db: self.db.clone(),
        }
    }

    pub fn blobs_and_proofs_provider(&self) -> BlobsAndProofsTable {
        BlobsAndProofsTable {
            db: self.db.clone(),
        }
    }

    pub fn block_timeliness_provider(&self) -> BlockTimelinessTable {
        BlockTimelinessTable {
            db: self.db.clone(),
        }
    }

    pub fn checkpoint_states_provider(&self) -> CheckpointStatesTable {
        CheckpointStatesTable {
            db: self.db.clone(),
        }
    }

    pub fn latest_messages_provider(&self) -> LatestMessagesTable {
        LatestMessagesTable {
            db: self.db.clone(),
        }
    }

    pub fn unrealized_justifications_provider(&self) -> UnrealizedJustificationsTable {
        UnrealizedJustificationsTable {
            db: self.db.clone(),
        }
    }

    pub fn parent_root_index_multimap_provider(&self) -> ParentRootIndexMultimapTable {
        ParentRootIndexMultimapTable {
            db: self.db.clone(),
        }
    }

    pub fn proposer_boost_root_provider(&self) -> ProposerBoostRootField {
        ProposerBoostRootField {
            db: self.db.clone(),
        }
    }

    pub fn unrealized_finalized_checkpoint_provider(&self) -> UnrealizedFinalizedCheckpointField {
        UnrealizedFinalizedCheckpointField {
            db: self.db.clone(),
        }
    }

    pub fn unrealized_justified_checkpoint_provider(&self) -> UnrealizedJustifiedCheckpointField {
        UnrealizedJustifiedCheckpointField {
            db: self.db.clone(),
        }
    }

    pub fn finalized_checkpoint_provider(&self) -> FinalizedCheckpointField {
        FinalizedCheckpointField {
            db: self.db.clone(),
        }
    }

    pub fn justified_checkpoint_provider(&self) -> JustifiedCheckpointField {
        JustifiedCheckpointField {
            db: self.db.clone(),
        }
    }

    pub fn genesis_time_provider(&self) -> GenesisTimeField {
        GenesisTimeField {
            db: self.db.clone(),
        }
    }

    pub fn time_provider(&self) -> TimeField {
        TimeField {
            db: self.db.clone(),
        }
    }

    pub fn equivocating_indices_provider(&self) -> EquivocatingIndicesField {
        EquivocatingIndicesField {
            db: self.db.clone(),
        }
    }

    pub fn slot_index_provider(&self) -> SlotIndexTable {
        SlotIndexTable {
            db: self.db.clone(),
        }
    }

    pub fn state_root_index_provider(&self) -> StateRootIndexTable {
        StateRootIndexTable {
            db: self.db.clone(),
        }
    }
}
