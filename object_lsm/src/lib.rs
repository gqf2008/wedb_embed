//! wedb_object_lsm —— Object-storage-backed LSM storage engine.
//!
//! Implements the [`wedb_embed_engine`] [`Engine`](wedb_embed_engine::Engine) /
//! [`Partition`](wedb_embed_engine::Partition) / [`Batch`](wedb_embed_engine::Batch)
//! traits on top of S3-compatible object storage (AWS S3 / Cloudflare R2 / MinIO).
//!
//! Data model:
//! - committed writes first land in a per-instance **journal** (immutable objects),
//!   giving cross-partition atomic batches + crash-safe replay;
//! - per-partition **memtables** absorb recent writes for µs-scale hot reads;
//! - full memtables are flushed into immutable sorted **segment** objects;
//! - a small **manifest** object records live segments + per-partition watermark.
//!
//! M1 (current milestone): correct vertical slice over the in-memory [`Store`]
//! implementation; remote R2/S3 backend and block-indexed segments follow in M2+.

pub mod batch;
pub mod codec;
pub mod config;
pub mod engine;
pub mod error;
pub mod journal;
pub mod keys;
pub mod manifest;
pub mod partition;
pub mod segment;
pub mod state;
pub mod store;

pub use batch::ObjectLsmBatch;
pub use config::Config;
pub use engine::ObjectLsm;
pub use error::{Error, Result};
pub use partition::{ObjectLsmEntry, ObjectLsmIter, ObjectLsmPartition};
pub use store::{MemoryStore, Store};
