//! Immutable sorted segment (SST-like) format.
//!
//! M1 layout (correctness-first, block indexing arrives in M2):
//! ```text
//! [magic u32]["WOLS"][count u64]
//!   per entry: key bytes | flag u8 | (value bytes)?
//! [crc32 u32 over everything above]
//! ```
//! `flag == 0` marks a tombstone (deleted key), `flag == 1` a live value.

use serde::{Deserialize, Serialize};

use crate::{
  codec::{Reader, put_bytes, put_u32, put_u64},
  error::{Error, Result},
};

/// Magic identifying a segment payload.
pub const SEG_MAGIC: u32 = 0x574F_4C53; // "WOLS"

/// A decoded segment: sorted (key, value) pairs where None = tombstone.
pub type SegmentEntries = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// Persistent metadata of one immutable segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentMeta {
  /// Globally unique segment id (also the object suffix).
  pub id: u64,
  /// Journal watermark reached when this segment was flushed: every journal
  /// group `<= seq` targeting this partition is included in the segment.
  pub seq: u64,
  /// Smallest key (bounds for read skipping).
  pub first: Vec<u8>,
  /// Largest key.
  pub last: Vec<u8>,
  /// Number of encoded entries (including tombstones).
  pub count: u64,
  /// Number of tombstone entries.
  pub tombstones: u64,
  /// Encoded object size in bytes.
  pub bytes: u64,
}

/// Encode a sorted entry list into a segment payload.
///
/// `entries` must be sorted by key and free of duplicate keys. `None` value
/// encodes a tombstone.
pub fn encode_segment(entries: &SegmentEntries) -> Result<Vec<u8>> {
  let mut body = Vec::with_capacity(32 + entries.len() * 32);
  put_u32(&mut body, SEG_MAGIC);
  put_u64(&mut body, entries.len() as u64);
  for (key, value) in entries {
    put_bytes(&mut body, key)?;
    match value {
      Some(v) => {
        body.push(1u8);
        put_bytes(&mut body, v)?;
      }
      None => body.push(0u8),
    }
  }
  let crc = crc32fast::hash(&body);
  put_u32(&mut body, crc);
  Ok(body)
}

/// Decode a segment payload back into a sorted entry list.
pub fn decode_segment(buf: &[u8]) -> Result<SegmentEntries> {
  if buf.len() < 4 {
    return Err(Error::Corrupt("segment too short".into()));
  }
  let (body, crc_b) = buf.split_at(buf.len() - 4);
  let expect = u32::from_le_bytes([crc_b[0], crc_b[1], crc_b[2], crc_b[3]]);
  if crc32fast::hash(body) != expect {
    return Err(Error::Corrupt("segment checksum mismatch".into()));
  }
  let mut r = Reader::new(body);
  let magic = r.u32()?;
  if magic != SEG_MAGIC {
    return Err(Error::Corrupt(format!("bad segment magic {magic:#x}")));
  }
  let count = r.u64()?;
  let mut entries = Vec::with_capacity(count as usize);
  let mut prev: Option<Vec<u8>> = None;
  for _ in 0..count {
    let key = r.bytes()?;
    if let Some(p) = &prev
      && &key <= p
    {
      return Err(Error::Corrupt(
        "segment keys out of order / duplicated".into(),
      ));
    }
    let flag = r.u8()?;
    let value = match flag {
      0 => None,
      1 => Some(r.bytes()?),
      other => return Err(Error::Corrupt(format!("bad segment flag {other}"))),
    };
    prev = Some(key.clone());
    entries.push((key, value));
  }
  if r.remaining() != 0 {
    return Err(Error::Corrupt("trailing bytes after segment".into()));
  }
  Ok(entries)
}

/// Build [`SegmentMeta`] from an encoded payload (without re-encoding).
pub fn meta_for(id: u64, seq: u64, encoded: &[u8], entries: &SegmentEntries) -> SegmentMeta {
  let tombstones = entries.iter().filter(|(_, v)| v.is_none()).count() as u64;
  SegmentMeta {
    id,
    seq,
    first: entries.first().map(|(k, _)| k.clone()).unwrap_or_default(),
    last: entries.last().map(|(k, _)| k.clone()).unwrap_or_default(),
    count: entries.len() as u64,
    tombstones,
    bytes: encoded.len() as u64,
  }
}
