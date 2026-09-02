//! Journal record-group format.
//!
//! A committed batch is serialized as one [`Group`] containing an ordered op
//! list that may span multiple partitions (`data` + `meta` in wedb_embed).
//! Each group is stored as its own immutable journal object, which makes a PUT
//! the atomic durability point: an uploaded group is either fully present or
//! absent — there is no torn tail to repair on recovery.

use crate::{
  codec::{Reader, put_bytes, put_str, put_u32, put_u64},
  error::{Error, Result},
};

/// Magic identifying a journal group payload: `"WGL1"`.
pub const GROUP_MAGIC: u32 = 0x5747_4C31;

/// A single key-value mutation targeting one partition.
///
/// `value == None` encodes a delete (tombstone).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op {
  /// Target partition (keyspace) name.
  pub part: String,
  /// Key bytes.
  pub key: Vec<u8>,
  /// New value, or `None` to delete.
  pub value: Option<Vec<u8>>,
}

impl Op {
  pub fn put(part: impl Into<String>, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
    Self {
      part: part.into(),
      key: key.into(),
      value: Some(value.into()),
    }
  }

  pub fn delete(part: impl Into<String>, key: impl Into<Vec<u8>>) -> Self {
    Self {
      part: part.into(),
      key: key.into(),
      value: None,
    }
  }
}

/// An atomically committed group of mutations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
  /// Monotonic sequence assigned at commit time.
  pub seq: u64,
  pub ops: Vec<Op>,
}

pub fn encode_group(g: &Group) -> Result<Vec<u8>> {
  let mut out = Vec::with_capacity(64 + g.ops.len() * 32);
  put_u32(&mut out, GROUP_MAGIC);
  put_u64(&mut out, g.seq);
  put_u32(&mut out, g.ops.len() as u32);
  for op in &g.ops {
    put_str(&mut out, &op.part)?;
    put_bytes(&mut out, &op.key)?;
    match &op.value {
      Some(v) => {
        out.push(1u8);
        put_bytes(&mut out, v)?;
      }
      None => out.push(0u8),
    }
  }
  Ok(out)
}

pub fn decode_group(buf: &[u8]) -> Result<Group> {
  let mut r = Reader::new(buf);
  let magic = r.u32()?;
  if magic != GROUP_MAGIC {
    return Err(Error::Corrupt(format!("bad journal magic {magic:#x}")));
  }
  let seq = r.u64()?;
  let count = r.u32()?;
  let mut ops = Vec::with_capacity(count as usize);
  for _ in 0..count {
    let part = r.str()?;
    let key = r.bytes()?;
    let flag = r.u8()?;
    let value = match flag {
      0 => None,
      1 => Some(r.bytes()?),
      other => return Err(Error::Corrupt(format!("bad journal op flag {other}"))),
    };
    ops.push(Op { part, key, value });
  }
  if r.remaining() != 0 {
    return Err(Error::Corrupt("trailing bytes after journal group".into()));
  }
  Ok(Group { seq, ops })
}
