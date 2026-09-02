use crate::{
  key_composer::KeyTag,
  meta::{KeyMeta, MetaOps, RedisType},
};

/// 有序整型集合结构元数据（对标 Apache Kvrocks SortedintMetadata 26字节 / 紧凑25字节）
#[derive(Debug, Clone, Copy, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
pub struct SortedintMeta {
  pub base: KeyMeta,
}

impl SortedintMeta {
  pub const ENCODED_SIZE: usize = KeyMeta::ENCODED_SIZE;
  pub const KVROCKS_ENCODED_SIZE: usize = KeyMeta::KVROCKS_COMPLEX_ENCODED_SIZE;

  #[inline]
  pub const fn new(expire_at: u64, version: u64, size: u64) -> Self {
    Self {
      base: KeyMeta::new(RedisType::SortedInt, expire_at, version, size),
    }
  }

  #[inline]
  pub fn new_with_version(expire_at: u64, size: u64) -> Self {
    Self {
      base: KeyMeta::new_with_version(RedisType::SortedInt, expire_at, size),
    }
  }

  #[inline]
  pub const fn size(&self) -> u64 {
    self.base.size
  }

  #[inline]
  pub const fn version(&self) -> u64 {
    self.base.version
  }

  #[inline]
  pub const fn expire_at(&self) -> u64 {
    self.base.expire_at
  }

  #[inline]
  pub const fn ttl(&self, now_ms: u64) -> i64 {
    self.base.ttl(now_ms)
  }

  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.base.size == 0
  }

  #[inline]
  pub const fn is_expired(&self, now_ms: u64) -> bool {
    self.base.is_expired(now_ms)
  }

  #[inline]
  pub fn encode(&self) -> [u8; Self::ENCODED_SIZE] {
    self.base.encode()
  }

  #[inline]
  pub fn encode_kvrocks(&self) -> Vec<u8> {
    self.base.encode_kvrocks()
  }

  #[inline]
  pub fn decode(bytes: &[u8]) -> Option<Self> {
    let base = KeyMeta::decode(bytes)?;
    if base.rtype == RedisType::SortedInt {
      Some(Self { base })
    } else {
      None
    }
  }
}

impl Default for SortedintMeta {
  #[inline]
  fn default() -> Self {
    Self::new_with_version(0, 0)
  }
}

impl MetaOps for SortedintMeta {
  const TAG: &[u8] = KeyTag::SortedIntMeta.as_slice();
  type EncodedBytes = [u8; Self::ENCODED_SIZE];

  #[inline]
  fn decode(bytes: &[u8]) -> Option<Self> {
    Self::decode(bytes)
  }

  #[inline]
  fn is_expired(&self, now_ms: u64) -> bool {
    self.base.is_expired(now_ms)
  }

  #[inline]
  fn encode_bytes(&self) -> Self::EncodedBytes {
    self.encode()
  }

  #[inline]
  fn base(&self) -> &KeyMeta {
    &self.base
  }

  #[inline]
  fn base_mut(&mut self) -> &mut KeyMeta {
    &mut self.base
  }
}
