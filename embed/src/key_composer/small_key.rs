use std::{
  borrow::Borrow,
  cmp::Ordering,
  hash::{Hash, Hasher},
  ops::{Deref, DerefMut},
};

pub const INLINE_CAP: usize = 55;

/// Stack-allocated 64-byte key aligned with CPU L1 cache line for zero-heap operations.
/// 栈上精确 64 字节单 Cache Line 快速键类型（对齐 CPU L1 缓存行，零堆内存分配）
#[derive(Clone, Debug)]
pub enum SmallKey {
  Inline { buf: [u8; INLINE_CAP], len: u8 },
  Heap(Vec<u8>),
}

impl Default for SmallKey {
  #[inline(always)]
  fn default() -> Self {
    Self::Inline {
      buf: [0u8; INLINE_CAP],
      len: 0,
    }
  }
}

impl SmallKey {
  #[inline(always)]
  pub fn new() -> Self {
    Self::default()
  }

  #[inline]
  pub fn from_slice(s: &[u8]) -> Self {
    if s.len() <= INLINE_CAP {
      let mut buf = [0u8; INLINE_CAP];
      buf[..s.len()].copy_from_slice(s);
      Self::Inline {
        buf,
        len: s.len() as u8,
      }
    } else {
      Self::Heap(s.to_vec())
    }
  }

  #[inline]
  pub fn push(&mut self, b: u8) {
    match self {
      Self::Inline { buf, len } => {
        let cur_len = *len as usize;
        if cur_len < INLINE_CAP {
          buf[cur_len] = b;
          *len += 1;
        } else {
          let mut v = Vec::with_capacity(cur_len + 17);
          v.extend_from_slice(&buf[..cur_len]);
          v.push(b);
          *self = Self::Heap(v);
        }
      }
      Self::Heap(v) => v.push(b),
    }
  }

  #[inline]
  pub fn extend_from_slice(&mut self, s: &[u8]) {
    match self {
      Self::Inline { buf, len } => {
        let cur_len = *len as usize;
        if cur_len + s.len() <= INLINE_CAP {
          buf[cur_len..cur_len + s.len()].copy_from_slice(s);
          *len += s.len() as u8;
        } else {
          let mut v = Vec::with_capacity(cur_len + s.len() + 16);
          v.extend_from_slice(&buf[..cur_len]);
          v.extend_from_slice(s);
          *self = Self::Heap(v);
        }
      }
      Self::Heap(v) => v.extend_from_slice(s),
    }
  }

  #[inline(always)]
  pub fn len(&self) -> usize {
    match self {
      Self::Inline { len, .. } => *len as usize,
      Self::Heap(v) => v.len(),
    }
  }

  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  #[inline(always)]
  pub fn clear(&mut self) {
    match self {
      Self::Inline { len, .. } => *len = 0,
      Self::Heap(v) => v.clear(),
    }
  }

  #[inline(always)]
  pub fn as_bytes(&self) -> &[u8] {
    match self {
      Self::Inline { buf, len } => &buf[..*len as usize],
      Self::Heap(v) => v.as_slice(),
    }
  }

  #[inline(always)]
  pub fn as_slice(&self) -> &[u8] {
    self.as_bytes()
  }

  #[inline(always)]
  pub fn to_vec(&self) -> Vec<u8> {
    self.as_bytes().to_vec()
  }

  #[inline(always)]
  pub fn into_vec(self) -> Vec<u8> {
    match self {
      Self::Inline { buf, len } => buf[..len as usize].to_vec(),
      Self::Heap(v) => v,
    }
  }
}

impl Deref for SmallKey {
  type Target = [u8];
  #[inline(always)]
  fn deref(&self) -> &[u8] {
    self.as_bytes()
  }
}

impl DerefMut for SmallKey {
  #[inline(always)]
  fn deref_mut(&mut self) -> &mut [u8] {
    match self {
      Self::Inline { buf, len } => &mut buf[..*len as usize],
      Self::Heap(v) => v.as_mut_slice(),
    }
  }
}

impl AsRef<[u8]> for SmallKey {
  #[inline(always)]
  fn as_ref(&self) -> &[u8] {
    self.as_bytes()
  }
}

impl Borrow<[u8]> for SmallKey {
  #[inline(always)]
  fn borrow(&self) -> &[u8] {
    self.as_bytes()
  }
}

impl From<&[u8]> for SmallKey {
  #[inline(always)]
  fn from(s: &[u8]) -> Self {
    Self::from_slice(s)
  }
}

impl From<&str> for SmallKey {
  #[inline(always)]
  fn from(s: &str) -> Self {
    Self::from_slice(s.as_bytes())
  }
}

impl From<Vec<u8>> for SmallKey {
  #[inline]
  fn from(v: Vec<u8>) -> Self {
    if v.len() <= INLINE_CAP {
      Self::from_slice(&v)
    } else {
      Self::Heap(v)
    }
  }
}

impl From<String> for SmallKey {
  #[inline(always)]
  fn from(s: String) -> Self {
    Self::from(s.into_bytes())
  }
}

impl From<SmallKey> for Vec<u8> {
  #[inline(always)]
  fn from(k: SmallKey) -> Self {
    k.into_vec()
  }
}

impl PartialEq for SmallKey {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.as_bytes() == other.as_bytes()
  }
}

impl Eq for SmallKey {}

impl Hash for SmallKey {
  #[inline]
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_bytes().hash(state);
  }
}

impl PartialOrd for SmallKey {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for SmallKey {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_bytes().cmp(other.as_bytes())
  }
}

#[cfg(test)]
mod tests {
  use std::mem::{align_of, size_of};

  use super::*;

  #[test]
  fn test_small_key_exact_cache_line_size() {
    assert_eq!(
      size_of::<SmallKey>(),
      64,
      "SmallKey must be exactly 64 bytes to fit in a single CPU L1 cache line"
    );
    assert_eq!(
      align_of::<SmallKey>(),
      8,
      "SmallKey alignment must be 8 bytes on 64-bit platforms"
    );
  }

  #[test]
  fn test_small_key_inline_and_heap_boundaries() {
    // 0 长度
    let empty = SmallKey::new();
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.as_slice(), b"");

    // 刚好 55 字节（INLINE_CAP）
    let data55 = vec![b'k'; INLINE_CAP];
    let sk55 = SmallKey::from_slice(&data55);
    assert!(matches!(sk55, SmallKey::Inline { .. }));
    assert_eq!(sk55.len(), INLINE_CAP);
    assert_eq!(sk55.as_bytes(), &data55[..]);

    // 56 字节（超出 INLINE_CAP 触发堆分配）
    let data56 = vec![b'k'; INLINE_CAP + 1];
    let sk56 = SmallKey::from_slice(&data56);
    assert!(matches!(sk56, SmallKey::Heap(_)));
    assert_eq!(sk56.len(), INLINE_CAP + 1);
    assert_eq!(sk56.as_bytes(), &data56[..]);

    // 逐步 push 跨越边界
    let mut sk = SmallKey::new();
    for i in 0..INLINE_CAP {
      sk.push(i as u8);
      assert!(matches!(sk, SmallKey::Inline { .. }));
    }
    assert_eq!(sk.len(), INLINE_CAP);
    sk.push(99);
    assert!(matches!(sk, SmallKey::Heap(_)));
    assert_eq!(sk.len(), INLINE_CAP + 1);
    assert_eq!(sk[INLINE_CAP], 99);

    // extend_from_slice 跨越边界
    let mut sk_ext = SmallKey::from_slice(b"prefix");
    sk_ext.extend_from_slice(&[b'x'; 60]);
    assert!(matches!(sk_ext, SmallKey::Heap(_)));
    assert_eq!(sk_ext.len(), 66);
    assert!(sk_ext.starts_with(b"prefix"));

    // into_vec 转换
    let vec_res = sk55.into_vec();
    assert_eq!(vec_res, data55);
  }
}
