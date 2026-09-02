//! Order-Preserving Prefix Varint (OPPV) encoding.
//! 保序变长整型编码（Order-Preserving Prefix Varint - OPPV）。
//! Encodes data into binary format.
//! 保证对任意 u64 a < b，其二进制编码在字节字典序上恒满足 a_bytes < b_bytes（支持 memcmp/LSM Range Seek 直接保序）。

/// Returns exact byte length required to encode a u64 in OPPV format.
/// 计算 OPPV 编码 u64 所需的精确字节数（零堆分配，纯 CPU 快速分支）
#[inline(always)]
pub const fn oppv_len_u64(val: u64) -> usize {
  if val < 0x80 {
    1
  } else if val < 0x4000 {
    2
  } else if val < 0x20_0000 {
    3
  } else if val < 0x1000_0000 {
    4
  } else {
    9
  }
}

/// Encodes a u64 in OPPV format into a mutable slice without heap allocation.
/// 栈上零堆分配编码 OPPV u64 到切片缓冲区，返回写入字节数
#[inline(always)]
pub fn encode_oppv_u64_slice(val: u64, buf: &mut [u8]) -> usize {
  if val < 0x80 {
    buf[0] = val as u8;
    1
  } else if val < 0x4000 {
    buf[0] = 0x80 | ((val >> 8) as u8);
    buf[1] = (val & 0xFF) as u8;
    2
  } else if val < 0x20_0000 {
    buf[0] = 0xC0 | ((val >> 16) as u8);
    buf[1] = ((val >> 8) & 0xFF) as u8;
    buf[2] = (val & 0xFF) as u8;
    3
  } else if val < 0x1000_0000 {
    buf[0] = 0xE0 | ((val >> 24) as u8);
    buf[1] = ((val >> 16) & 0xFF) as u8;
    buf[2] = ((val >> 8) & 0xFF) as u8;
    buf[3] = (val & 0xFF) as u8;
    4
  } else {
    buf[0] = 0xF8;
    buf[1..9].copy_from_slice(&val.to_be_bytes());
    9
  }
}

/// Encodes a u64 in OPPV format into a fixed-size 9-byte stack buffer.
/// 栈上零堆分配编码 OPPV u64 到定长 9 字节数组，返回写入字节数
#[inline(always)]
pub fn encode_oppv_u64_fixed(val: u64, buf: &mut [u8; 9]) -> usize {
  encode_oppv_u64_slice(val, buf)
}

/// Appends an OPPV encoded u64 to an existing Vec buffer.
/// 编码 u64 到已有 Vec 缓冲区
#[inline]
pub fn encode_oppv_u64(val: u64, buf: &mut Vec<u8>) {
  let mut fixed = [0u8; 9];
  let n = encode_oppv_u64_fixed(val, &mut fixed);
  buf.extend_from_slice(&fixed[..n]);
}

/// Decodes a leading OPPV-encoded u64 from a byte slice, returning (value, bytes_read).
/// 解码字节切片开头的 OPPV u64，返回 (解码数值, 消耗字节数)
#[inline]
pub fn decode_oppv_u64(slice: &[u8]) -> Option<(u64, usize)> {
  let first = *slice.first()?;
  if first < 0x80 {
    Some((first as u64, 1))
  } else if first < 0xC0 {
    if slice.len() < 2 {
      return None;
    }
    let val = (((first & 0x3F) as u64) << 8) | (slice[1] as u64);
    Some((val, 2))
  } else if first < 0xE0 {
    if slice.len() < 3 {
      return None;
    }
    let val = (((first & 0x1F) as u64) << 16) | ((slice[1] as u64) << 8) | (slice[2] as u64);
    Some((val, 3))
  } else if first < 0xF0 {
    if slice.len() < 4 {
      return None;
    }
    let val = (((first & 0x0F) as u64) << 24)
      | ((slice[1] as u64) << 16)
      | ((slice[2] as u64) << 8)
      | (slice[3] as u64);
    Some((val, 4))
  } else if first == 0xF8 {
    if slice.len() < 9 {
      return None;
    }
    let bytes = slice[1..9].try_into().ok()?;
    Some((u64::from_be_bytes(bytes), 9))
  } else {
    None
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_oppv_strict_order_preservation() {
    let test_values = [
      0u64,
      1,
      2,
      127,
      128,
      129,
      1000,
      16383,
      16384,
      100_000,
      1_000_000,
      268_435_455,
      268_435_456,
      1_000_000_000,
      u64::MAX - 1,
      u64::MAX,
    ];

    let mut encoded_list = Vec::new();
    for &v in &test_values {
      let mut buf = Vec::new();
      encode_oppv_u64(v, &mut buf);
      let (decoded, consumed) = decode_oppv_u64(&buf).unwrap();
      assert_eq!(decoded, v);
      assert_eq!(consumed, buf.len());
      encoded_list.push((v, buf));
    }

    // 验证二进制字典序必须与数值大小完全等价
    for w in encoded_list.windows(2) {
      let (v1, ref b1) = w[0];
      let (v2, ref b2) = w[1];
      assert!(
        v1 < v2,
        "Value {v1} should be strictly less than value {v2}"
      );
      assert!(
        b1 < b2,
        "Encoded bytes {b1:?} should be strictly less than bytes {b2:?}"
      );
    }
  }
}
