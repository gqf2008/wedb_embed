use crate::{
  constants::{
    AlpFloat, BITS_PER_BYTE, BITS_U64, BYTES_U16, BYTES_U32, BYTES_U64, LUT_SIZE_1BIT,
    LUT_SIZE_2BIT, LUT_SIZE_4BIT, LUT_SIZE_8BIT, bit_mask,
  },
  error::{Error, Result},
};

const MASK_1BIT: u8 = 0x01;
const MASK_2BIT: u8 = 0x03;
const MASK_4BIT: u8 = 0x0f;

const BITS_1: u8 = 1;
const BITS_2: u8 = 2;
const BITS_4: u8 = 4;
const BITS_8: u8 = 8;
const BITS_16: u8 = 16;
const BITS_32: u8 = 32;
const BITS_64: u8 = 64;

const CHUNK_8: usize = 8;
const CHUNK_4: usize = 4;
const CHUNK_2: usize = 2;

/// Calculates total bytes required to pack N W-bit integers.
/// 计算 N 个 W-bit 整数打包所需的总字节数
#[inline(always)]
pub const fn packed_byte_size(count: usize, bit_width: u8) -> usize {
  (count * (bit_width as usize)).div_ceil(BITS_PER_BYTE)
}

/// Fast bit packing: packs `values` into `dst`.
/// 高速位打包：将 `values` 打包入 `dst`
pub fn bitpack_u64(values: &[u64], bit_width: u8, dst: &mut Vec<u8>) {
  if values.is_empty() || bit_width == 0 {
    return;
  }

  let total_bytes = packed_byte_size(values.len(), bit_width);
  let old_len = dst.len();
  dst.reserve(total_bytes);

  if bit_width == BITS_1 {
    let (chunks, rem) = values.as_chunks::<CHUNK_8>();
    // SAFETY: dst 已 reserve(total_bytes)，按 8 个整数一组打包写入 chunks.len() 字节，余数最多写入 1 字节，刚好填满 total_bytes。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for chunk in chunks {
        let o0 = (chunk[0] as u8) & MASK_1BIT;
        let o1 = (chunk[1] as u8) & MASK_1BIT;
        let o2 = (chunk[2] as u8) & MASK_1BIT;
        let o3 = (chunk[3] as u8) & MASK_1BIT;
        let o4 = (chunk[4] as u8) & MASK_1BIT;
        let o5 = (chunk[5] as u8) & MASK_1BIT;
        let o6 = (chunk[6] as u8) & MASK_1BIT;
        let o7 = (chunk[7] as u8) & MASK_1BIT;
        *dst_ptr =
          o0 | (o1 << 1) | (o2 << 2) | (o3 << 3) | (o4 << 4) | (o5 << 5) | (o6 << 6) | (o7 << 7);
        dst_ptr = dst_ptr.add(1);
      }
      if !rem.is_empty() {
        let mut b = 0u8;
        for (i, &val) in rem.iter().enumerate() {
          let o = (val as u8) & MASK_1BIT;
          b |= o << i;
        }
        *dst_ptr = b;
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  } else if bit_width == BITS_2 {
    let (chunks, rem) = values.as_chunks::<CHUNK_4>();
    // SAFETY: dst 已 reserve(total_bytes)，按 4 个整数一组打包写入 chunks.len() 字节，余数最多写入 1 字节，刚好填满 total_bytes。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for chunk in chunks {
        let o0 = (chunk[0] as u8) & MASK_2BIT;
        let o1 = (chunk[1] as u8) & MASK_2BIT;
        let o2 = (chunk[2] as u8) & MASK_2BIT;
        let o3 = (chunk[3] as u8) & MASK_2BIT;
        *dst_ptr = o0 | (o1 << 2) | (o2 << 4) | (o3 << 6);
        dst_ptr = dst_ptr.add(1);
      }
      if !rem.is_empty() {
        let mut b = 0u8;
        for (i, &val) in rem.iter().enumerate() {
          let o = (val as u8) & MASK_2BIT;
          b |= o << (i * 2);
        }
        *dst_ptr = b;
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  } else if bit_width == BITS_4 {
    let (chunks, rem) = values.as_chunks::<CHUNK_2>();
    // SAFETY: dst 已 reserve(total_bytes)，按 2 个整数一组打包写入 chunks.len() 字节，余数最多写入 1 字节，刚好填满 total_bytes。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for chunk in chunks {
        let o0 = (chunk[0] as u8) & MASK_4BIT;
        let o1 = (chunk[1] as u8) & MASK_4BIT;
        *dst_ptr = o0 | (o1 << 4);
        dst_ptr = dst_ptr.add(1);
      }
      if let Some(&last) = rem.first() {
        let o0 = (last as u8) & MASK_4BIT;
        *dst_ptr = o0;
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  } else if bit_width == BITS_8 {
    // SAFETY: dst 已 reserve(total_bytes)，且循环严格写入 values.len() 个字节，写入完成后调用 set_len 确保内存全部初始化完毕。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for &v in values {
        *dst_ptr = v as u8;
        dst_ptr = dst_ptr.add(1);
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  } else if bit_width == BITS_16 {
    // SAFETY: dst 已 reserve(total_bytes)，逐元素写入 2-byte 小端序列后安全更新长度。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for &v in values {
        dst_ptr.cast::<u16>().write_unaligned((v as u16).to_le());
        dst_ptr = dst_ptr.add(BYTES_U16);
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  } else if bit_width == BITS_32 {
    // SAFETY: dst 已 reserve(total_bytes)，逐元素写入 4-byte 小端序列后安全更新长度。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for &v in values {
        dst_ptr.cast::<u32>().write_unaligned((v as u32).to_le());
        dst_ptr = dst_ptr.add(BYTES_U32);
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  } else if bit_width == BITS_64 {
    // SAFETY: dst 已 reserve(total_bytes)，逐元素写入 8-byte 小端序列后安全更新长度。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for &v in values {
        dst_ptr.cast::<u64>().write_unaligned(v.to_le());
        dst_ptr = dst_ptr.add(BYTES_U64);
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  }

  let mask = bit_mask(bit_width);
  let mut acc: u128 = 0;
  let mut bits: u32 = 0;

  // SAFETY: dst 已预先 reserve(total_bytes)，通过指针直接写入累加器中的完整 64 位或剩余字节，完全覆盖 total_bytes。
  unsafe {
    let mut dst_ptr = dst.as_mut_ptr().add(old_len);
    for &val in values {
      acc |= ((val & mask) as u128) << bits;
      bits += bit_width as u32;
      if bits >= BITS_U64 as u32 {
        dst_ptr.cast::<u64>().write_unaligned((acc as u64).to_le());
        dst_ptr = dst_ptr.add(BYTES_U64);
        acc >>= BITS_U64;
        bits -= BITS_U64 as u32;
      }
    }

    while bits > 0 {
      *dst_ptr = acc as u8;
      dst_ptr = dst_ptr.add(1);
      acc >>= BITS_PER_BYTE;
      bits = bits.saturating_sub(BITS_PER_BYTE as u32);
    }

    dst.set_len(old_len + total_bytes);
  }
}

/// Generic fast bit packing of encoded floating-point frame-of-reference deltas into `dst`.
/// 通用高速位打包已编码的浮点差值整数并直接写入 `dst`
pub fn bitpack_encoded<F: AlpFloat>(
  encoded_ints: &[F::Int],
  base: F::Int,
  bit_width: u8,
  dst: &mut Vec<u8>,
) {
  if encoded_ints.is_empty() || bit_width == 0 {
    return;
  }

  let total_bytes = packed_byte_size(encoded_ints.len(), bit_width);
  let old_len = dst.len();
  dst.reserve(total_bytes);

  if bit_width == BITS_1 {
    let (chunks, rem) = encoded_ints.as_chunks::<CHUNK_8>();
    // SAFETY: dst 已 reserve(total_bytes)，按 8 个整数一组打包写入 chunks.len() 字节，余数最多写入 1 字节，刚好填满 total_bytes，无越界与未初始化。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for chunk in chunks {
        let o0 = (F::int_diff_to_u64(chunk[0], base) as u8) & MASK_1BIT;
        let o1 = (F::int_diff_to_u64(chunk[1], base) as u8) & MASK_1BIT;
        let o2 = (F::int_diff_to_u64(chunk[2], base) as u8) & MASK_1BIT;
        let o3 = (F::int_diff_to_u64(chunk[3], base) as u8) & MASK_1BIT;
        let o4 = (F::int_diff_to_u64(chunk[4], base) as u8) & MASK_1BIT;
        let o5 = (F::int_diff_to_u64(chunk[5], base) as u8) & MASK_1BIT;
        let o6 = (F::int_diff_to_u64(chunk[6], base) as u8) & MASK_1BIT;
        let o7 = (F::int_diff_to_u64(chunk[7], base) as u8) & MASK_1BIT;
        *dst_ptr =
          o0 | (o1 << 1) | (o2 << 2) | (o3 << 3) | (o4 << 4) | (o5 << 5) | (o6 << 6) | (o7 << 7);
        dst_ptr = dst_ptr.add(1);
      }
      if !rem.is_empty() {
        let mut b = 0u8;
        for (i, &val) in rem.iter().enumerate() {
          let o = (F::int_diff_to_u64(val, base) as u8) & MASK_1BIT;
          b |= o << i;
        }
        *dst_ptr = b;
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  } else if bit_width == BITS_2 {
    let (chunks, rem) = encoded_ints.as_chunks::<CHUNK_4>();
    // SAFETY: dst 已 reserve(total_bytes)，按 4 个整数一组打包写入 chunks.len() 字节，余数最多写入 1 字节，刚好填满 total_bytes。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for chunk in chunks {
        let o0 = (F::int_diff_to_u64(chunk[0], base) as u8) & MASK_2BIT;
        let o1 = (F::int_diff_to_u64(chunk[1], base) as u8) & MASK_2BIT;
        let o2 = (F::int_diff_to_u64(chunk[2], base) as u8) & MASK_2BIT;
        let o3 = (F::int_diff_to_u64(chunk[3], base) as u8) & MASK_2BIT;
        *dst_ptr = o0 | (o1 << 2) | (o2 << 4) | (o3 << 6);
        dst_ptr = dst_ptr.add(1);
      }
      if !rem.is_empty() {
        let mut b = 0u8;
        for (i, &val) in rem.iter().enumerate() {
          let o = (F::int_diff_to_u64(val, base) as u8) & MASK_2BIT;
          b |= o << (i * 2);
        }
        *dst_ptr = b;
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  } else if bit_width == BITS_4 {
    let (chunks, rem) = encoded_ints.as_chunks::<CHUNK_2>();
    // SAFETY: dst 已 reserve(total_bytes)，按 2 个整数一组打包写入 chunks.len() 字节，余数最多写入 1 字节，刚好填满 total_bytes。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for chunk in chunks {
        let o0 = (F::int_diff_to_u64(chunk[0], base) as u8) & MASK_4BIT;
        let o1 = (F::int_diff_to_u64(chunk[1], base) as u8) & MASK_4BIT;
        *dst_ptr = o0 | (o1 << 4);
        dst_ptr = dst_ptr.add(1);
      }
      if let Some(&last) = rem.first() {
        let o0 = (F::int_diff_to_u64(last, base) as u8) & MASK_4BIT;
        *dst_ptr = o0;
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  } else if bit_width == BITS_8 {
    // SAFETY: dst 已 reserve(encoded_ints.len())，逐元素写入 u8，完全覆盖 total_bytes。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for &v in encoded_ints {
        *dst_ptr = F::int_diff_to_u64(v, base) as u8;
        dst_ptr = dst_ptr.add(1);
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  } else if bit_width == BITS_16 {
    // SAFETY: dst 已 reserve(total_bytes)，逐元素写入 2-byte 小端序列，完全覆盖 total_bytes。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for &v in encoded_ints {
        dst_ptr
          .cast::<u16>()
          .write_unaligned((F::int_diff_to_u64(v, base) as u16).to_le());
        dst_ptr = dst_ptr.add(BYTES_U16);
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  } else if bit_width == BITS_32 {
    // SAFETY: dst 已 reserve(total_bytes)，逐元素写入 4-byte 小端序列，完全覆盖 total_bytes。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for &v in encoded_ints {
        dst_ptr
          .cast::<u32>()
          .write_unaligned((F::int_diff_to_u64(v, base) as u32).to_le());
        dst_ptr = dst_ptr.add(BYTES_U32);
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  } else if bit_width == BITS_64 {
    // SAFETY: dst 已 reserve(total_bytes)，逐元素写入 8-byte 小端序列，完全覆盖 total_bytes。
    unsafe {
      let mut dst_ptr = dst.as_mut_ptr().add(old_len);
      for &v in encoded_ints {
        dst_ptr
          .cast::<u64>()
          .write_unaligned(F::int_diff_to_u64(v, base).to_le());
        dst_ptr = dst_ptr.add(BYTES_U64);
      }
      dst.set_len(old_len + total_bytes);
    }
    return;
  }

  let mask = bit_mask(bit_width);
  let mut acc: u128 = 0;
  let mut bits: u32 = 0;

  // SAFETY: dst 已预先 reserve(total_bytes)，通过指针直接写入累加器中的完整 64 位或剩余字节，完全覆盖 total_bytes。
  unsafe {
    let mut dst_ptr = dst.as_mut_ptr().add(old_len);
    for &val in encoded_ints {
      let offset = F::int_diff_to_u64(val, base) & mask;
      acc |= (offset as u128) << bits;
      bits += bit_width as u32;
      if bits >= BITS_U64 as u32 {
        dst_ptr.cast::<u64>().write_unaligned((acc as u64).to_le());
        dst_ptr = dst_ptr.add(BYTES_U64);
        acc >>= BITS_U64;
        bits -= BITS_U64 as u32;
      }
    }

    while bits > 0 {
      *dst_ptr = acc as u8;
      dst_ptr = dst_ptr.add(1);
      acc >>= BITS_PER_BYTE;
      bits = bits.saturating_sub(BITS_PER_BYTE as u32);
    }

    dst.set_len(old_len + total_bytes);
  }
}

/// Fast bit unpacking: unpacks `count` integers of `bit_width` from `src` into `dst`.
/// 高速位解包：从 `src` 解包出 `count` 个 `bit_width` 位的整数至 `dst`
pub fn bitunpack_u64(src: &[u8], count: usize, bit_width: u8, dst: &mut Vec<u64>) -> Result<()> {
  if count == 0 {
    return Ok(());
  }
  if bit_width == 0 {
    dst.resize(dst.len() + count, 0);
    return Ok(());
  }

  let required_bytes = packed_byte_size(count, bit_width);
  if src.len() < required_bytes {
    return Err(Error::UnexpectedEof {
      needed: required_bytes,
      available: src.len(),
    });
  }

  let old_len = dst.len();
  dst.reserve(count);

  // SAFETY:
  // 1. 上方已校验 src.len() >= required_bytes，保证读指针与 read_unaligned 严格在 src 有效内存边界内；
  // 2. dst 已预分配 dst.reserve(count)，写入 old_len..old_len+count 空间完全充足且无越界风险；
  // 3. 循环严格写入并初始化 count 个元素后，调用 dst.set_len(old_len + count) 安全更新长度。
  unsafe {
    let mut dst_ptr = dst.as_mut_ptr().add(old_len);

    if bit_width == BITS_1 {
      let full_bytes = count / CHUNK_8;
      for &b in &src[..full_bytes] {
        *dst_ptr.add(0) = (b & MASK_1BIT) as u64;
        *dst_ptr.add(1) = ((b >> 1) & MASK_1BIT) as u64;
        *dst_ptr.add(2) = ((b >> 2) & MASK_1BIT) as u64;
        *dst_ptr.add(3) = ((b >> 3) & MASK_1BIT) as u64;
        *dst_ptr.add(4) = ((b >> 4) & MASK_1BIT) as u64;
        *dst_ptr.add(5) = ((b >> 5) & MASK_1BIT) as u64;
        *dst_ptr.add(6) = ((b >> 6) & MASK_1BIT) as u64;
        *dst_ptr.add(7) = ((b >> 7) & MASK_1BIT) as u64;
        dst_ptr = dst_ptr.add(CHUNK_8);
      }
      let rem = count % CHUNK_8;
      if rem > 0 {
        let b = *src.get_unchecked(full_bytes);
        for shift in 0..rem {
          *dst_ptr = ((b >> shift) & MASK_1BIT) as u64;
          dst_ptr = dst_ptr.add(1);
        }
      }
      dst.set_len(old_len + count);
      return Ok(());
    } else if bit_width == BITS_2 {
      let full_bytes = count / CHUNK_4;
      for &b in &src[..full_bytes] {
        *dst_ptr.add(0) = (b & MASK_2BIT) as u64;
        *dst_ptr.add(1) = ((b >> 2) & MASK_2BIT) as u64;
        *dst_ptr.add(2) = ((b >> 4) & MASK_2BIT) as u64;
        *dst_ptr.add(3) = ((b >> 6) & MASK_2BIT) as u64;
        dst_ptr = dst_ptr.add(CHUNK_4);
      }
      let rem = count % CHUNK_4;
      if rem > 0 {
        let b = *src.get_unchecked(full_bytes);
        for i in 0..rem {
          *dst_ptr = ((b >> (i * 2)) & MASK_2BIT) as u64;
          dst_ptr = dst_ptr.add(1);
        }
      }
      dst.set_len(old_len + count);
      return Ok(());
    } else if bit_width == BITS_4 {
      let full_bytes = count / CHUNK_2;
      let (byte_chunks, byte_rem) = src[..full_bytes].as_chunks::<CHUNK_2>();
      for chunk in byte_chunks {
        let b0 = chunk[0];
        let b1 = chunk[1];
        *dst_ptr.add(0) = (b0 & MASK_4BIT) as u64;
        *dst_ptr.add(1) = (b0 >> 4) as u64;
        *dst_ptr.add(2) = (b1 & MASK_4BIT) as u64;
        *dst_ptr.add(3) = (b1 >> 4) as u64;
        dst_ptr = dst_ptr.add(CHUNK_4);
      }
      for &b in byte_rem {
        *dst_ptr.add(0) = (b & MASK_4BIT) as u64;
        *dst_ptr.add(1) = (b >> 4) as u64;
        dst_ptr = dst_ptr.add(CHUNK_2);
      }
      if !count.is_multiple_of(CHUNK_2) {
        let b = *src.get_unchecked(full_bytes);
        *dst_ptr = (b & MASK_4BIT) as u64;
      }
      dst.set_len(old_len + count);
      return Ok(());
    } else if bit_width == BITS_8 {
      for (i, &b) in src[..count].iter().enumerate() {
        *dst_ptr.add(i) = b as u64;
      }
      dst.set_len(old_len + count);
      return Ok(());
    } else if bit_width == BITS_16 {
      let src_ptr = src.as_ptr().cast::<u16>();
      for i in 0..count {
        *dst_ptr.add(i) = u16::from_le(src_ptr.add(i).read_unaligned()) as u64;
      }
      dst.set_len(old_len + count);
      return Ok(());
    } else if bit_width == BITS_32 {
      let src_ptr = src.as_ptr().cast::<u32>();
      for i in 0..count {
        *dst_ptr.add(i) = u32::from_le(src_ptr.add(i).read_unaligned()) as u64;
      }
      dst.set_len(old_len + count);
      return Ok(());
    } else if bit_width == BITS_64 {
      let src_ptr = src.as_ptr().cast::<u64>();
      for i in 0..count {
        *dst_ptr.add(i) = u64::from_le(src_ptr.add(i).read_unaligned());
      }
      dst.set_len(old_len + count);
      return Ok(());
    }

    let mask = bit_mask(bit_width);
    let mut acc: u128 = 0;
    let mut bits_in_acc: u32 = 0;
    let mut src_ptr = src.as_ptr();
    let src_end = src.as_ptr().add(src.len());

    let mut i = 0;
    while i < count && src_ptr.add(BYTES_U64) <= src_end {
      if bits_in_acc < bit_width as u32 {
        let chunk = u64::from_le(src_ptr.cast::<u64>().read_unaligned());
        acc |= (chunk as u128) << bits_in_acc;
        bits_in_acc += BITS_U64 as u32;
        src_ptr = src_ptr.add(BYTES_U64);
      }
      let val = (acc as u64) & mask;
      acc >>= bit_width;
      bits_in_acc -= bit_width as u32;
      *dst_ptr = val;
      dst_ptr = dst_ptr.add(1);
      i += 1;
    }

    while i < count {
      while bits_in_acc < bit_width as u32 && src_ptr < src_end {
        acc |= (*src_ptr as u128) << bits_in_acc;
        bits_in_acc += BITS_PER_BYTE as u32;
        src_ptr = src_ptr.add(1);
      }
      let val = (acc as u64) & mask;
      acc >>= bit_width;
      bits_in_acc = bits_in_acc.saturating_sub(bit_width as u32);
      *dst_ptr = val;
      dst_ptr = dst_ptr.add(1);
      i += 1;
    }

    dst.set_len(old_len + count);
  }

  Ok(())
}

/// Generic zero-copy direct bit unpacking and floating-point reconstruction into `dst`.
/// 通用零拷贝直接解包并重构浮点数据至 `dst`
#[inline(always)]
pub fn bitunpack_into<F: AlpFloat>(
  src: &[u8],
  count: usize,
  bit_width: u8,
  base: F::Int,
  fac_int: i64,
  frac_flt: F,
  dst: &mut Vec<F>,
) -> Result<()> {
  if count == 0 {
    return Ok(());
  }

  let required_bytes = packed_byte_size(count, bit_width);
  if src.len() < required_bytes {
    return Err(Error::UnexpectedEof {
      needed: required_bytes,
      available: src.len(),
    });
  }

  if bit_width == 0 {
    let val = F::decode_from_offset(0, base, fac_int, frac_flt);
    dst.resize(dst.len() + count, val);
    return Ok(());
  }

  let old_len = dst.len();
  dst.reserve(count);

  // SAFETY:
  // 1. 上方已校验 src.len() >= required_bytes，保证读指针与各 bit_width 分支的 read_unaligned / 查表访问严格在合法内存范围内；
  // 2. dst 已预分配 dst.reserve(count)，写入 old_len..old_len+count 空间完全充足且无越界风险；
  // 3. 循环严格解码并初始化 count 个浮点元素后，调用 dst.set_len(old_len + count) 安全更新长度。
  unsafe {
    let mut dst_ptr = dst.as_mut_ptr().add(old_len);

    if bit_width == BITS_1 {
      let lut = F::build_lut::<LUT_SIZE_1BIT>(base, fac_int, frac_flt);
      let full_bytes = count / CHUNK_8;
      for &b in &src[..full_bytes] {
        *dst_ptr.add(0) = *lut.get_unchecked((b & MASK_1BIT) as usize);
        *dst_ptr.add(1) = *lut.get_unchecked(((b >> 1) & MASK_1BIT) as usize);
        *dst_ptr.add(2) = *lut.get_unchecked(((b >> 2) & MASK_1BIT) as usize);
        *dst_ptr.add(3) = *lut.get_unchecked(((b >> 3) & MASK_1BIT) as usize);
        *dst_ptr.add(4) = *lut.get_unchecked(((b >> 4) & MASK_1BIT) as usize);
        *dst_ptr.add(5) = *lut.get_unchecked(((b >> 5) & MASK_1BIT) as usize);
        *dst_ptr.add(6) = *lut.get_unchecked(((b >> 6) & MASK_1BIT) as usize);
        *dst_ptr.add(7) = *lut.get_unchecked(((b >> 7) & MASK_1BIT) as usize);
        dst_ptr = dst_ptr.add(CHUNK_8);
      }
      let rem = count % CHUNK_8;
      if rem > 0 {
        let b = *src.get_unchecked(full_bytes);
        for shift in 0..rem {
          let idx = ((b >> shift) & MASK_1BIT) as usize;
          *dst_ptr = *lut.get_unchecked(idx);
          dst_ptr = dst_ptr.add(1);
        }
      }
      dst.set_len(old_len + count);
      return Ok(());
    } else if bit_width == BITS_2 {
      let lut = F::build_lut::<LUT_SIZE_2BIT>(base, fac_int, frac_flt);
      let full_bytes = count / CHUNK_4;
      for &b in &src[..full_bytes] {
        *dst_ptr.add(0) = *lut.get_unchecked((b & MASK_2BIT) as usize);
        *dst_ptr.add(1) = *lut.get_unchecked(((b >> 2) & MASK_2BIT) as usize);
        *dst_ptr.add(2) = *lut.get_unchecked(((b >> 4) & MASK_2BIT) as usize);
        *dst_ptr.add(3) = *lut.get_unchecked(((b >> 6) & MASK_2BIT) as usize);
        dst_ptr = dst_ptr.add(CHUNK_4);
      }
      let rem = count % CHUNK_4;
      if rem > 0 {
        let b = *src.get_unchecked(full_bytes);
        for i in 0..rem {
          let idx = ((b >> (i * 2)) & MASK_2BIT) as usize;
          *dst_ptr = *lut.get_unchecked(idx);
          dst_ptr = dst_ptr.add(1);
        }
      }
      dst.set_len(old_len + count);
      return Ok(());
    } else if bit_width == BITS_4 {
      let lut = F::build_lut::<LUT_SIZE_4BIT>(base, fac_int, frac_flt);
      let full_bytes = count / CHUNK_2;
      let (byte_chunks, byte_rem) = src[..full_bytes].as_chunks::<CHUNK_2>();
      for chunk in byte_chunks {
        let b0 = chunk[0];
        let b1 = chunk[1];
        *dst_ptr.add(0) = *lut.get_unchecked((b0 & MASK_4BIT) as usize);
        *dst_ptr.add(1) = *lut.get_unchecked((b0 >> 4) as usize);
        *dst_ptr.add(2) = *lut.get_unchecked((b1 & MASK_4BIT) as usize);
        *dst_ptr.add(3) = *lut.get_unchecked((b1 >> 4) as usize);
        dst_ptr = dst_ptr.add(CHUNK_4);
      }
      for &b in byte_rem {
        *dst_ptr.add(0) = *lut.get_unchecked((b & MASK_4BIT) as usize);
        *dst_ptr.add(1) = *lut.get_unchecked((b >> 4) as usize);
        dst_ptr = dst_ptr.add(CHUNK_2);
      }
      if !count.is_multiple_of(CHUNK_2) {
        let b = *src.get_unchecked(full_bytes);
        *dst_ptr = *lut.get_unchecked((b & MASK_4BIT) as usize);
      }
      dst.set_len(old_len + count);
      return Ok(());
    } else if bit_width == BITS_8 {
      let lut = F::build_lut::<LUT_SIZE_8BIT>(base, fac_int, frac_flt);
      let (chunks, rem) = src[..count].as_chunks::<CHUNK_8>();
      for chunk in chunks {
        *dst_ptr.add(0) = *lut.get_unchecked(chunk[0] as usize);
        *dst_ptr.add(1) = *lut.get_unchecked(chunk[1] as usize);
        *dst_ptr.add(2) = *lut.get_unchecked(chunk[2] as usize);
        *dst_ptr.add(3) = *lut.get_unchecked(chunk[3] as usize);
        *dst_ptr.add(4) = *lut.get_unchecked(chunk[4] as usize);
        *dst_ptr.add(5) = *lut.get_unchecked(chunk[5] as usize);
        *dst_ptr.add(6) = *lut.get_unchecked(chunk[6] as usize);
        *dst_ptr.add(7) = *lut.get_unchecked(chunk[7] as usize);
        dst_ptr = dst_ptr.add(CHUNK_8);
      }
      for &b in rem {
        *dst_ptr = *lut.get_unchecked(b as usize);
        dst_ptr = dst_ptr.add(1);
      }
      dst.set_len(old_len + count);
      return Ok(());
    } else if bit_width == BITS_16 {
      let src_ptr = src.as_ptr().cast::<u16>();
      for i in 0..count {
        let off = u16::from_le(src_ptr.add(i).read_unaligned()) as u64;
        *dst_ptr.add(i) = F::decode_from_offset(off, base, fac_int, frac_flt);
      }
      dst.set_len(old_len + count);
      return Ok(());
    } else if bit_width == BITS_32 {
      let src_ptr = src.as_ptr().cast::<u32>();
      for i in 0..count {
        let off = u32::from_le(src_ptr.add(i).read_unaligned()) as u64;
        *dst_ptr.add(i) = F::decode_from_offset(off, base, fac_int, frac_flt);
      }
      dst.set_len(old_len + count);
      return Ok(());
    } else if bit_width == BITS_64 {
      let src_ptr = src.as_ptr().cast::<u64>();
      for i in 0..count {
        let off = u64::from_le(src_ptr.add(i).read_unaligned());
        *dst_ptr.add(i) = F::decode_from_offset(off, base, fac_int, frac_flt);
      }
      dst.set_len(old_len + count);
      return Ok(());
    }

    let mask = bit_mask(bit_width);
    let mut acc: u128 = 0;
    let mut bits_in_acc: u32 = 0;
    let mut src_ptr = src.as_ptr();
    let src_end = src.as_ptr().add(src.len());

    let mut i = 0;
    while i < count && src_ptr.add(BYTES_U64) <= src_end {
      if bits_in_acc < bit_width as u32 {
        let chunk = u64::from_le(src_ptr.cast::<u64>().read_unaligned());
        acc |= (chunk as u128) << bits_in_acc;
        bits_in_acc += BITS_U64 as u32;
        src_ptr = src_ptr.add(BYTES_U64);
      }
      let off = (acc as u64) & mask;
      acc >>= bit_width;
      bits_in_acc -= bit_width as u32;
      *dst_ptr = F::decode_from_offset(off, base, fac_int, frac_flt);
      dst_ptr = dst_ptr.add(1);
      i += 1;
    }

    while i < count {
      while bits_in_acc < bit_width as u32 && src_ptr < src_end {
        acc |= (*src_ptr as u128) << bits_in_acc;
        bits_in_acc += BITS_PER_BYTE as u32;
        src_ptr = src_ptr.add(1);
      }
      let off = (acc as u64) & mask;
      acc >>= bit_width;
      bits_in_acc = bits_in_acc.saturating_sub(bit_width as u32);
      *dst_ptr = F::decode_from_offset(off, base, fac_int, frac_flt);
      dst_ptr = dst_ptr.add(1);
      i += 1;
    }

    dst.set_len(old_len + count);
  }

  Ok(())
}
