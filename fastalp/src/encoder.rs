use crate::{
  bitpack::{bitpack_encoded, packed_byte_size},
  constants::{AlpFloat, EXC_COUNT_LEN, HEADER_LEN, MIN_HEADER_LEN, pack_params},
  sampler::{BestParams, find_best_params},
};

/// Single exception value record.
/// 单个异常值记录
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exception<R> {
  pub pos: u16,
  pub bits: R,
}

/// Generic floating-point compression writing directly into `dst` buffer.
/// 通用压缩浮点数组并直接写入 `dst` 缓冲区
pub fn compress_into<F: AlpFloat>(data: &[F], dst: &mut Vec<u8>) {
  let count = data.len().min(u16::MAX as usize) as u16;
  if count == 0 {
    dst.reserve(MIN_HEADER_LEN);
    let count_bytes = 0u16.to_le_bytes();
    let header = [F::TYPE_BYTE, count_bytes[0], count_bytes[1]];
    dst.extend_from_slice(&header);
    return;
  }

  let slice = &data[..count as usize];
  let BestParams { exp, fac } = find_best_params(slice);

  let exp_factor = F::exp_factor(exp, fac);
  let fac_int = F::fac_int(fac);
  let frac_exp = F::frac_exp(exp);

  let mut encoded_ints = Vec::with_capacity(slice.len());
  let mut exceptions = Vec::new();
  let mut min_val = F::MAX_INT;
  let mut max_val = F::MIN_INT;

  for (i, &val) in slice.iter().enumerate() {
    match F::try_encode_fast(val, exp_factor, fac_int, frac_exp) {
      Some(enc) => {
        encoded_ints.push(enc);
        min_val = min_val.min(enc);
        max_val = max_val.max(enc);
      }
      None => {
        encoded_ints.push(F::ZERO_INT);
        exceptions.push(Exception {
          pos: i as u16,
          bits: val.to_raw_bits(),
        });
      }
    }
  }

  let base = if min_val <= max_val {
    min_val
  } else {
    F::ZERO_INT
  };
  let max_offset = if min_val <= max_val {
    F::calc_range(min_val, max_val)
  } else {
    0
  };

  if !exceptions.is_empty() {
    for exc in &exceptions {
      // SAFETY: exc.pos 是在上方遍历 slice (0..slice.len()) 时记录的索引，encoded_ints 的长度与 slice.len() 完全一致，因此 exc.pos as usize 严格小于 encoded_ints.len()，索引安全有效。
      unsafe {
        *encoded_ints.get_unchecked_mut(exc.pos as usize) = base;
      }
    }
  }

  let bit_width = F::bits_needed(max_offset);
  let packed_len = packed_byte_size(slice.len(), bit_width);
  let exc_len = if exceptions.is_empty() {
    0
  } else {
    EXC_COUNT_LEN + exceptions.len() * F::EXC_ENTRY_SIZE
  };
  let total_needed = HEADER_LEN + F::BASE_SIZE + packed_len + exc_len;
  dst.reserve(total_needed);

  // 1. Header (5B): 1B 类型 + 2B 数量 + 2B 参数 (exp, fac, bit_width)
  let count_bytes = count.to_le_bytes();
  let params_bytes = pack_params(exp, fac, bit_width).to_le_bytes();
  let header = [
    F::TYPE_BYTE,
    count_bytes[0],
    count_bytes[1],
    params_bytes[0],
    params_bytes[1],
  ];
  dst.extend_from_slice(&header);

  // 2. Base
  F::write_base(base, dst);

  // 3. Bitpacked data
  bitpack_encoded::<F>(&encoded_ints, base, bit_width, dst);

  // 4. Exceptions (仅在存在异常值时写入)
  if !exceptions.is_empty() {
    let exc_count = exceptions.len() as u16;
    dst.extend_from_slice(&exc_count.to_le_bytes());
    for exc in exceptions {
      F::write_exception(exc.pos, exc.bits, dst);
    }
  }
}

/// Generic floating-point slice compression.
/// 通用压缩浮点数切片
#[inline]
pub fn compress<F: AlpFloat>(data: &[F]) -> Vec<u8> {
  let mut dst = Vec::new();
  compress_into(data, &mut dst);
  dst
}
