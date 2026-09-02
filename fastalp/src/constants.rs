use std::{mem::size_of, ptr::read_unaligned};

pub const BITS_PER_BYTE: usize = 8;
pub const BITS_U64: usize = 64;

pub const BYTES_U16: usize = size_of::<u16>();
pub const BYTES_U32: usize = size_of::<u32>();
pub const BYTES_U64: usize = size_of::<u64>();

pub const TYPE_F64: u8 = 1;
pub const TYPE_F32: u8 = 2;

pub const MAX_EXPONENT_F64: u8 = 18;
pub const MAX_EXPONENT_F32: u8 = 10;

pub const MAX_FAC_F64: u8 = 8;
pub const MAX_FAC_F32: u8 = 4;

pub const MAGIC_NUMBER_F64: f64 = 6755399441055744.0; // 1.5 * 2^52
pub const MAGIC_NUMBER_F32: f32 = 12582912.0; // 1.5 * 2^23

pub const ENCODING_UPPER_LIMIT_F64: f64 = 9223372036854774784.0;
pub const ENCODING_UPPER_LIMIT_F32: f32 = 2147483520.0;

/// Minimum header length (bytes): only used for empty sequences with count == 0 (1B type + 2B count).
/// 最小头部长度 (字节): 仅空序列 count == 0 时使用 (1B 类型 + 2B 数量)
pub const MIN_HEADER_LEN: usize = 3;

/// Full header length (bytes): 1B type + 2B count + 2B packed parameters (exp:5b, fac:4b, bit_width:7b).
/// 完整头部长度 (字节): 1B 类型 + 2B 数量 + 2B 打包参数 (exp:5b, fac:4b, bit_width:7b)
pub const HEADER_LEN: usize = 5;

/// Header field byte offset constants.
/// 头部各字段字节偏移常量
pub const HDR_TYPE_IDX: usize = 0;
pub const HDR_COUNT_START: usize = 1;
pub const HDR_COUNT_END: usize = 3;
pub const HDR_PARAMS_START: usize = 3;
pub const HDR_PARAMS_END: usize = 5;

/// Parameter bitfield mask and shift constants.
/// 参数位域掩码与位移常量
pub const EXP_MASK: u16 = 0x001F;
pub const FAC_SHIFT: u16 = 5;
pub const FAC_MASK: u16 = 0x000F;
pub const BIT_WIDTH_SHIFT: u16 = 9;
pub const BIT_WIDTH_MASK: u16 = 0x007F;

/// Packs exp (5b), fac (4b), and bit_width (7b) into 2-byte u16.
/// 将 exp (5b), fac (4b), bit_width (7b) 打包进 2 字节 u16
#[inline(always)]
pub const fn pack_params(exp: u8, fac: u8, bit_width: u8) -> u16 {
  ((exp as u16) & EXP_MASK)
    | (((fac as u16) & FAC_MASK) << FAC_SHIFT)
    | (((bit_width as u16) & BIT_WIDTH_MASK) << BIT_WIDTH_SHIFT)
}

/// Unpacks (exp, fac, bit_width) from 2-byte u16.
/// 从 2 字节 u16 解包 (exp, fac, bit_width)
#[inline(always)]
pub const fn unpack_params(params: u16) -> (u8, u8, u8) {
  let exp = (params & EXP_MASK) as u8;
  let fac = ((params >> FAC_SHIFT) & FAC_MASK) as u8;
  let bit_width = ((params >> BIT_WIDTH_SHIFT) & BIT_WIDTH_MASK) as u8;
  (exp, fac, bit_width)
}

/// Exception count field length (u16).
/// 异常总数字段长度 (u16)
pub const EXC_COUNT_LEN: usize = size_of::<u16>();

/// Exception position index field length (u16).
/// 异常位置索引字段长度 (u16)
pub const EXC_POS_LEN: usize = size_of::<u16>();

/// Sampling and search constants.
/// 采样与搜索常量
pub const SAMPLES_COUNT: usize = 32;
pub const EARLY_EXIT_BIT_WIDTH: usize = 2;

/// Decompression local lookup table size constant.
/// 解压局部查找表大小常量
pub const LUT_SIZE_1BIT: usize = 2;
pub const LUT_SIZE_2BIT: usize = 4;
pub const LUT_SIZE_4BIT: usize = 16;
pub const LUT_SIZE_8BIT: usize = 256;

/// f64 static positive power table 10^0 .. 10^18.
/// f64 静态正幂表 10^0 .. 10^18
pub const EXP_ARR_F64: [f64; 19] = [
  1.0,
  10.0,
  100.0,
  1_000.0,
  10_000.0,
  100_000.0,
  1_000_000.0,
  10_000_000.0,
  100_000_000.0,
  1_000_000_000.0,
  10_000_000_000.0,
  100_000_000_000.0,
  1_000_000_000_000.0,
  10_000_000_000_000.0,
  100_000_000_000_000.0,
  1_000_000_000_000_000.0,
  10_000_000_000_000_000.0,
  100_000_000_000_000_000.0,
  1_000_000_000_000_000_000.0,
];

/// f64 static negative power table 10^-0 .. 10^-18.
/// f64 静态负幂表 10^-0 .. 10^-18
pub const FRAC_ARR_F64: [f64; 19] = [
  1.0,
  0.1,
  0.01,
  0.001,
  0.0001,
  0.00001,
  0.000001,
  0.0000001,
  0.00000001,
  0.000000001,
  0.0000000001,
  0.00000000001,
  0.000000000001,
  0.0000000000001,
  0.00000000000001,
  0.000000000000001,
  0.0000000000000001,
  0.00000000000000001,
  0.000000000000000001,
];

/// f64 static integer factor table 10^0 .. 10^18.
/// f64 静态整型因子表 10^0 .. 10^18
pub const FACT_ARR_F64: [i64; 19] = [
  1,
  10,
  100,
  1_000,
  10_000,
  100_000,
  1_000_000,
  10_000_000,
  100_000_000,
  1_000_000_000,
  10_000_000_000,
  100_000_000_000,
  1_000_000_000_000,
  10_000_000_000_000,
  100_000_000_000_000,
  1_000_000_000_000_000,
  10_000_000_000_000_000,
  100_000_000_000_000_000,
  1_000_000_000_000_000_000,
];

/// f32 static positive power table 10^0 .. 10^10.
/// f32 静态正幂表 10^0 .. 10^10
pub const EXP_ARR_F32: [f32; 11] = [
  1.0,
  10.0,
  100.0,
  1_000.0,
  10_000.0,
  100_000.0,
  1_000_000.0,
  10_000_000.0,
  100_000_000.0,
  1_000_000_000.0,
  10_000_000_000.0,
];

/// f32 static negative power table 10^-0 .. 10^-10.
/// f32 静态负幂表 10^-0 .. 10^-10
pub const FRAC_ARR_F32: [f32; 11] = [
  1.0,
  0.1,
  0.01,
  0.001,
  0.0001,
  0.00001,
  0.000001,
  0.0000001,
  0.00000001,
  0.000000001,
  0.0000000001,
];

/// f32 static integer factor table 10^0 .. 10^10.
/// f32 静态整型因子表 10^0 .. 10^10
pub const FACT_ARR_F32: [i64; 11] = [
  1,
  10,
  100,
  1_000,
  10_000,
  100_000,
  1_000_000,
  10_000_000,
  100_000_000,
  1_000_000_000,
  10_000_000_000,
];

/// Fast branchless computation of minimum bit width required (0..=64).
/// 快速计算表示数值所需的最少比特位数 (0..=64，无分支实现)
#[inline(always)]
pub const fn bits_needed(max_val: u64) -> u8 {
  (u64::BITS - max_val.leading_zeros()) as u8
}

/// Fast computation of low bit mask for bit width 0..=64.
/// 快速计算 0..=64 比特宽度的低位掩码
#[inline(always)]
pub const fn bit_mask(bit_width: u8) -> u64 {
  if bit_width >= BITS_U64 as u8 {
    u64::MAX
  } else {
    (1u64 << bit_width).wrapping_sub(1)
  }
}

/// ALP floating-point abstraction trait for unified zero-cost f32/f64 codec.
/// ALP 浮点数抽象特征（统一 f32 / f64 零成本编解码）
pub trait AlpFloat: Copy + Default + PartialEq + PartialOrd + Send + Sync + 'static {
  type Int: Copy + Default + PartialEq + Eq + PartialOrd + Ord + Send + Sync + 'static;
  type RawBits: Copy + Default + Send + Sync + 'static;

  const TYPE_BYTE: u8;
  const MAX_EXPONENT: u8;
  const MAX_FAC: u8;
  const MAX_BIT_WIDTH: u8;
  const MAGIC_NUMBER: Self;
  const ENCODING_UPPER_LIMIT: Self;
  const EXCEPTION_PENALTY: usize;
  const EXC_ENTRY_SIZE: usize;
  const BASE_SIZE: usize;
  const ZERO: Self;
  const ZERO_INT: Self::Int;
  const MIN_INT: Self::Int;
  const MAX_INT: Self::Int;

  fn exp_factor(exp: u8, fac: u8) -> Self;
  fn fac_int(fac: u8) -> i64;
  fn frac_exp(exp: u8) -> Self;

  fn is_impossible(self) -> bool;
  fn try_encode_fast(self, exp_factor: Self, fac_int: i64, frac_exp: Self) -> Option<Self::Int>;
  fn fast_round_to_int(self, exp_factor: Self) -> Self::Int;
  fn decode_from_int(encoded: Self::Int, fac_int: i64, frac_exp: Self) -> Self;
  fn decode_from_offset(offset: u64, base: Self::Int, fac_int: i64, frac_exp: Self) -> Self;

  fn int_diff_to_u64(val: Self::Int, base: Self::Int) -> u64;
  fn u64_to_int_add(offset: u64, base: Self::Int) -> Self::Int;
  fn calc_range(min_val: Self::Int, max_val: Self::Int) -> u64;

  #[inline(always)]
  fn bits_needed(max_offset: u64) -> u8 {
    bits_needed(max_offset)
  }

  fn to_raw_bits(self) -> Self::RawBits;
  fn from_raw_bits(bits: Self::RawBits) -> Self;

  fn write_base(base: Self::Int, dst: &mut Vec<u8>);
  fn read_base(src: &[u8]) -> Self::Int;

  fn write_exception(pos: u16, bits: Self::RawBits, dst: &mut Vec<u8>);
  fn read_exception(chunk: &[u8]) -> (usize, Self);

  #[inline(always)]
  fn build_lut<const N: usize>(base: Self::Int, fac_int: i64, frac_exp: Self) -> [Self; N] {
    let mut lut = [Self::ZERO; N];
    for (i, slot) in lut.iter_mut().enumerate() {
      *slot = Self::decode_from_offset(i as u64, base, fac_int, frac_exp);
    }
    lut
  }
}

impl AlpFloat for f64 {
  type Int = i64;
  type RawBits = u64;

  const TYPE_BYTE: u8 = TYPE_F64;
  const MAX_EXPONENT: u8 = MAX_EXPONENT_F64;
  const MAX_FAC: u8 = MAX_FAC_F64;
  const MAX_BIT_WIDTH: u8 = u64::BITS as u8;
  const MAGIC_NUMBER: Self = MAGIC_NUMBER_F64;
  const ENCODING_UPPER_LIMIT: Self = ENCODING_UPPER_LIMIT_F64;
  const EXC_ENTRY_SIZE: usize = EXC_POS_LEN + size_of::<Self::RawBits>();
  const EXCEPTION_PENALTY: usize = Self::EXC_ENTRY_SIZE * BITS_PER_BYTE;
  const BASE_SIZE: usize = size_of::<Self::Int>();
  const ZERO: Self = 0.0;
  const ZERO_INT: Self::Int = 0;
  const MIN_INT: Self::Int = i64::MIN;
  const MAX_INT: Self::Int = i64::MAX;

  #[inline(always)]
  fn exp_factor(exp: u8, fac: u8) -> Self {
    // SAFETY: 调用方已前置校验 fac <= exp <= MAX_EXPONENT_F64 (18)，且 EXP_ARR_F64 长度为 19，(exp - fac) 必然在 [0, 18] 范围内，索引绝不越界。
    unsafe { *EXP_ARR_F64.get_unchecked((exp - fac) as usize) }
  }

  #[inline(always)]
  fn fac_int(fac: u8) -> i64 {
    // SAFETY: 调用方已前置校验 fac <= MAX_FAC (8) <= 18，且 FACT_ARR_F64 长度为 19，fac 必然在 [0, 8] 范围内，索引绝不越界。
    unsafe { *FACT_ARR_F64.get_unchecked(fac as usize) }
  }

  #[inline(always)]
  fn frac_exp(exp: u8) -> Self {
    // SAFETY: 调用方已前置校验 exp <= MAX_EXPONENT_F64 (18)，且 FRAC_ARR_F64 长度为 19，exp 必然在 [0, 18] 范围内，索引绝不越界。
    unsafe { *FRAC_ARR_F64.get_unchecked(exp as usize) }
  }

  #[inline(always)]
  fn is_impossible(self) -> bool {
    !self.is_finite()
      || self.abs() > Self::ENCODING_UPPER_LIMIT
      || (self == Self::ZERO && self.is_sign_negative())
  }

  #[inline(always)]
  fn try_encode_fast(self, exp_factor: Self, fac_int: i64, frac_exp: Self) -> Option<Self::Int> {
    if self.is_impossible() {
      return None;
    }
    let scaled = self * exp_factor;
    if scaled.is_impossible() {
      return None;
    }
    let rounded = (scaled + Self::MAGIC_NUMBER) - Self::MAGIC_NUMBER;
    let encoded = rounded as i64;

    let int_with_fac = if fac_int == 1 {
      encoded
    } else {
      encoded.checked_mul(fac_int)?
    };
    let decoded = (int_with_fac as f64) * frac_exp;
    if decoded.to_bits() == self.to_bits() {
      Some(encoded)
    } else {
      None
    }
  }

  #[inline(always)]
  fn fast_round_to_int(self, exp_factor: Self) -> Self::Int {
    let scaled = self * exp_factor;
    let rounded = (scaled + Self::MAGIC_NUMBER) - Self::MAGIC_NUMBER;
    rounded as i64
  }

  #[inline(always)]
  fn decode_from_int(encoded: Self::Int, fac_int: i64, frac_exp: Self) -> Self {
    let int_with_fac = if fac_int == 1 {
      encoded
    } else {
      encoded.wrapping_mul(fac_int)
    };
    (int_with_fac as f64) * frac_exp
  }

  #[inline(always)]
  fn decode_from_offset(offset: u64, base: Self::Int, fac_int: i64, frac_exp: Self) -> Self {
    let unscaled = (offset as i64).wrapping_add(base);
    let int_with_fac = if fac_int == 1 {
      unscaled
    } else {
      unscaled.wrapping_mul(fac_int)
    };
    (int_with_fac as f64) * frac_exp
  }

  #[inline(always)]
  fn int_diff_to_u64(val: Self::Int, base: Self::Int) -> u64 {
    val.wrapping_sub(base) as u64
  }

  #[inline(always)]
  fn u64_to_int_add(offset: u64, base: Self::Int) -> Self::Int {
    (offset as i64).wrapping_add(base)
  }

  #[inline(always)]
  fn calc_range(min_val: Self::Int, max_val: Self::Int) -> u64 {
    max_val.wrapping_sub(min_val) as u64
  }

  #[inline(always)]
  fn to_raw_bits(self) -> Self::RawBits {
    self.to_bits()
  }

  #[inline(always)]
  fn from_raw_bits(bits: Self::RawBits) -> Self {
    f64::from_bits(bits)
  }

  #[inline(always)]
  fn write_base(base: Self::Int, dst: &mut Vec<u8>) {
    dst.extend_from_slice(&base.to_le_bytes());
  }

  #[inline(always)]
  fn read_base(src: &[u8]) -> Self::Int {
    // SAFETY: 调用方在进入前已校验 src.len() >= BASE_SIZE (8)，使用 read_unaligned 保证任何内存对齐下的安全读取。
    unsafe { i64::from_le(read_unaligned(src.as_ptr().cast::<i64>())) }
  }

  #[inline(always)]
  fn write_exception(pos: u16, bits: Self::RawBits, dst: &mut Vec<u8>) {
    dst.extend_from_slice(&pos.to_le_bytes());
    dst.extend_from_slice(&bits.to_le_bytes());
  }

  #[inline(always)]
  fn read_exception(chunk: &[u8]) -> (usize, Self) {
    // SAFETY: 调用方在进入前已校验 chunk.len() >= EXC_ENTRY_SIZE (10)，使用 read_unaligned 保证安全读取 u16 与 u64。
    unsafe {
      let pos = u16::from_le(read_unaligned(chunk.as_ptr().cast::<u16>())) as usize;
      let bits = u64::from_le(read_unaligned(
        chunk.as_ptr().add(EXC_POS_LEN).cast::<u64>(),
      ));
      (pos, f64::from_bits(bits))
    }
  }
}

impl AlpFloat for f32 {
  type Int = i32;
  type RawBits = u32;

  const TYPE_BYTE: u8 = TYPE_F32;
  const MAX_EXPONENT: u8 = MAX_EXPONENT_F32;
  const MAX_FAC: u8 = MAX_FAC_F32;
  const MAX_BIT_WIDTH: u8 = u32::BITS as u8;
  const MAGIC_NUMBER: Self = MAGIC_NUMBER_F32;
  const ENCODING_UPPER_LIMIT: Self = ENCODING_UPPER_LIMIT_F32;
  const EXC_ENTRY_SIZE: usize = EXC_POS_LEN + size_of::<Self::RawBits>();
  const EXCEPTION_PENALTY: usize = Self::EXC_ENTRY_SIZE * BITS_PER_BYTE;
  const BASE_SIZE: usize = size_of::<Self::Int>();
  const ZERO: Self = 0.0;
  const ZERO_INT: Self::Int = 0;
  const MIN_INT: Self::Int = i32::MIN;
  const MAX_INT: Self::Int = i32::MAX;

  #[inline(always)]
  fn exp_factor(exp: u8, fac: u8) -> Self {
    // SAFETY: 调用方已前置校验 fac <= exp <= MAX_EXPONENT_F32 (10)，且 EXP_ARR_F32 长度为 11，(exp - fac) 必然在 [0, 10] 范围内，索引绝不越界。
    unsafe { *EXP_ARR_F32.get_unchecked((exp - fac) as usize) }
  }

  #[inline(always)]
  fn fac_int(fac: u8) -> i64 {
    // SAFETY: 调用方已前置校验 fac <= MAX_FAC (4) <= 10，且 FACT_ARR_F32 长度为 11，fac 必然在 [0, 4] 范围内，索引绝不越界。
    unsafe { *FACT_ARR_F32.get_unchecked(fac as usize) }
  }

  #[inline(always)]
  fn frac_exp(exp: u8) -> Self {
    // SAFETY: 调用方已前置校验 exp <= MAX_EXPONENT_F32 (10)，且 FRAC_ARR_F32 长度为 11，exp 必然在 [0, 10] 范围内，索引绝不越界。
    unsafe { *FRAC_ARR_F32.get_unchecked(exp as usize) }
  }

  #[inline(always)]
  fn is_impossible(self) -> bool {
    !self.is_finite()
      || self.abs() > Self::ENCODING_UPPER_LIMIT
      || (self == Self::ZERO && self.is_sign_negative())
  }

  #[inline(always)]
  fn try_encode_fast(self, exp_factor: Self, fac_int: i64, frac_exp: Self) -> Option<Self::Int> {
    if self.is_impossible() {
      return None;
    }
    let scaled = self * exp_factor;
    if scaled.is_impossible() {
      return None;
    }
    let rounded = (scaled + Self::MAGIC_NUMBER) - Self::MAGIC_NUMBER;
    let encoded = rounded as i32;

    let int_with_fac = if fac_int == 1 {
      encoded as i64
    } else {
      (encoded as i64).checked_mul(fac_int)?
    };
    let decoded = (int_with_fac as f32) * frac_exp;
    if decoded.to_bits() == self.to_bits() {
      Some(encoded)
    } else {
      None
    }
  }

  #[inline(always)]
  fn fast_round_to_int(self, exp_factor: Self) -> Self::Int {
    let scaled = self * exp_factor;
    let rounded = (scaled + Self::MAGIC_NUMBER) - Self::MAGIC_NUMBER;
    rounded as i32
  }

  #[inline(always)]
  fn decode_from_int(encoded: Self::Int, fac_int: i64, frac_exp: Self) -> Self {
    let int_with_fac = if fac_int == 1 {
      encoded as i64
    } else {
      (encoded as i64).wrapping_mul(fac_int)
    };
    (int_with_fac as f32) * frac_exp
  }

  #[inline(always)]
  fn decode_from_offset(offset: u64, base: Self::Int, fac_int: i64, frac_exp: Self) -> Self {
    let unscaled = (offset as i32).wrapping_add(base);
    let int_with_fac = if fac_int == 1 {
      unscaled as i64
    } else {
      (unscaled as i64).wrapping_mul(fac_int)
    };
    (int_with_fac as f32) * frac_exp
  }

  #[inline(always)]
  fn int_diff_to_u64(val: Self::Int, base: Self::Int) -> u64 {
    val.wrapping_sub(base) as u32 as u64
  }

  #[inline(always)]
  fn u64_to_int_add(offset: u64, base: Self::Int) -> Self::Int {
    (offset as i32).wrapping_add(base)
  }

  #[inline(always)]
  fn calc_range(min_val: Self::Int, max_val: Self::Int) -> u64 {
    max_val.wrapping_sub(min_val) as u32 as u64
  }

  #[inline(always)]
  fn to_raw_bits(self) -> Self::RawBits {
    self.to_bits()
  }

  #[inline(always)]
  fn from_raw_bits(bits: Self::RawBits) -> Self {
    f32::from_bits(bits)
  }

  #[inline(always)]
  fn write_base(base: Self::Int, dst: &mut Vec<u8>) {
    dst.extend_from_slice(&base.to_le_bytes());
  }

  #[inline(always)]
  fn read_base(src: &[u8]) -> Self::Int {
    // SAFETY: 调用方在进入前已校验 src.len() >= BASE_SIZE (4)，使用 read_unaligned 保证任何内存对齐下的安全读取。
    unsafe { i32::from_le(read_unaligned(src.as_ptr().cast::<i32>())) }
  }

  #[inline(always)]
  fn write_exception(pos: u16, bits: Self::RawBits, dst: &mut Vec<u8>) {
    dst.extend_from_slice(&pos.to_le_bytes());
    dst.extend_from_slice(&bits.to_le_bytes());
  }

  #[inline(always)]
  fn read_exception(chunk: &[u8]) -> (usize, Self) {
    // SAFETY: 调用方在进入前已校验 chunk.len() >= EXC_ENTRY_SIZE (6)，使用 read_unaligned 保证安全读取 u16 与 u32。
    unsafe {
      let pos = u16::from_le(read_unaligned(chunk.as_ptr().cast::<u16>())) as usize;
      let bits = u32::from_le(read_unaligned(
        chunk.as_ptr().add(EXC_POS_LEN).cast::<u32>(),
      ));
      (pos, f32::from_bits(bits))
    }
  }
}
