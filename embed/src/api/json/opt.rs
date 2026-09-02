use bitcode::{Decode, Encode};

/// Operation definition.
/// JSON.SET 命令条件选项
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum JsonSet {
  /// Operation definition.
  /// 仅在键不存在时写入
  Nx,
  /// Operation definition.
  /// 仅在键已存在时覆盖写入
  Xx,
}

/// Command options enumeration.
/// JSON.ARRINDEX 命令区间选项枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum JsonArrIndex {
  Start(isize),
  Stop(isize),
  Range(isize, isize),
}

/// Operation definition.
/// JSON 数值运算类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum JsonNumberOp {
  /// Operation definition.
  /// 数值累加 (Incr)
  Incr,
  /// Operation definition.
  /// 数值累乘 (Mul)
  Mul,
}

/// Command options enumeration.
/// JSON.GET 格式化选项枚举
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonGet {
  Indent(String),
  Newline(String),
  Space(String),
}
