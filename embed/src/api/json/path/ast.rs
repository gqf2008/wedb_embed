use std::borrow::Cow;

use sonic_rs::Value;

/// Operation definition.
/// 数组切片索引
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SliceIndex {
  Index(isize),
  Slice {
    start: Option<isize>,
    stop: Option<isize>,
    step: Option<isize>,
  },
}

/// Operation definition.
/// JSONPath 过滤操作符
#[derive(Debug, Clone, PartialEq)]
pub enum FilterOp {
  Exists,
  NotExists,
  Eq(Value),
  Ne(Value),
  Lt(f64),
  Le(f64),
  Gt(f64),
  Ge(f64),
}

/// Operation definition.
/// JSONPath 过滤表达式 (如 `@.price < 30`, `@.name == 'Alice'`, `@.active`, `!@.active`)
#[derive(Debug, Clone, PartialEq)]
pub struct FilterExpr<'a> {
  pub path: Vec<Cow<'a, str>>,
  pub op: FilterOp,
}

/// Operation definition.
/// JSONPath 语法片段（基于 Cow<'a, str> 零堆分配借用路径）
#[derive(Debug, Clone, PartialEq)]
pub enum PathSegment<'a> {
  Root,
  Field(Cow<'a, str>),
  MultiField(Vec<Cow<'a, str>>),
  Index(isize),
  MultiIndex(Vec<SliceIndex>),
  Wildcard,
  Filter(FilterExpr<'a>),
  Recursive(Box<PathSegment<'a>>),
}
