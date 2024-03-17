use std::fmt;

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnumDataType {
  // Integers
  Int8 = 1,
  Int16 = 2,
  Int32 = 3,
  Int64 = 4,
  // Int128 = 5,
  // Unsigned Integers
  UInt8 = 6,
  UInt16 = 7,
  UInt32 = 8,
  UInt64 = 9,
  // UInt128 = 10,
  // Floats
  Float32 = 11,
  Float64 = 12,
  // Boolean
  Boolean = 13,
  // String
  // String = 14,
  DateTime32 = 16,
  DateTime64 = 17
  // UUID
  // Map
  // Array
  // Tuple
  // IP
  // etc...
}

impl EnumDataType {
  pub fn from_u16(value: u16) -> Option<Self> {
    match value {
      1 => Some(EnumDataType::Int8),
      2 => Some(EnumDataType::Int16),
      3 => Some(EnumDataType::Int32),
      4 => Some(EnumDataType::Int64),
      6 => Some(EnumDataType::UInt8),
      7 => Some(EnumDataType::UInt16),
      8 => Some(EnumDataType::UInt32),
      9 => Some(EnumDataType::UInt64),
      11 => Some(EnumDataType::Float32),
      12 => Some(EnumDataType::Float64),
      13 => Some(EnumDataType::Boolean),
      16 => Some(EnumDataType::DateTime32),
      17 => Some(EnumDataType::DateTime64),
      _ => None,
    }
  }
}

#[derive(Debug, Clone)]
pub enum EnumDataValue {
    Int8Value(i8),
    Int16Value(i16),
    Int32Value(i32),
    Int64Value(i64),
    UInt8Value(u8),
    UInt16Value(u16),
    UInt32Value(u32),
    UInt64Value(u64),
    Float32Value(f32),
    Float64Value(f64),
    BooleanValue(bool),
    DateTime32Value(i32),
    DateTime64Value(i64),
}

impl EnumDataValue {
  pub fn from_data_type(data_type: EnumDataType) -> Self {
    match data_type {
      EnumDataType::Int8 => EnumDataValue::Int8Value(0),
      EnumDataType::Int16 => EnumDataValue::Int16Value(0),
      EnumDataType::Int32 => EnumDataValue::Int32Value(0),
      EnumDataType::Int64 => EnumDataValue::Int64Value(0),
      EnumDataType::UInt8 => EnumDataValue::UInt8Value(0),
      EnumDataType::UInt16 => EnumDataValue::UInt16Value(0),
      EnumDataType::UInt32 => EnumDataValue::UInt32Value(0),
      EnumDataType::UInt64 => EnumDataValue::UInt64Value(0),
      EnumDataType::Float32 => EnumDataValue::Float32Value(0.0),
      EnumDataType::Float64 => EnumDataValue::Float64Value(0.0),
      EnumDataType::Boolean => EnumDataValue::BooleanValue(false),
      EnumDataType::DateTime32 => EnumDataValue::Int32Value(0),
      EnumDataType::DateTime64 => EnumDataValue::Int64Value(0),
      _ => unimplemented!(),
    }
  }
}

impl fmt::Display for EnumDataValue {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      match self {
          EnumDataValue::Int8Value(val) => write!(f, "{}", val),
          EnumDataValue::Int16Value(val) => write!(f, "{}", val),
          EnumDataValue::Int32Value(val) => write!(f, "{}", val),
          EnumDataValue::Int64Value(val) => write!(f, "{}", val),
          EnumDataValue::UInt8Value(val) => write!(f, "{}", val),
          EnumDataValue::UInt16Value(val) => write!(f, "{}", val),
          EnumDataValue::UInt32Value(val) => write!(f, "{}", val),
          EnumDataValue::UInt64Value(val) => write!(f, "{}", val),
          EnumDataValue::Float32Value(val) => write!(f, "{}", val),
          EnumDataValue::Float64Value(val) => write!(f, "{}", val),
          EnumDataValue::BooleanValue(val) => write!(f, "{}", val),
          EnumDataValue::DateTime32Value(val) => write!(f, "{}", val),
          EnumDataValue::DateTime64Value(val) => write!(f, "{}", val),
      }
  }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ColumnMeta {
  None,
  Decimal { precision: u32, scale: u32 },
  Enum { mappings: Vec<String> },
  DateTime { format: String },
  Text { encoding: String },
}

impl Default for ColumnMeta {
  fn default() -> Self {
      ColumnMeta::None
  }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnumDataEnc {
  // Types of Encoding
  None = 0,
  Delta = 1,
  DoubleDelta = 2,
}

impl EnumDataEnc {
  pub fn from_u8(value: u8) -> Option<Self> {
    match value {
      0 => Some(EnumDataEnc::None),
      1 => Some(EnumDataEnc::Delta),
      2 => Some(EnumDataEnc::DoubleDelta),
      _ => None,
    }
  }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnumDataComp {
  // Types of Compression
  None = 0,
  ZStd = 1,
}

impl EnumDataComp {
  pub fn from_u8(value: u8) -> Option<Self> {
    match value {
      0 => Some(EnumDataComp::None),
      1 => Some(EnumDataComp::ZStd),
      _ => None,
    }
  }
}

#[derive(Debug, Clone)]
pub enum EnumColumnData {
  Int8Vec(Vec<i8>),
  Int16Vec(Vec<i16>),
  Int32Vec(Vec<i32>),
  Int64Vec(Vec<i64>),
  UInt8Vec(Vec<u8>),
  UInt16Vec(Vec<u16>),
  UInt32Vec(Vec<u32>),
  UInt64Vec(Vec<u64>),
  Float32Vec(Vec<f32>),
  Float64Vec(Vec<f64>),
  BooleanVec(Vec<bool>),
  DateTime32Vec(Vec<i32>),
  DateTime64Vec(Vec<i64>),
  // StringVec(Vec<String>),
}

impl EnumColumnData {
  pub fn from_enum_data_type(data_type: EnumDataType) -> EnumColumnData {
    match data_type {
      EnumDataType::Int8 => EnumColumnData::Int8Vec(Vec::new()),
      EnumDataType::Int16 => EnumColumnData::Int16Vec(Vec::new()),
      EnumDataType::Int32 => EnumColumnData::Int32Vec(Vec::new()),
      EnumDataType::Int64 => EnumColumnData::Int64Vec(Vec::new()),
      EnumDataType::UInt8 => EnumColumnData::UInt8Vec(Vec::new()),
      EnumDataType::UInt16 => EnumColumnData::UInt16Vec(Vec::new()),
      EnumDataType::UInt32 => EnumColumnData::UInt32Vec(Vec::new()),
      EnumDataType::UInt64 => EnumColumnData::UInt64Vec(Vec::new()),
      EnumDataType::Float32 => EnumColumnData::Float32Vec(Vec::new()),
      EnumDataType::Float64 => EnumColumnData::Float64Vec(Vec::new()),
      EnumDataType::Boolean => EnumColumnData::BooleanVec(Vec::new()),
      EnumDataType::DateTime32 => EnumColumnData::DateTime32Vec(Vec::new()),
      EnumDataType::DateTime64 => EnumColumnData::DateTime64Vec(Vec::new()),
      // Add cases for other data types as needed...
    }
  }
}
