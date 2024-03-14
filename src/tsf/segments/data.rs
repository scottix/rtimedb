use std::{fs::File, io::{self, Cursor, Read, Write}};
use std::str;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use tracing::trace;
use uuid7;

#[repr(C)]
pub struct SegmentData {
  data_header: SegmentDataHeader,
  data: Vec<SegmentColumnData>,
}

impl SegmentData {
  // Initialize a new SegmentData with necessary header information
  pub fn new() -> Self {
    let data_header: SegmentDataHeader = SegmentDataHeader {
      tombstone: false,
      next_offset: 0,
      uuid_txid: [0; 16],
      date_start: 0,
      date_end: 0,
      row_count: 0,
      column_count: 0,
      column_header_size: 0,
      column_headers: vec![],
      segment_check: [0; 8],
    };
    SegmentData {
        data_header,
        data: vec![],
    }
  }

  pub fn start_tx(mut self) -> Self {
    trace!("SegmentData::start_tx");
    let txid: uuid7::Uuid = uuid7::uuid7();
    self.data_header.uuid_txid = *txid.as_bytes();

    self
  }

  pub fn get_column_count(self) -> u16 {
    trace!("SegmentData::get_column_count");
    self.data_header.column_count
  }

  pub fn get_segment_data<'a>(&'a self, index: usize) -> Option<&'a SegmentColumnData> {
    trace!("SegmentData::get_segment_data");
    return self.data.get(index);
  }

  pub fn add_column_header(&mut self, header: SegmentColumnHeader) {
    trace!("SegmentData::add_column_header");
    self.data_header.column_headers.push(header);
    self.data_header.column_count = self.data_header.column_headers.len() as u16;
    self.data_header.column_header_size = self.data_header.column_headers.iter()
      .map(|header| header.byte_size())
      .sum();
  }

  pub fn add_column_data(&mut self, data: SegmentColumnData) -> io::Result<()> {
    trace!("SegmentData::add_column_data");
    // Ensure there's a corresponding column header for the data being added.
    if self.data_header.column_count as usize <= self.data.len() {
      return Err(io::Error::new(io::ErrorKind::InvalidInput, "No corresponding column header for the data."));
    }

    let data_row_count: usize = match &data.data {
      EnumColumnData::Int8Vec(vec) => vec.len(),
      // @TODO Add cases for other data types...
      _ => 0,
    };

    if data_row_count == 0 {
      return Err(io::Error::new(io::ErrorKind::InvalidInput, "Zero rows added"));
    }

    if self.data_header.row_count != 0 {
      if self.data_header.row_count as usize != data_row_count {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Inconsistent number of rows."));
      }
    } else {
      if data_row_count > 0 {
        self.data_header.row_count = data_row_count as u32;
      }
    }

    // Directly append the provided SegmentColumnData instance to the data vector.
    self.data.push(data);

    Ok(())
  }

  // Writes the SegmentData to a file, including the header and data.
  pub fn write_to_file(&mut self, file: &mut File) -> io::Result<()> {
    trace!("SegmentData::write_to_file");
    // First, ensure column sizes in headers match the data that will be written.
    let mut total_data_size: usize = 0;
    for (index, column_data) in self.data.iter_mut().enumerate() {
      // Prepare the buffer for each column and get its size.
      let data_size: usize = column_data.convert_data_into_buffer()?;
      self.data_header.column_headers[index].column_size = data_size as u64;
      total_data_size += data_size;
    }

    // Next, calculate the total size of the header, including dynamic parts.
    self.data_header.column_header_size = self.data_header.column_headers.iter()
        .map(|header| header.byte_size() as u32)
        .sum::<u32>();

    // Calculate next_offset based on header size and total data size.
    self.data_header.next_offset = self.data_header.calculate_header_size() + total_data_size as u32;

    // Now, write the header to the file.
    self.data_header.write_header(file)?;

    // Write each column's data from its prepared buffer to the file.
    for column_data in &self.data {
      if let Some(ref buffer) = column_data.buffer {
        file.write_all(buffer)?;
      } else {
        // If for some reason the buffer is not prepared, you might want to handle this as an error.
        return Err(io::Error::new(io::ErrorKind::Other, "Data buffer not prepared"));
      }
    }

    Ok(())
  }

  // Reads SegmentData from a file, reconstructing the header and data.
  pub fn read_segment_from_file(&mut self, file: &mut File) -> io::Result<()> {
    trace!("SegmentData::read_segment_from_file");
    self.data_header.read_segment_header(file)?;
    self.read_segment_data(file)?;

    Ok(())
  }

  fn read_segment_data(&mut self, file: &mut File) -> io::Result<()> {
    trace!("SegmentData::read_segment_data");
    // Ensure the data vector is clear or correctly sized before loading new data
    self.data.clear();

    for header in &self.data_header.column_headers {
      let mut column_data: SegmentColumnData = SegmentColumnData::new(
        header.column_type,
        header.column_enc,
        header.column_comp,
      );
      column_data.read_file_into_buffer(file, header.column_size as usize)?;
      column_data.convert_buffer_into_data()?;
      self.data.push(column_data);
    }

    Ok(())
  }
}

#[repr(C)]
pub struct SegmentDataHeader {
  tombstone: bool,
  next_offset: u32,
  uuid_txid: [u8; 16],
  date_start: u64,
  date_end: u64,
  row_count: u32,
  column_count: u16,
  column_header_size: u32,
  column_headers: Vec<SegmentColumnHeader>,
  segment_check: [u8; 8],
}

impl SegmentDataHeader {
  fn calculate_header_size(&self) -> u32 {
    trace!("SegmentDataHeader::calculate_header_size");
    // Fixed size parts: 1 (tombstone) + 4 (next_offset) + 16 (uuid_txid) + 8 (date_start) + 8 (date_end) + 
    // 4 (row_count) + 2 (column_count) + 4 (column_header_size) + 8 (segment_check)
    let fixed_size = 1 + 4 + 16 + 8 + 8 + 4 + 2 + 4 + 8;
    fixed_size + self.column_header_size
  }

  fn write_header(&self, file: &mut File) -> io::Result<()> {
    trace!("SegmentDataHeader::write_header");
    let mut buffer = Vec::new();

    // Write the fixed-size part of the header
    buffer.push(self.tombstone as u8);
    buffer.write_u32::<LittleEndian>(self.next_offset)?;
    buffer.extend_from_slice(&self.uuid_txid);
    buffer.write_u64::<LittleEndian>(self.date_start)?;
    buffer.write_u64::<LittleEndian>(self.date_end)?;
    buffer.write_u32::<LittleEndian>(self.row_count)?;
    buffer.write_u16::<LittleEndian>(self.column_count)?;

    // Serialize and write each column header, keeping track of the total size
    let mut column_headers_buffer: Vec<u8> = Vec::new();
    for column_header in &self.column_headers {
      let column_header_buf: Vec<u8> = column_header.prepare_buffer()?;
      column_headers_buffer.extend(column_header_buf);
    }

    // Update and write the column_header_size
    let column_header_size: u32 = column_headers_buffer.len() as u32;
    buffer.write_u32::<LittleEndian>(column_header_size)?;

    // Append the serialized column headers
    buffer.extend_from_slice(&column_headers_buffer);

    // Append the segment check
    buffer.extend_from_slice(&self.segment_check);

    // Write the entire buffer to the file in one go
    file.write_all(&buffer)?;

    Ok(())
  }

  fn read_segment_header(&mut self, file: &mut File) -> io::Result<()> {
    trace!("SegmentDataHeader::read_segment_header");
    let mut buffer: [u8; 47] = [0; 47];
    file.read_exact(&mut buffer)?;

    self.tombstone = buffer[0] != 0;
    self.next_offset = LittleEndian::read_u32(&buffer[1..5]); 
    self.uuid_txid.copy_from_slice(&buffer[5..21]);
    self.date_start = LittleEndian::read_u64(&buffer[21..29]);
    self.date_end = LittleEndian::read_u64(&buffer[29..37]);
    self.row_count = LittleEndian::read_u32(&buffer[37..41]);
    self.column_count = LittleEndian::read_u16(&buffer[41..43]);
    self.column_header_size = LittleEndian::read_u32(&buffer[43..47]);

    // Preparing to read column_headers and segment_check together
    let total_size = self.column_header_size as usize + 8;
    let mut column_headers_and_check: Vec<u8> = vec![0u8; total_size];
    file.read_exact(&mut column_headers_and_check)?;

    let mut pos: usize = 0;
    self.column_headers.clear();

    for _ in 0..self.column_count {
      let column_header: SegmentColumnHeader = SegmentColumnHeader::read_from_buffer(&column_headers_and_check, &mut pos)?;
      self.column_headers.push(column_header);
    }
    
    // Need to read the last 8 bytes
    let segment_check_start: usize = column_headers_and_check.len() - 8;
    self.segment_check.copy_from_slice(&column_headers_and_check[segment_check_start..]);

    Ok(())
  }
}

pub struct SegmentColumnHeader {
  column_name_length: u16,
  column_name: String,
  column_type: EnumDataType,
  column_meta_length: u16,
  column_meta: ColumnMeta,
  column_enc: EnumDataEnc,
  column_comp: EnumDataComp,
  column_size: u64,
  column_check: [u8; 8]
}

impl SegmentColumnHeader {
  pub fn new(column_name: String, column_type: EnumDataType, column_enc: EnumDataEnc, column_comp: EnumDataComp) -> Self {
    trace!("SegmentColumnHeader::SegmentColumnHeader::new");
    let column_name_length: u16 = column_name.len() as u16;
    let column_meta_length: u16 = 0;
    let column_meta: ColumnMeta = ColumnMeta::None;
    let column_size: u64 = 0;
    let column_check: [u8; 8] = [0u8; 8];

    SegmentColumnHeader {
        column_name_length,
        column_name,
        column_type,
        column_meta_length,
        column_meta,
        column_enc,
        column_comp,
        column_size,
        column_check,
    }
  }

  fn byte_size(&self) -> u32 {
    trace!("SegmentColumnHeader::byte_size");
    // Start with the size of fixed-length fields.
    let mut size: u32 = 0u32;

    size += 2; // column_name_length (u16)
    size += self.column_name.len() as u32; // Length of the column_name string
    size += 2; // column_type (u16)
    size += 2; // column_meta_length (u16)
    // Add the size of column_meta, assuming it can be determined.
    // For simplicity, this example assumes no metadata or fixed-size metadata.
    size += self.column_meta_length as u32;
    size += 1; // column_enc (u8)
    size += 1; // column_comp (u8)
    size += 8; // column_size (u64)
    size += 8; // column_check ([u8; 8])

    size
  }

  fn prepare_buffer(&self) -> io::Result<Vec<u8>> {
    trace!("SegmentColumnHeader::prepare_buffer");
    let mut buffer = Vec::new();

    // Write column name length and column name
    buffer.write_u16::<LittleEndian>(self.column_name_length)?;
    buffer.extend_from_slice(self.column_name.as_bytes());

    // Write column type
    let column_type_val = self.column_type as u16;
    buffer.write_u16::<LittleEndian>(column_type_val)?;

    // Write column meta length
    buffer.write_u16::<LittleEndian>(self.column_meta_length)?;

    // Assuming column_meta is serialized here. For simplicity, skipping actual serialization
    // You might need to serialize `column_meta` based on its type and content

    // Write column_enc and column_comp
    let column_enc_val = self.column_enc as u8;
    buffer.push(column_enc_val);

    let column_comp_val = self.column_comp as u8;
    buffer.push(column_comp_val);

    // Write column size
    buffer.write_u64::<LittleEndian>(self.column_size)?;

    // Write column check
    buffer.extend_from_slice(&self.column_check);

    Ok(buffer)
  }

  fn read_from_buffer(buffer: &[u8], pos: &mut usize) -> io::Result<Self> {
    trace!("SegmentColumnHeader::read_from_buffer");
    let column_name_length: u16 = LittleEndian::read_u16(&buffer[*pos..*pos+2]);
    *pos += 2;

    let column_name_end: usize = *pos + column_name_length as usize;
    let column_name_bytes: &[u8] = &buffer[*pos..column_name_end];
    *pos += column_name_length as usize;

    let column_name: String = std::str::from_utf8(column_name_bytes)
      .map_err(|e: str::Utf8Error| io::Error::new(io::ErrorKind::InvalidData, e))?
      .to_string();

    let column_type: u16 = LittleEndian::read_u16(&buffer[*pos..*pos+2]);
    *pos += 2;

    let column_meta_length: u16 = LittleEndian::read_u16(&buffer[*pos..*pos+2]);
    *pos += 2;

    // Skip meta bytes for simplicity in this example
    *pos += column_meta_length as usize;

    let column_enc: u8 = buffer[*pos];
    *pos += 1;

    let column_comp: u8 = buffer[*pos];
    *pos += 1;

    let column_size: u64 = LittleEndian::read_u64(&buffer[*pos..*pos+8]);
    *pos += 8;

    let column_check_slice: &[u8] = &buffer[*pos..*pos+8];
    let mut column_check: [u8; 8] = [0; 8];
    column_check.copy_from_slice(column_check_slice);
    *pos += 8;

    Ok(Self {
        column_name_length,
        column_name,
        column_type: EnumDataType::from_u16(column_type).unwrap_or_else(|| panic!("Invalid column type")),
        column_meta_length,
        column_meta: ColumnMeta::default(), // This might need actual logic to parse based on `column_meta_length`
        column_enc: EnumDataEnc::from_u8(column_enc).unwrap_or_else(|| panic!("Invalid encoding type")),
        column_comp: EnumDataComp::from_u8(column_comp).unwrap_or_else(|| panic!("Invalid compression type")),
        column_size,
        column_check,
    })
  }
}

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
  // Datetime
  // UUID
  // Map
  // Array
  // Tuple
  // IP
  // etc...
}

impl EnumDataType {
  fn from_u16(value: u16) -> Option<Self> {
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
      _ => None,
    }
  }
}

#[derive(Debug, PartialEq, Eq)]
enum ColumnMeta {
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
  fn from_u8(value: u8) -> Option<Self> {
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
  fn from_u8(value: u8) -> Option<Self> {
    match value {
      0 => Some(EnumDataComp::None),
      1 => Some(EnumDataComp::ZStd),
      _ => None,
    }
  }
}

pub trait ColumnDataCreator {
  fn create_segment_column_data(column: Vec<Self>, encoding: EnumDataEnc, compression: EnumDataComp) -> SegmentColumnData
  where
    Self: Sized;
}

impl ColumnDataCreator for i8 {
  fn create_segment_column_data(column: Vec<Self>, encoding: EnumDataEnc, compression: EnumDataComp) -> SegmentColumnData {
    SegmentColumnData::new_int8_vec(column, encoding, compression)
  }
}

impl ColumnDataCreator for i16 {
  fn create_segment_column_data(column: Vec<Self>, encoding: EnumDataEnc, compression: EnumDataComp) -> SegmentColumnData {
    SegmentColumnData::new_int16_vec(column, encoding, compression)
  }
}

pub struct SegmentColumnData {
  pub data: EnumColumnData,
  encoding: EnumDataEnc,
  compression: EnumDataComp,
  buffer: Option<Vec<u8>>,
}

impl SegmentColumnData {
  pub fn new(data_type: EnumDataType, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    trace!("SegmentColumnData::new");
    SegmentColumnData {
      data: EnumColumnData::from_enum_data_type(data_type),
      encoding: encoding,
      compression: compression,
      buffer: None,
    }
  }

  pub fn new_int8_vec(initial_data: Vec<i8>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    trace!("SegmentColumnData::new_int8_vec");
    SegmentColumnData {
        data: EnumColumnData::Int8Vec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_int16_vec(initial_data: Vec<i16>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::Int16Vec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_int32_vec(initial_data: Vec<i32>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::Int32Vec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_int64_vec(initial_data: Vec<i64>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::Int64Vec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_uint8_vec(initial_data: Vec<u8>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::UInt8Vec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_uint16_vec(initial_data: Vec<u16>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::UInt16Vec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_uint32_vec(initial_data: Vec<u32>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::UInt32Vec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_uint64_vec(initial_data: Vec<u64>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::UInt64Vec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_float32_vec(initial_data: Vec<f32>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::Float32Vec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_float64_vec(initial_data: Vec<f64>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::Float64Vec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_boolean_vec(initial_data: Vec<bool>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::BooleanVec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn convert_data_into_buffer(&mut self) -> io::Result<usize> {
    trace!("SegmentColumnData::convert_data_into_buffer");
    let mut buffer: Vec<u8> = Vec::new();

    match &self.data {
      EnumColumnData::Int8Vec(data) => {
        for &value in data {
          buffer.write_i8(value)?;
        }
      },
      EnumColumnData::Int16Vec(data) => {
        for &value in data {
          buffer.write_i16::<LittleEndian>(value)?;
        }
      },
      EnumColumnData::Int32Vec(data) => {
        for &value in data {
          buffer.write_i32::<LittleEndian>(value)?;
        }
      },
      EnumColumnData::Int64Vec(data) => {
        for &value in data {
          buffer.write_i64::<LittleEndian>(value)?;
        }
      },
      EnumColumnData::UInt8Vec(data) => {
        for &value in data {
          buffer.write_u8(value)?;
        }
      },
      EnumColumnData::UInt16Vec(data) => {
        for &value in data {
          buffer.write_u16::<LittleEndian>(value)?;
        }
      },
      EnumColumnData::UInt32Vec(data) => {
        for &value in data {
          buffer.write_u32::<LittleEndian>(value)?;
        }
      },
      EnumColumnData::UInt64Vec(data) => {
        for &value in data {
          buffer.write_u64::<LittleEndian>(value)?;
        }
      },
      EnumColumnData::Float32Vec(data) => {
        for &value in data {
          buffer.write_f32::<LittleEndian>(value)?;
        }
      },
      EnumColumnData::Float64Vec(data) => {
        for &value in data {
          buffer.write_f64::<LittleEndian>(value)?;
        }
      },
      EnumColumnData::BooleanVec(data) => {
        for &value in data {
          // Convert bool to u8 (true -> 255, false -> 0)
          let byte_value: u8 = if value { 255u8 } else { 0u8 };
          buffer.write_u8(byte_value)?;
        }
      },
      // EnumColumnData::StringVec(data) => {
      //   for value in data {
      //     file.write_all(value.as_bytes())?;
      //   }
      // },
      // Handle other types...
    }

    let total_bytes: usize = buffer.len();
    self.buffer = Some(buffer);

    Ok(total_bytes)
  }

  fn convert_buffer_into_data(&mut self) -> io::Result<()> {
    trace!("SegmentColumnData::convert_buffer_into_data");
    let buffer: Vec<u8> = self.buffer.take()
      .ok_or(io::Error::new(io::ErrorKind::Other, "Buffer is empty"))?;

    let mut cursor: Cursor<Vec<u8>> = Cursor::new(buffer);

    match &mut self.data {
      EnumColumnData::Int8Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = cursor.read_i8() {
          data_vec.push(value);
        }

        if let Err(e) = cursor.read_i8() {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::Int16Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = cursor.read_i16::<LittleEndian>() {
          data_vec.push(value);
        }

        if let Err(e) = cursor.read_i16::<LittleEndian>() {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::Int32Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = cursor.read_i32::<LittleEndian>() {
          data_vec.push(value);
        }

        if let Err(e) = cursor.read_i32::<LittleEndian>() {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::Int64Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = cursor.read_i64::<LittleEndian>() {
          data_vec.push(value);
        }

        if let Err(e) = cursor.read_i64::<LittleEndian>() {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::UInt8Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = cursor.read_u8() {
          data_vec.push(value);
        }

        if let Err(e) = cursor.read_u8() {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::UInt16Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = cursor.read_u16::<LittleEndian>() {
          data_vec.push(value);
        }

        if let Err(e) = cursor.read_u16::<LittleEndian>() {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::UInt32Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = cursor.read_u32::<LittleEndian>() {
          data_vec.push(value);
        }

        if let Err(e) = cursor.read_u32::<LittleEndian>() {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::UInt64Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = cursor.read_u64::<LittleEndian>() {
          data_vec.push(value);
        }

        if let Err(e) = cursor.read_u64::<LittleEndian>() {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::Float32Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = cursor.read_f32::<LittleEndian>() {
          data_vec.push(value);
        }

        if let Err(e) = cursor.read_f32::<LittleEndian>() {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::Float64Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = cursor.read_f64::<LittleEndian>() {
          data_vec.push(value);
        }

        if let Err(e) = cursor.read_f64::<LittleEndian>() {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::BooleanVec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = cursor.read_u8() {
          // Convert bool to u8 (true -> 255, false -> 0)
          let bool_value: bool = if value == 0u8 { false } else { true };
          data_vec.push(bool_value);
        }

        if let Err(e) = cursor.read_u8() {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
    }

    Ok(())
  }

  fn write_buffer_into_file(&self, file: &mut File) -> io::Result<()> {
    trace!("SegmentColumnData::write_buffer_into_file");
    if let Some(ref buffer) = self.buffer {
      file.write_all(buffer)?;
      Ok(())
    } else {
      Err(io::Error::new(io::ErrorKind::Other, "Data not prepared"))
    }
  }

  fn read_file_into_buffer(&mut self, file: &mut File, bytes: usize) -> io::Result<()> {
    trace!("SegmentColumnData::read_file_into_buffer");
    self.buffer = Some(vec![0u8; bytes]);

    if let Some(ref mut buffer) = self.buffer {
        file.read_exact(buffer)?;
    } else {
        return Err(io::Error::new(io::ErrorKind::Other, "Buffer was not initialized."));
    }
    Ok(())
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
      // Add cases for other data types as needed...
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::{Read, Seek, SeekFrom};
  use tempfile::tempfile;

  #[test]
  fn test_write_and_read_header() -> io::Result<()> {
    // Setup: Create a SegmentDataHeader with test data
    let header: SegmentDataHeader = SegmentDataHeader {
      tombstone: true,
      next_offset: 123,
      uuid_txid: [0xAA; 16],
      date_start: 1625097600,
      date_end: 1627689600,
      row_count: 10,
      column_count: 0, // Simplified to 0 for this test
      column_header_size: 0, // Assuming no column headers for simplicity
      column_headers: vec![],
      segment_check: [0xBB; 8],
    };

    // Write the header to a temporary file
    let mut file: File = tempfile()?;
    header.write_header(&mut file)?;

    // Reset the file cursor to the beginning
    file.seek(SeekFrom::Start(0))?;

    // Read back the written data
    let mut read_tombstone: [u8; 1] = [0u8; 1];
    file.read_exact(&mut read_tombstone)?;
    let read_next_offset: u32 = file.read_u32::<LittleEndian>()?;
    let mut read_uuid_txid: [u8; 16] = [0u8; 16];
    file.read_exact(&mut read_uuid_txid)?;
    let read_date_start: u64 = file.read_u64::<LittleEndian>()?;
    let read_date_end: u64 = file.read_u64::<LittleEndian>()?;
    let read_row_count: u32 = file.read_u32::<LittleEndian>()?;
    let read_column_count: u16 = file.read_u16::<LittleEndian>()?;
    let read_column_header_size: u32 = file.read_u32::<LittleEndian>()?;
    // For column_headers, you would read and deserialize them here based on read_column_count and read_column_header_size
    let mut read_segment_check: [u8; 8] = [0u8; 8];
    file.read_exact(&mut read_segment_check)?;

    // Verify the data read matches what was written
    assert_eq!(read_tombstone[0], 1u8);
    assert_eq!(read_next_offset, 123);
    assert_eq!(read_uuid_txid, [0xAA; 16]);
    assert_eq!(read_date_start, 1625097600);
    assert_eq!(read_date_end, 1627689600);
    assert_eq!(read_row_count, 10);
    assert_eq!(read_column_count, 0);
    assert_eq!(read_column_header_size, 0);
    assert_eq!(read_segment_check, [0xBB; 8]);

    Ok(())
  }

  #[test]
  fn test_prepare_buffer() -> io::Result<()> {
    let header: SegmentColumnHeader = SegmentColumnHeader {
      column_name_length: 4, // Assuming "Test" is the column name
      column_name: "Test".to_string(),
      column_type: EnumDataType::Int32, // Example, ensure this matches an actual variant
      column_meta_length: 0, // Simplified for the test
      column_meta: ColumnMeta::None, // Assuming ColumnMeta::None is the default
      column_enc: EnumDataEnc::None, // Example, ensure this matches an actual variant
      column_comp: EnumDataComp::None, // Example, ensure this matches an actual variant
      column_size: 123, // Example size
      column_check: [1, 2, 3, 4, 5, 6, 7, 8], // Example checksum
    };

    let buffer: Vec<u8> = header.prepare_buffer()?;
    
    let mut expected_buffer: Vec<u8> = Vec::new();
    expected_buffer.write_u16::<LittleEndian>(header.column_name_length)?;
    expected_buffer.extend_from_slice(header.column_name.as_bytes());
    expected_buffer.write_u16::<LittleEndian>(header.column_type as u16)?;
    expected_buffer.write_u16::<LittleEndian>(header.column_meta_length)?;
    // Skipping actual serialization of `column_meta` for simplicity
    expected_buffer.push(header.column_enc as u8);
    expected_buffer.push(header.column_comp as u8);
    expected_buffer.write_u64::<LittleEndian>(header.column_size)?;
    expected_buffer.extend_from_slice(&header.column_check);

    assert_eq!(buffer, expected_buffer, "The prepared buffer does not match the expected bytes.");

    Ok(())
  }

  #[test]
  fn test_read_full_header() -> io::Result<()> {
      let mut file: File = tempfile()?;

      // Example data to write to the tempfile
      let tombstone: u8 = 1;
      let next_offset: u32 = 123; // Example offset
      let uuid_txid: [u8; 16] = [0xAA; 16]; // Example UUID
      let date_start: u64 = 1625097600;
      let date_end: u64 = 1627689600;
      let row_count: u32 = 10;
      let column_count: u16 = 0;
      let column_header_size: u32 = 0; // Simplified, assuming no column headers for this test
      let segment_check: [u8; 8] = [0xBB; 8]; // Example checksum

      // Write these values to the tempfile
      file.write_all(&[tombstone])?;
      file.write_u32::<LittleEndian>(next_offset)?;
      file.write_all(&uuid_txid)?;
      file.write_u64::<LittleEndian>(date_start)?;
      file.write_u64::<LittleEndian>(date_end)?;
      file.write_u32::<LittleEndian>(row_count)?;
      file.write_u16::<LittleEndian>(column_count)?;
      file.write_u32::<LittleEndian>(column_header_size)?;
      // Assuming column_headers would be written here
      file.write_all(&segment_check)?;

      // Reset file position to the beginning before reading
      file.seek(io::SeekFrom::Start(0))?;

      // Attempt to read the header back from the tempfile
      let mut header: SegmentDataHeader = SegmentDataHeader {
          tombstone: false,
          next_offset: 0,
          uuid_txid: [0; 16],
          date_start: 0,
          date_end: 0,
          row_count: 0,
          column_count: 0,
          column_header_size: 0,
          column_headers: Vec::new(),
          segment_check: [0; 8],
      };
      
      header.read_segment_header(&mut file)?;

      // Perform assertions
      assert_eq!(header.tombstone, true);
      assert_eq!(header.next_offset, 123);
      assert_eq!(header.uuid_txid, [0xAA; 16]);
      assert_eq!(header.date_start, 1625097600);
      assert_eq!(header.date_end, 1627689600);
      assert_eq!(header.row_count, 10);
      assert_eq!(header.column_count, 0);
      assert_eq!(header.column_header_size, 0); // Simplified
      assert_eq!(header.segment_check, [0xBB; 8]);

      Ok(())
  }

  #[test]
  fn test_segment_column_header_read_from_buffer() -> io::Result<()> {
      // Prepare a buffer to simulate serialized SegmentColumnHeader data
      let mut buffer: Vec<u8> = vec![];
      let column_name: &str = "TestColumn";
      let column_name_length = column_name.len() as u16;
      
      // Simulate writing data as it would be serialized
      buffer.write_u16::<LittleEndian>(column_name_length)?;
      buffer.extend_from_slice(column_name.as_bytes());
      
      let column_type: u16 = EnumDataType::Int32 as u16; // Example enum to u16 conversion
      buffer.write_u16::<LittleEndian>(column_type)?;
      
      let column_meta_length: u16 = 0u16; // Assuming no meta for simplicity
      buffer.write_u16::<LittleEndian>(column_meta_length)?;
      
      let column_enc: u8 = EnumDataEnc::None as u8; // Example enum to u8 conversion
      buffer.push(column_enc);
      
      let column_comp: u8 = EnumDataComp::None as u8; // Example enum to u8 conversion
      buffer.push(column_comp);
      
      let column_size: u64 = 123u64;
      buffer.write_u64::<LittleEndian>(column_size)?;

      let column_check: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
      buffer.extend_from_slice(&column_check);
      
      // Now try to read this buffer back into a SegmentColumnHeader
      let mut pos: usize = 0;
      let header: SegmentColumnHeader = SegmentColumnHeader::read_from_buffer(&buffer, &mut pos)?;
      
      // Perform assertions to verify the correctness of the parsed data
      assert_eq!(header.column_name, "TestColumn");
      assert_eq!(header.column_type, EnumDataType::Int32); // Ensure correct enum variant
      assert_eq!(header.column_enc, EnumDataEnc::None); // Ensure correct enum variant
      assert_eq!(header.column_comp, EnumDataComp::None); // Ensure correct enum variant
      assert_eq!(header.column_size, 123);
      assert_eq!(header.column_check, column_check);

      Ok(())
  }

  #[test]
  fn test_prepare_and_write_int8_data() -> io::Result<()> {
      let mut segment_data: SegmentColumnData = SegmentColumnData::new_int8_vec(
        vec![1, 2, -3, -4],
        EnumDataEnc::None,
        EnumDataComp::None
      );
      let prepare_bytes: usize = segment_data.convert_data_into_buffer()?;
      assert_eq!(prepare_bytes, 4);

      // Write data to a temporary file
      let mut temp_file: File = tempfile()?;
      segment_data.write_buffer_into_file(&mut temp_file)?;

      // Seek back to beginning
      temp_file.seek(std::io::SeekFrom::Start(0))?;

      // Read back the data to verify
      let mut read_buffer: Vec<u8> = Vec::new();
      temp_file.read_to_end(&mut read_buffer)?;
      assert_eq!(read_buffer, vec![1u8, 2, 253, 252]);

      Ok(())
  }

  #[test]
  fn test_read_int8_data() -> io::Result<()> {
      // Prepare a buffer with int8 data and write it to a temporary file
      let data: Vec<i8> = vec![1i8, 2, -3, -4];
      let mut temp_file: File = tempfile()?;
      for &val in &data {
          temp_file.write_i8(val)?;
      }

      // Ensure the file cursor is set to the start before reading
      temp_file.seek(std::io::SeekFrom::Start(0))?;

      // Initialize SegmentColumnData and read data from the file
      let mut segment_data: SegmentColumnData = SegmentColumnData::new_int8_vec(
        Vec::new(),
        EnumDataEnc::None,
        EnumDataComp::None
      );
      segment_data.read_file_into_buffer(&mut temp_file, 4)?;

      // Verify the buffer matches the original data
      if let Some(buffer) = &segment_data.buffer {
          assert_eq!(*buffer, vec![1u8, 2, 253, 252]);
      } else {
          panic!("Buffer was not populated");
      }

      Ok(())
  }
}
