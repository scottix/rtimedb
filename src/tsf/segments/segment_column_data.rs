use std::{fs::File, io::{self, Cursor, Read, Write}};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use tracing::trace;

use super::types::{EnumColumnData, EnumDataComp, EnumDataEnc, EnumDataType};

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

impl ColumnDataCreator for i32 {
  fn create_segment_column_data(column: Vec<Self>, encoding: EnumDataEnc, compression: EnumDataComp) -> SegmentColumnData {
    SegmentColumnData::new_int32_vec(column, encoding, compression)
  }
}

pub struct SegmentColumnData {
  pub data: EnumColumnData,
  encoding: EnumDataEnc,
  compression: EnumDataComp,
  buffer: Option<Vec<u8>>,
}

impl SegmentColumnData {
  pub fn get_data<'a>(&'a self) -> Option<&'a EnumColumnData> {
    Some(&self.data)
  }

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

  fn new_datetime32_vec(initial_data: Vec<i32>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::DateTime32Vec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_datetime64_vec(initial_data: Vec<i64>, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::DateTime64Vec(initial_data),
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  pub fn convert_data_into_buffer(&mut self) -> io::Result<usize> {
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
      EnumColumnData::DateTime32Vec(data) => {
        for &value in data {
          buffer.write_i32::<LittleEndian>(value)?;
        }
      },
      EnumColumnData::DateTime64Vec(data) => {
        for &value in data {
          buffer.write_i64::<LittleEndian>(value)?;
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

  pub fn convert_buffer_into_data(&mut self) -> io::Result<()> {
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
      EnumColumnData::DateTime32Vec(data_vec) => {
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
      EnumColumnData::DateTime64Vec(data_vec) => {
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
    }

    Ok(())
  }

  pub fn write_buffer_into_file(&self, file: &mut File) -> io::Result<()> {
    trace!("SegmentColumnData::write_buffer_into_file");
    
    if let Some(ref buffer) = self.buffer {
      file.write_all(buffer)?;
    } else {
      return Err(io::Error::new(io::ErrorKind::Other, "Data not prepared"));
    }

    Ok(())
  }

  pub fn read_file_into_buffer(&mut self, file: &mut File, bytes: usize) -> io::Result<()> {
    trace!("SegmentColumnData::read_file_into_buffer");

    // Prepare the buffer
    self.buffer = Some(vec![0u8; bytes]);

    if let Some(ref mut buffer) = self.buffer {
        file.read_exact(buffer)?;
    } else {
        return Err(io::Error::new(io::ErrorKind::Other, "Buffer was not initialized."));
    }

    Ok(())
  }

}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::{Read, Seek, SeekFrom};
  use tempfile::tempfile;

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
      temp_file.seek(SeekFrom::Start(0))?;

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
