use std::io::Cursor;

use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tracing::trace;

use super::types::{EnumColumnData, EnumDataComp, EnumDataEnc, EnumDataType};

pub trait ColumnDataCreator {
  fn create_segment_column_data(column: Vec<Self>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> SegmentColumnData
  where
    Self: Sized;
}

impl ColumnDataCreator for i8 {
  fn create_segment_column_data(column: Vec<Self>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> SegmentColumnData {
    SegmentColumnData::new_int8_vec(column, file_pos, encoding, compression)
  }
}

impl ColumnDataCreator for i16 {
  fn create_segment_column_data(column: Vec<Self>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> SegmentColumnData {
    SegmentColumnData::new_int16_vec(column, file_pos, encoding, compression)
  }
}

impl ColumnDataCreator for i32 {
  fn create_segment_column_data(column: Vec<Self>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> SegmentColumnData {
    SegmentColumnData::new_int32_vec(column, file_pos, encoding, compression)
  }
}

pub struct SegmentColumnData {
  pub data: EnumColumnData,
  file_pos: usize,
  encoding: EnumDataEnc,
  compression: EnumDataComp,
  buffer: Option<Vec<u8>>,
}

impl SegmentColumnData {
  pub fn get_data<'a>(&'a self) -> Option<&'a EnumColumnData> {
    trace!("SegmentColumnData::get_data");
    Some(&self.data)
  }

  pub fn new(data_type: EnumDataType, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    trace!("SegmentColumnData::new");
    SegmentColumnData {
      data: EnumColumnData::from_enum_data_type(data_type),
      file_pos: file_pos,
      encoding: encoding,
      compression: compression,
      buffer: None,
    }
  }

  pub fn new_int8_vec(initial_data: Vec<i8>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    trace!("SegmentColumnData::new_int8_vec");
    SegmentColumnData {
        data: EnumColumnData::Int8Vec(initial_data),
        file_pos: file_pos,
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_int16_vec(initial_data: Vec<i16>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    trace!("SegmentColumnData::new_int16_vec");
    SegmentColumnData {
        data: EnumColumnData::Int16Vec(initial_data),
        file_pos: file_pos,
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_int32_vec(initial_data: Vec<i32>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    trace!("SegmentColumnData::new_int32_vec");
    SegmentColumnData {
        data: EnumColumnData::Int32Vec(initial_data),
        file_pos: file_pos,
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_int64_vec(initial_data: Vec<i64>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    trace!("SegmentColumnData::new_int64_vec");
    SegmentColumnData {
        data: EnumColumnData::Int64Vec(initial_data),
        file_pos: file_pos,
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_uint8_vec(initial_data: Vec<u8>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::UInt8Vec(initial_data),
        file_pos: file_pos,
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_uint16_vec(initial_data: Vec<u16>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::UInt16Vec(initial_data),
        file_pos: file_pos,
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_uint32_vec(initial_data: Vec<u32>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::UInt32Vec(initial_data),
        file_pos: file_pos,
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_uint64_vec(initial_data: Vec<u64>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::UInt64Vec(initial_data),
        file_pos: file_pos,
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_float32_vec(initial_data: Vec<f32>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::Float32Vec(initial_data),
        file_pos: file_pos,
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_float64_vec(initial_data: Vec<f64>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::Float64Vec(initial_data),
        file_pos: file_pos,
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_boolean_vec(initial_data: Vec<bool>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::BooleanVec(initial_data),
        file_pos: file_pos,
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_datetime32_vec(initial_data: Vec<i32>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::DateTime32Vec(initial_data),
        file_pos: file_pos,
        encoding: encoding,
        compression: compression,
        buffer: None,
    }
  }

  fn new_datetime64_vec(initial_data: Vec<i64>, file_pos: usize, encoding: EnumDataEnc, compression: EnumDataComp) -> Self {
    SegmentColumnData {
        data: EnumColumnData::DateTime64Vec(initial_data),
        file_pos: file_pos,
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
          byteorder::WriteBytesExt::write_i8(&mut buffer, value)?;
        }
      },
      EnumColumnData::Int16Vec(data) => {
        for &value in data {
          byteorder::WriteBytesExt::write_i16::<byteorder::LittleEndian>(&mut buffer, value)?;
        }
      },
      EnumColumnData::Int32Vec(data) => {
        for &value in data {
          byteorder::WriteBytesExt::write_i32::<byteorder::LittleEndian>(&mut buffer, value)?;
        }
      },
      EnumColumnData::Int64Vec(data) => {
        for &value in data {
          byteorder::WriteBytesExt::write_i64::<byteorder::LittleEndian>(&mut buffer, value)?;
        }
      },
      EnumColumnData::UInt8Vec(data) => {
        for &value in data {
          byteorder::WriteBytesExt::write_u8(&mut buffer, value)?;
        }
      },
      EnumColumnData::UInt16Vec(data) => {
        for &value in data {
          byteorder::WriteBytesExt::write_u16::<byteorder::LittleEndian>(&mut buffer, value)?;
        }
      },
      EnumColumnData::UInt32Vec(data) => {
        for &value in data {
          byteorder::WriteBytesExt::write_u32::<byteorder::LittleEndian>(&mut buffer, value)?;
        }
      },
      EnumColumnData::UInt64Vec(data) => {
        for &value in data {
          byteorder::WriteBytesExt::write_u64::<byteorder::LittleEndian>(&mut buffer, value)?;
        }
      },
      EnumColumnData::Float32Vec(data) => {
        for &value in data {
          byteorder::WriteBytesExt::write_f32::<byteorder::LittleEndian>(&mut buffer, value)?;
        }
      },
      EnumColumnData::Float64Vec(data) => {
        for &value in data {
          byteorder::WriteBytesExt::write_f64::<byteorder::LittleEndian>(&mut buffer, value)?;
        }
      },
      EnumColumnData::BooleanVec(data) => {
        for &value in data {
          // Convert bool to u8 (true -> 255, false -> 0)
          let byte_value: u8 = if value { 255u8 } else { 0u8 };
          byteorder::WriteBytesExt::write_u8(&mut buffer, byte_value)?;
        }
      },
      EnumColumnData::DateTime32Vec(data) => {
        for &value in data {
          byteorder::WriteBytesExt::write_i32::<byteorder::LittleEndian>(&mut buffer, value)?;
        }
      },
      EnumColumnData::DateTime64Vec(data) => {
        for &value in data {
          byteorder::WriteBytesExt::write_i64::<byteorder::LittleEndian>(&mut buffer, value)?;
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

        while let Ok(value) = byteorder::ReadBytesExt::read_i8(&mut cursor) {
          data_vec.push(value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_i8(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::Int16Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = byteorder::ReadBytesExt::read_i16::<byteorder::LittleEndian>(&mut cursor) {
          data_vec.push(value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_i16::<byteorder::LittleEndian>(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::Int32Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = byteorder::ReadBytesExt::read_i32::<byteorder::LittleEndian>(&mut cursor) {
          data_vec.push(value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_i32::<byteorder::LittleEndian>(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::Int64Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = byteorder::ReadBytesExt::read_i64::<byteorder::LittleEndian>(&mut cursor) {
          data_vec.push(value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_i64::<byteorder::LittleEndian>(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::UInt8Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = byteorder::ReadBytesExt::read_u8(&mut cursor) {
          data_vec.push(value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_u8(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::UInt16Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = byteorder::ReadBytesExt::read_u16::<byteorder::LittleEndian>(&mut cursor) {
          data_vec.push(value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_u16::<byteorder::LittleEndian>(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::UInt32Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = byteorder::ReadBytesExt::read_u32::<byteorder::LittleEndian>(&mut cursor) {
          data_vec.push(value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_u32::<byteorder::LittleEndian>(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::UInt64Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = byteorder::ReadBytesExt::read_u64::<byteorder::LittleEndian>(&mut cursor) {
          data_vec.push(value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_u64::<byteorder::LittleEndian>(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::Float32Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = byteorder::ReadBytesExt::read_f32::<byteorder::LittleEndian>(&mut cursor) {
          data_vec.push(value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_f32::<byteorder::LittleEndian>(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::Float64Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = byteorder::ReadBytesExt::read_f64::<byteorder::LittleEndian>(&mut cursor) {
          data_vec.push(value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_f64::<byteorder::LittleEndian>(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::BooleanVec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = byteorder::ReadBytesExt::read_u8(&mut cursor) {
          // Convert bool to u8 (true -> 255, false -> 0)
          let bool_value: bool = if value == 0u8 { false } else { true };
          data_vec.push(bool_value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_u8(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::DateTime32Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = byteorder::ReadBytesExt::read_i32::<byteorder::LittleEndian>(&mut cursor) {
          data_vec.push(value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_i32::<byteorder::LittleEndian>(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
      EnumColumnData::DateTime64Vec(data_vec) => {
        data_vec.clear();

        while let Ok(value) = byteorder::ReadBytesExt::read_i64::<byteorder::LittleEndian>(&mut cursor) {
          data_vec.push(value);
        }

        if let Err(e) = byteorder::ReadBytesExt::read_i64::<byteorder::LittleEndian>(&mut cursor) {
          if e.kind() != io::ErrorKind::UnexpectedEof {
            return Err(e);
          }
        }
      },
    }

    Ok(())
  }

  pub async fn write_buffer_into_file(&self, file: &mut File) -> io::Result<()> {
    trace!("SegmentColumnData::write_buffer_into_file");
    
    if let Some(ref buffer) = self.buffer {
      let current_position: usize = file.seek(SeekFrom::Current(0)).await? as usize;
      if current_position != self.file_pos {
        file.seek(SeekFrom::Start(self.file_pos as u64)).await?;
      }
      file.write_all(buffer).await?;
    } else {
      return Err(io::Error::new(io::ErrorKind::Other, "Data not prepared"));
    }

    Ok(())
  }

  pub async fn read_file_into_buffer(&mut self, file: &mut File, bytes: usize) -> io::Result<()> {
    trace!("SegmentColumnData::read_file_into_buffer");

    let current_position: usize = file.seek(SeekFrom::Current(0)).await? as usize;
    if current_position != self.file_pos {
      file.seek(SeekFrom::Start(self.file_pos as u64)).await?;
    }

    // Prepare the buffer
    let mut buffer: Vec<u8> = vec![0u8; bytes];
    file.read_exact(&mut buffer).await?;
    
    self.buffer = Some(buffer);

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
  use tempfile::tempfile;
  use tokio::fs::File;

  #[tokio::test]
  async fn test_prepare_and_write_int8_data() -> io::Result<()> {
    let mut segment_data: SegmentColumnData = SegmentColumnData::new_int8_vec(
      vec![1, 2, -3, -4],
      0,
      EnumDataEnc::None,
      EnumDataComp::None
    );
    let prepare_bytes: usize = segment_data.convert_data_into_buffer()?;
    assert_eq!(prepare_bytes, 4);

    // Convert a tempfile::File to a tokio::fs::File for async operations
    let temp_file: std::fs::File = tempfile()?;
    let mut temp_file: File = File::from_std(temp_file);

    segment_data.write_buffer_into_file(&mut temp_file).await?;

    // Seek back to beginning
    temp_file.seek(SeekFrom::Start(0)).await?;

    // Read back the data to verify
    let mut read_buffer = vec![0u8; prepare_bytes]; // Prepare a buffer with the size of data written
    temp_file.read_exact(&mut read_buffer).await?;
    assert_eq!(read_buffer, vec![1u8, 2, 253, 252]);

    Ok(())
  }

  #[tokio::test]
  async fn test_read_int8_data() -> io::Result<()> {
    // Initialize a temporary file with tokio::fs::File for async write
    let temp_file: std::fs::File = tempfile()?;
    let mut temp_file: File = File::from_std(temp_file);

    // Prepare a buffer with int8 data and write it to a temporary file
    let data: Vec<u8> = vec![1u8, 2, 253, 252];
    temp_file.write_all(&data).await?;

    // Ensure the file cursor is set to the start before reading
    temp_file.seek(SeekFrom::Start(0)).await?;

    // Initialize SegmentColumnData for async read
    let mut segment_data: SegmentColumnData = SegmentColumnData::new_int8_vec(
      Vec::new(),
      0,
      EnumDataEnc::None,
      EnumDataComp::None
    );
    segment_data.read_file_into_buffer(&mut temp_file, data.len()).await?;

    // Verify the buffer matches the original data
    match segment_data.buffer {
      Some(buffer) => assert_eq!(buffer, data),
      None => panic!("Buffer was not populated"),
    }

    Ok(())
  }
}
