use std::{fs::{self, File, OpenOptions}, io, path::PathBuf};

use super::header::FileHeader;
use super::segments::{data::{ColumnDataCreator, SegmentData, SegmentColumnHeader, SegmentColumnData}, types::{EnumDataType, EnumDataEnc, EnumDataComp}};

pub struct TSFWriter {
  file: File,
  file_path: PathBuf,
  file_exists: bool,
  file_header: FileHeader,
  segment_data: SegmentData,
  cleanup: bool,
}

impl TSFWriter {
  pub fn new(path: &str) -> io::Result<Self> {
    let path_buf: PathBuf = PathBuf::from(path);
    let file_exists: bool = path_buf.exists();

    let file: File = if file_exists {
      OpenOptions::new()
        .append(true)
        .open(&path_buf)?
    } else {
      OpenOptions::new()
        .create(true)
        .write(true)
        .open(&path_buf)?
    };

    let file_header: FileHeader = FileHeader::new();
    let segment_data: SegmentData = SegmentData::new()
      .start_tx();

    Ok(TSFWriter {
      file,
      file_path: path_buf,
      file_exists,
      file_header,
      segment_data,
      cleanup: false,
    })
  }

  pub fn add_column_header(&mut self, column_name: &str, column_type: EnumDataType, encoding: EnumDataEnc, compression: EnumDataComp, ts_column: bool) -> Result<(), String> {
    let header: SegmentColumnHeader = SegmentColumnHeader::new(
      column_name.to_string(),
      column_type,
      encoding,
      compression,
    );
    
    self.segment_data.add_column_header(header, ts_column)?;

    Ok(())
  }

  pub fn add_column_data<T>(&mut self, column: Vec<T>, encoding: EnumDataEnc, compression: EnumDataComp) -> Result<(), String>
  where
      T: ColumnDataCreator + Sized,
  {
      if column.is_empty() {
          return Err("Column data empty".to_string());
      }

      // Using the trait to create the SegmentColumnData
      let data_segment: SegmentColumnData = T::create_segment_column_data(column, encoding, compression);
      self.segment_data.add_column_data(data_segment)?;

      Ok(())
  }

  pub fn update_segment_dates(&mut self, date_start: i64, date_end: i64) {
    self.segment_data.update_header_dates(date_start, date_end);
  }

  pub fn try_save(&mut self) -> io::Result<()> {
    self.cleanup = false;
    if let Err(e) = self.save() {
      self.cleanup = true;
      return Err(e);
    }
    Ok(())
  }

  // Save the SegmentData to the file
  fn save(&mut self) -> io::Result<()> {
    self.file_header.write_header(&mut self.file)?;
    self.segment_data.write_to_file(&mut self.file)?;
    Ok(())
  }
}

impl Drop for TSFWriter {
  fn drop(&mut self) {
    if self.cleanup && !self.file_exists {
      let _ = fs::remove_file(&self.file_path);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::Read;
  use tempfile::NamedTempFile;

  #[test]
  fn test_tsf_writer_new() -> io::Result<()> {
    let temp_file: NamedTempFile = NamedTempFile::new()?;
    let file_path: &str = temp_file.path().to_str().unwrap();

    let writer_result: Result<TSFWriter, io::Error> = TSFWriter::new(file_path);
    assert!(writer_result.is_ok());

    Ok(())
  }

  #[test]
  fn test_add_column_header() -> io::Result<()> {
    let temp_file: NamedTempFile = NamedTempFile::new()?;
    let file_path: &str = temp_file.path().to_str().unwrap();

    let mut writer: TSFWriter = TSFWriter::new(file_path)?;
    writer.add_column_header("temperature", EnumDataType::Int8, EnumDataEnc::None, EnumDataComp::None, false)
      .map_err(|e: String| io::Error::new(io::ErrorKind::InvalidData, e))?;
    assert_eq!(writer.segment_data.get_column_count(), 1);

    Ok(())
  }

  #[test]
  fn test_add_column_data_and_save() -> io::Result<()> {
    let temp_file: NamedTempFile = NamedTempFile::new()?;
    let file_path: &str = temp_file.path().to_str().unwrap();

    let mut writer: TSFWriter = TSFWriter::new(file_path)?;
    writer.add_column_header("metric_time", EnumDataType::DateTime32, EnumDataEnc::None, EnumDataComp::None, true)
      .map_err(|e: String| io::Error::new(io::ErrorKind::InvalidData, e))?;
    writer.add_column_header("temperature", EnumDataType::Int8, EnumDataEnc::None, EnumDataComp::None, false)
      .map_err(|e: String| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let time_data: Vec<i32> = vec![1710555318, 1710555319, 1710555320, 1710555321];
    writer.add_column_data(time_data, EnumDataEnc::None, EnumDataComp::None)
      .map_err(|e: String| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let column_data: Vec<i8> = vec![20, 22, 21, 23];
    writer.add_column_data(column_data, EnumDataEnc::None, EnumDataComp::None)
      .map_err(|e: String| io::Error::new(io::ErrorKind::InvalidData, e))?;

    writer.update_segment_dates(1710555318, 1710555321);

    writer.save()?;

    // Open the file again and verify its contents
    let mut file: File = File::open(file_path)?;
    let mut contents: Vec<u8> = Vec::new();
    file.read_to_end(&mut contents)?;

    // Check that the file isn't empty, for a more detailed check, 
    // you'll need to deserialize the data and compare
    assert!(!contents.is_empty());

    Ok(())
  }
}
