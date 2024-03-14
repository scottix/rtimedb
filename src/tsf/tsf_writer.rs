use std::{fs::{File, OpenOptions}, io, path::Path};

use super::header::FileHeader;
use super::segments::data::{ColumnDataCreator, SegmentData, SegmentColumnHeader, SegmentColumnData, EnumDataType, EnumDataEnc, EnumDataComp};

pub struct TSFWriter {
  file: File,
  file_header: FileHeader,
  segment_data: SegmentData,
}

impl TSFWriter {
  pub fn new(file_path: &str) -> io::Result<Self> {
    let file: File = OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .open(Path::new(file_path))?;

    let file_header: FileHeader = FileHeader::new();
    let segment_data: SegmentData = SegmentData::new()
      .start_tx();

    Ok(TSFWriter {
      file,
      file_header,
      segment_data,
    })
  }

  pub fn add_column_header(&mut self, column_name: &str, column_type: EnumDataType, encoding: EnumDataEnc, compression: EnumDataComp) -> io::Result<()> {
    let header: SegmentColumnHeader = SegmentColumnHeader::new(
      column_name.to_string(),
      column_type,
      encoding,
      compression,
    );
    
    self.segment_data.add_column_header(header);

    Ok(())
  }

  pub fn add_column_data<T>(&mut self, column: Vec<T>, encoding: EnumDataEnc, compression: EnumDataComp) -> io::Result<()>
  where
      T: ColumnDataCreator + Sized,
  {
      if column.is_empty() {
          return Err(io::Error::new(io::ErrorKind::InvalidInput, "Column data empty"));
      }

      // Using the trait to create the SegmentColumnData
      let data_segment: SegmentColumnData = T::create_segment_column_data(column, encoding, compression);
      self.segment_data.add_column_data(data_segment)?;

      Ok(())
  }

  // Save the SegmentData to the file
  pub fn save(&mut self) -> io::Result<()> {
    // Assume new file for now
    self.file_header.write_header(&mut self.file)?;
    self.segment_data.write_to_file(&mut self.file)?;
    Ok(())
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
        writer.add_column_header("temperature", EnumDataType::Int8, EnumDataEnc::None, EnumDataComp::None)?;
        assert_eq!(writer.segment_data.get_column_count(), 1);

        Ok(())
    }

    #[test]
    fn test_add_column_data_and_save() -> io::Result<()> {
        let temp_file: NamedTempFile = NamedTempFile::new()?;
        let file_path: &str = temp_file.path().to_str().unwrap();

        let mut writer: TSFWriter = TSFWriter::new(file_path)?;
        writer.add_column_header("temperature", EnumDataType::Int8, EnumDataEnc::None, EnumDataComp::None)?;

        let column_data: Vec<i8> = vec![20, 22, 21, 23];
        writer.add_column_data(column_data, EnumDataEnc::None, EnumDataComp::None)?;

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
