use std::{fs::{File, OpenOptions}, io, path::Path};

use futures::stream::BoxStream;
use tokio_stream::StreamExt;
use tracing::trace;

use super::header::FileHeader;
use super::segments::{segment_data::SegmentData, types::{EnumColumnData, EnumDataValue}};

#[derive(Debug)]
pub struct DataRow {
  pub values: Vec<EnumDataValue>
}

pub struct TSFReader {
  file: File,
  file_header: FileHeader,
  segment_data: SegmentData,
}

impl TSFReader {
  pub fn new(file_path: &str) -> io::Result<Self> {
    let file: File = OpenOptions::new()
      .read(true)
      .open(Path::new(file_path))?;

    let file_header: FileHeader = FileHeader::new();
    let segment_data: SegmentData = SegmentData::new();

    Ok(TSFReader {
      file,
      file_header,
      segment_data,
    })
  }

  pub fn stream_rows(&self) -> BoxStream<'static, io::Result<DataRow>> {
    let num_rows: usize = self.segment_data.get_row_count();
    
    let mut rows: Vec<Result<DataRow, io::Error>> = Vec::new();

    for row_index in 0..num_rows {
      let mut row_values: Vec<EnumDataValue> = Vec::new();

      // Assuming you have a way to iterate over each column index
      for column_index in 0..self.segment_data.get_column_count() {
        if let Some(column) = self.segment_data.get_segment_data(column_index) {
          if let Some(data) = column.get_data() {
            match data {
              EnumColumnData::Int8Vec(v) => {
                if row_index < v.len() {
                    row_values.push(EnumDataValue::Int8Value(v[row_index]));
                }
              },
              EnumColumnData::Int16Vec(v) => {
                if row_index < v.len() {
                    row_values.push(EnumDataValue::Int16Value(v[row_index]));
                }
              },
              EnumColumnData::Int32Vec(v) => {
                if row_index < v.len() {
                    row_values.push(EnumDataValue::Int32Value(v[row_index]));
                }
              },
              EnumColumnData::Int64Vec(v) => {
                if row_index < v.len() {
                    row_values.push(EnumDataValue::Int64Value(v[row_index]));
                }
              },
              _ => return Box::pin(tokio_stream::iter(vec![Err(io::Error::new(io::ErrorKind::Other, "EnumColumnData not implemented"))])),
            }
          }
        } else {
          // Handle the case where column data is missing
          return Box::pin(tokio_stream::iter(vec![Err(io::Error::new(io::ErrorKind::Other, "Column data missing"))]));
        }
      }

      // Create a DataRow for each row of values
      rows.push(Ok(DataRow { values: row_values }));
    }

    Box::pin(tokio_stream::iter(rows))
  }

  pub fn read_all(&mut self) -> io::Result<()> {
    trace!("TSFReader::read_all");
    self.read_header()?;
    self.read_data()?;
    Ok(())
  }

  pub fn read_header(&mut self) -> io::Result<()> {
    trace!("TSFReader::read_header");
    self.file_header.read_header(&mut self.file)?;

    if !self.file_header.verify_header() {
      return Err(io::Error::new(io::ErrorKind::InvalidData, "File header verification failed"));
    }

    Ok(())
  }

  pub fn read_data(&mut self) -> io::Result<()> {
    trace!("TSFReader::read_data");
    self.segment_data.read_segment_from_file(&mut self.file)?;
    Ok(())
  }
}
