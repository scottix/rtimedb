use std::{fs::{File, OpenOptions}, io, path::Path};

use tracing::debug;

use super::header::FileHeader;
use super::segments::data::{EnumColumnData, SegmentData};

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

  pub fn read_all(&mut self) -> io::Result<()> {
    self.read_header()?;
    self.read_data()?;
    Ok(())
  }

  pub fn read_header(&mut self) -> io::Result<()> {
    self.file_header.read_header(&mut self.file)?;

    if !self.file_header.verify_header() {
      return Err(io::Error::new(io::ErrorKind::InvalidData, "File header verification failed"));
    }

    Ok(())
  }

  pub fn read_data(&mut self) -> io::Result<()> {
    debug!("read_data");
    self.segment_data.read_segment_from_file(&mut self.file)?;

    let mut sum: i32 = 0;

    if let Some(column) = self.segment_data.get_segment_data(0) {
      match &column.data {
        EnumColumnData::Int8Vec(data) => {
          for value in data {
              // println!("{}", value);
              sum += *value as i32;
          }
        },
        EnumColumnData::Int16Vec(data) => {
          for value in data {
            println!("{}", value);
          }
        },
        EnumColumnData::Int32Vec(data) => {
            for value in data {
                println!("{}", value);
            }
        },
        EnumColumnData::Int64Vec(data) => {
            for value in data {
                println!("{}", value);
            }
        },
        EnumColumnData::UInt8Vec(data) => {
            for value in data {
                println!("{}", value);
            }
        },
        EnumColumnData::UInt16Vec(data) => {
            for value in data {
                println!("{}", value);
            }
        },
        EnumColumnData::UInt32Vec(data) => {
            for value in data {
                println!("{}", value);
            }
        },
        EnumColumnData::UInt64Vec(data) => {
            for value in data {
                println!("{}", value);
            }
        },
        EnumColumnData::Float32Vec(data) => {
            for value in data {
                println!("{}", value);
            }
        },
        EnumColumnData::Float64Vec(data) => {
            for value in data {
                println!("{}", value);
            }
        },
        EnumColumnData::BooleanVec(data) => {
            for value in data {
                println!("{}", value);
            }
        },
        EnumColumnData::DateTime32Vec(data) => {
          for value in data {
              println!("{}", value);
          }
        },
        EnumColumnData::DateTime64Vec(data) => {
            for value in data {
                println!("{}", value);
            }
        },
      }
    }

    println!("{}", sum);

    Ok(())
  }
}
