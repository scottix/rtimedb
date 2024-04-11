use std::io::{self};

use tokio::fs::File;
use tracing::trace;
use uuid7;

use super::async_column_data::SegmentColumnData;
use super::async_data_header::{SegmentColumnHeader, SegmentDataHeader};
use super::types::EnumColumnData;

#[repr(C)]
pub struct SegmentData {
  data_header: SegmentDataHeader,
  data: Vec<SegmentColumnData>,
  data_pos: usize,
}

impl SegmentData {
  pub fn new() -> Self {
    let data_header: SegmentDataHeader = SegmentDataHeader::new();
    SegmentData {
        data_header,
        data: vec![],
        data_pos: 0,
    }
  }

  pub fn start_tx(mut self) -> Self {
    trace!("SegmentData::start_tx");

    let txid: uuid7::Uuid = uuid7::uuid7();
    self.data_header.uuid_txid = Some(*txid.as_bytes());

    self
  }

  pub fn get_column_count(&self) -> usize {
    trace!("SegmentData::get_column_count");

    self.data_header.column_count as usize
  }

  pub fn get_row_count(&self) -> usize {
    trace!("SegmentData::get_column_count");

    self.data_header.row_count as usize
  }

  pub fn get_column_data_pos(&self) -> usize {
    // File Header + Size of Header + data_position
    return 6 + self.data_header.calculate_header_size() + self.data_pos;
  }

  pub fn get_segment_data<'a>(&'a self, index: usize) -> Option<&'a SegmentColumnData> {
    trace!("SegmentData::get_segment_data");

    self.data.get(index)
  }

  pub fn add_column_header(&mut self, column_header: SegmentColumnHeader, ts_column: bool) -> Result<(), String> {
    trace!("SegmentData::add_column_header");

    let index: u16 = self.data_header.add_column_header(column_header);
    if ts_column {
      self.data_header.set_ts_column(index)?;
    }

    Ok(())
  }

  pub fn add_column_data(&mut self, data: SegmentColumnData) -> Result<(), String> {
    trace!("SegmentData::add_column_data");

    // Check to make sure we are not adding more data that is not defined
    if self.data_header.column_count as usize <= self.data.len() {
      return Err("No corresponding column header for the data.".to_string());
    }

    let data_row_count: usize = match &data.data {
      EnumColumnData::Int8Vec(vec) => vec.len(),
      EnumColumnData::Int16Vec(vec) => vec.len(),
      EnumColumnData::Int32Vec(vec) => vec.len(),
      // @TODO Add cases for other data types...
      _ => 0,
    };

    // Can't add empty rows
    if data_row_count == 0 {
      return Err("Zero rows added".to_string());
    }

    // All data needs to have the same number of rows
    if self.data_header.row_count != 0 {
      if self.data_header.row_count as usize != data_row_count {
        return Err("Inconsistent number of rows.".to_string());
      }
    } else {
      self.data_header.row_count = data_row_count as u32;
    }

    // Directly append the provided SegmentColumnData instance to the data vector.
    self.data.push(data);

    Ok(())
  }

  pub fn update_header_dates(&mut self, date_start: i64, date_end: i64) {
    self.data_header.set_date_start(date_start);
    self.data_header.set_date_end(date_end);
  }

  // Writes the SegmentData to a file, including the header and data.
  pub async fn write_to_file(&mut self, file: &mut File) -> io::Result<()> {
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
    self.data_header.next_offset = Some((self.data_header.calculate_header_size() + total_data_size) as u32);

    // Now, write the header to the file.
    self.data_header.write_header(file).await?;

    // Write each column's data from its prepared buffer to the file.
    for column_data in &self.data {
      column_data.write_buffer_into_file(file).await?;
    }

    Ok(())
  }

  // Reads SegmentData from a file, reconstructing the header and data.
  pub async fn read_segment_from_file(&mut self, file: &mut File) -> io::Result<()> {
    trace!("SegmentData::read_segment_from_file");

    self.data_header.read_segment_header(file).await?;
    self.read_segment_data(file).await?;

    Ok(())
  }

  async fn read_segment_data(&mut self, file: &mut File) -> io::Result<()> {
    trace!("SegmentData::read_segment_data");

    // Ensure the data vector is clear
    self.data.clear();

    let mut column_data_pos: usize = self.get_column_data_pos();

    // This reads all the columns
    for header in &self.data_header.column_headers {
      let mut column_data: SegmentColumnData = SegmentColumnData::new(
        header.column_type,
        column_data_pos,
        header.column_enc,
        header.column_comp,
      );
      column_data.read_file_into_buffer(file, header.column_size as usize).await?;
      column_data.convert_buffer_into_data()?;
      self.data.push(column_data);
      column_data_pos = column_data_pos + header.column_size as usize;
    }

    Ok(())
  }
}
