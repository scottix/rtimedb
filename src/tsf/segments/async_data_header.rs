use std::io::{self, Cursor};

use byteorder::{ByteOrder, LittleEndian};
use tokio::{fs::File, io::{AsyncReadExt, AsyncWriteExt}};
use tracing::trace;

use super::types::{ColumnMeta, EnumDataComp, EnumDataEnc, EnumDataType};

#[repr(C)]
pub struct SegmentDataHeader {
  pub tombstone: bool,
  pub next_offset: Option<u32>,
  pub uuid_txid: Option<[u8; 16]>,
  date_start: Option<i64>,
  date_end: Option<i64>,
  pub row_count: u32,
  pub column_count: u16,
  ts_column: Option<u16>,
  pub column_header_size: u32,
  pub column_headers: Vec<SegmentColumnHeader>,
  segment_check: Option<[u8; 8]>,
}

impl SegmentDataHeader {
  pub fn new() -> Self {
    SegmentDataHeader {
      tombstone: false,
      next_offset: None,
      uuid_txid: None,
      date_start: None,
      date_end: None,
      row_count: 0,
      column_count: 0,
      ts_column: None,
      column_header_size: 0,
      column_headers: vec![],
      segment_check: None,
    }
  }

  pub fn add_column_header(&mut self, column_header: SegmentColumnHeader) -> u16 {
    trace!("SegmentDataHeader::add_column_header");

    self.column_headers.push(column_header);
    self.column_count = self.column_headers.len() as u16;
    self.column_header_size = self.column_headers.iter()
      .map(|header| header.byte_size())
      .sum();

    let new_column_index: u16 = self.column_count - 1;

    new_column_index
  }

  pub fn set_ts_column(&mut self, ts_column_index: u16) -> Result<(), String> {
    trace!("SegmentDataHeader::set_ts_column");

    if ts_column_index as usize >= self.column_headers.len() {
      return Err("Timestamp column index out of bounds.".to_string());
    }

    if self.ts_column.is_some() {
      return Err("Timestamp column already set.".to_string());
    }

    self.ts_column = Some(ts_column_index);
    Ok(())
  }

  pub fn set_date_start(&mut self, date_start: i64) {
    self.date_start = Some(date_start);
  }

  pub fn set_date_end(&mut self, date_end: i64) {
    self.date_end = Some(date_end);
  }

  pub fn calculate_header_size(&self) -> usize {
    trace!("SegmentDataHeader::calculate_header_size");

    // Fixed size parts: 1 (tombstone) + 4 (next_offset) + 16 (uuid_txid) + 8 (date_start) + 8 (date_end) + 
    // 4 (row_count) + 2 (column_count) + 2 (ts_column) + 4 (column_header_size) + 8 (segment_check)
    let fixed_size: usize = 1 + 4 + 16 + 8 + 8 + 4 + 2 + 2 + 4 + 8;

    fixed_size + self.column_header_size as usize
  }

  fn calculate_checksum(&self) -> [u8; 8] {
    // @TODO xxhash64
    let dummy_checksum: [u8; 8] = [0xBB; 8]; // Placeholder checksum value
    dummy_checksum
  }

  fn update_segment_check(&mut self) {
    // @TODO update segment_check
    self.segment_check = Some(self.calculate_checksum());
  }

  fn verify_segment_check(&self) -> bool {
    // @TODO add checker
    return true;
  }

  pub async fn write_header(&mut self, file: &mut File) -> io::Result<()> {
    trace!("SegmentDataHeader::write_header");

    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(self.tombstone as u8);

    match self.next_offset {
      Some(next_offset) => byteorder::WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, next_offset)?,
      None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "next_offset was not set")),
    }

    match self.uuid_txid {
      Some(uuid_txid) => buffer.extend_from_slice(&uuid_txid),
      None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "uuid_txid was not set")),
    }

    match self.date_start {
      Some(date_start) => byteorder::WriteBytesExt::write_i64::<LittleEndian>(&mut buffer, date_start)?,
      None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "date_start was not set")),
    }

    match self.date_end {
      Some(date_end) => byteorder::WriteBytesExt::write_i64::<LittleEndian>(&mut buffer, date_end)?,
      None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "date_end was not set")),
    }

    byteorder::WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, self.row_count)?;
    byteorder::WriteBytesExt::write_u16::<LittleEndian>(&mut buffer, self.column_count)?;

    match self.ts_column {
      Some(ts_column) => byteorder::WriteBytesExt::write_u16::<LittleEndian>(&mut buffer, ts_column)?,
      None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "ts_column was not set")),
    }

    // Serialize and write each column header, keeping track of the total size
    let mut column_headers_buffer: Vec<u8> = Vec::new();
    for column_header in &self.column_headers {
      let column_header_buf: Vec<u8> = column_header.prepare_buffer()?;
      column_headers_buffer.extend(column_header_buf);
    }

    // Update and write the column_header_size
    let column_header_size: u32 = column_headers_buffer.len() as u32;
    byteorder::WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, column_header_size)?;

    // Append the serialized column headers
    buffer.extend_from_slice(&column_headers_buffer);

    self.update_segment_check();

    // Writes the segment check
    match self.segment_check {
      Some(segment_check) => buffer.extend_from_slice(&segment_check),
      None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "segment_check was not set")),
    }

    // Write the entire buffer to the file in one go
    file.write_all(&buffer).await?;

    Ok(())
  }

  pub async fn read_segment_header(&mut self, file: &mut File) -> io::Result<()> {
    let mut header_buffer: Vec<u8> = vec![0; 49]; // Fixed size for the header
    file.read_exact(&mut header_buffer).await?;

    let cursor = Cursor::new(header_buffer);
    
    // Correct usage of byteorder for synchronous in-memory operations
    self.tombstone = cursor.get_ref()[0] != 0;
    
    self.next_offset = Some(LittleEndian::read_u32(&cursor.get_ref()[1..5]));
    
    let mut uuid_txid_arr = [0u8; 16];
    uuid_txid_arr.copy_from_slice(&cursor.get_ref()[5..21]);
    self.uuid_txid = Some(uuid_txid_arr);

    self.date_start = Some(LittleEndian::read_i64(&cursor.get_ref()[21..29]));
    self.date_end = Some(LittleEndian::read_i64(&cursor.get_ref()[29..37]));
    
    self.row_count = LittleEndian::read_u32(&cursor.get_ref()[37..41]);
    self.column_count = LittleEndian::read_u16(&cursor.get_ref()[41..43]);
    
    self.ts_column = Some(LittleEndian::read_u16(&cursor.get_ref()[43..45]));
    
    self.column_header_size = LittleEndian::read_u32(&cursor.get_ref()[45..49]);

    // Now read the dynamic part: column headers + segment check
    let header_size: usize = self.column_header_size as usize + 8; // +8 for segment check

    let mut dynamic_buffer: Vec<u8> = vec![0; header_size];
    file.read_exact(&mut dynamic_buffer).await?;

    let mut dynamic_cursor: Cursor<Vec<u8>> = Cursor::new(dynamic_buffer);

    self.column_headers.clear();
    for _ in 0..self.column_count {
      let column_header: SegmentColumnHeader = SegmentColumnHeader::read_from_buffer(&mut dynamic_cursor)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
      self.column_headers.push(column_header);
    }

    // Assuming segment check is the last 8 bytes
    let mut segment_check_arr: [u8; 8] = [0; 8];
    segment_check_arr.copy_from_slice(&dynamic_cursor.get_ref()[(header_size - 8)..]);
    self.segment_check = Some(segment_check_arr);

    Ok(())
  }
}

pub struct SegmentColumnHeader {
  column_name_length: u16,
  pub column_name: String,
  pub column_type: EnumDataType,
  column_meta_length: u16,
  column_meta: ColumnMeta,
  pub column_enc: EnumDataEnc,
  pub column_comp: EnumDataComp,
  pub column_size: u64,
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

  pub fn byte_size(&self) -> u32 {
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

    // Prepare a new buffer
    let mut buffer: Vec<u8> = Vec::new();

    // Write column name length and column name
    let _ = byteorder::WriteBytesExt::write_u16::<LittleEndian>(&mut buffer, self.column_name_length);
    buffer.extend_from_slice(self.column_name.as_bytes());

    // Write column type
    let _ = byteorder::WriteBytesExt::write_u16::<LittleEndian>(&mut buffer, self.column_type as u16);

    // Write column meta length
    let _ = byteorder::WriteBytesExt::write_u16::<LittleEndian>(&mut buffer, self.column_meta_length as u16);

    // Assuming column_meta is serialized here. For simplicity, skipping actual serialization
    // You might need to serialize `column_meta` based on its type and content

    // Write column_enc and column_comp
    let column_enc_val: u8 = self.column_enc as u8;
    buffer.push(column_enc_val);

    let column_comp_val: u8 = self.column_comp as u8;
    buffer.push(column_comp_val);

    // Write column size
    let _ = byteorder::WriteBytesExt::write_u64::<LittleEndian>(&mut buffer, self.column_size);

    // Write column check
    buffer.extend_from_slice(&self.column_check);

    Ok(buffer)
  }

  fn read_from_buffer(cursor: &mut Cursor<Vec<u8>>) -> Result<Self, String> {
    // Directly use byteorder's ReadBytesExt methods on Cursor
    let column_name_length = byteorder::ReadBytesExt::read_u16::<LittleEndian>(cursor)
      .map_err(|_| "Failed to read column name length".to_string())?;

    let mut column_name_bytes = vec![0u8; column_name_length as usize];
    io::Read::read_exact(cursor, &mut column_name_bytes)
      .map_err(|_| "Failed to read column name".to_string())?;

    let column_name = String::from_utf8(column_name_bytes)
      .map_err(|_| "Failed to decode column name".to_string())?;

    let column_type = byteorder::ReadBytesExt::read_u16::<LittleEndian>(cursor)
      .map_err(|_| "Failed to read column type".to_string())?;
    let column_meta_length = byteorder::ReadBytesExt::read_u16::<LittleEndian>(cursor)
      .map_err(|_| "Failed to read column meta length".to_string())?;

    let column_enc = byteorder::ReadBytesExt::read_u8(cursor)
      .map_err(|_| "Failed to read column encoding".to_string())?;
    let column_comp = byteorder::ReadBytesExt::read_u8(cursor)
      .map_err(|_| "Failed to read column compression".to_string())?;
    let column_size = byteorder::ReadBytesExt::read_u64::<LittleEndian>(cursor)
      .map_err(|_| "Failed to read column size".to_string())?;

    let mut column_check = [0u8; 8];
    io::Read::read_exact(cursor, &mut column_check)
      .map_err(|_| "Failed to read column check".to_string())?;

    // Return your SegmentColumnHeader struct initialization
    Ok(SegmentColumnHeader {
      column_name_length,
      column_name,
      column_type: EnumDataType::from_u16(column_type).ok_or_else(|| "Invalid column type".to_string())?,
      column_meta_length,
      // Assuming default for demonstration; replace as necessary
      column_meta: Default::default(), 
      column_enc: EnumDataEnc::from_u8(column_enc).ok_or_else(|| "Invalid encoding type".to_string())?,
      column_comp: EnumDataComp::from_u8(column_comp).ok_or_else(|| "Invalid compression type".to_string())?,
      column_size,
      column_check,
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
  use tempfile::tempfile;
  use tokio::fs::File;

  #[tokio::test]
  async fn test_write_and_read_header() -> io::Result<()> {
    // Setup: Create a SegmentDataHeader with test data
    let mut header: SegmentDataHeader = SegmentDataHeader {
      tombstone: true,
      next_offset: Some(123),
      uuid_txid: Some([0xAA; 16]),
      date_start: Some(1625097600),
      date_end: Some(1627689600),
      row_count: 10,
      column_count: 5,
      ts_column: Some(3),
      column_header_size: 0, // This gets overwritten
      column_headers: vec![],
      segment_check: Some([0xBB; 8]), // This gets overwritten
    };

    // Write the header to a temporary file
    let temp_file: std::fs::File = tempfile()?;
    let mut file: File = File::from_std(temp_file);

    header.write_header(&mut file).await?;

    // Reset the file cursor to the beginning
    file.seek(SeekFrom::Start(0)).await?;

    let header_size = header.calculate_header_size() as usize;
    let mut buffer = vec![0; header_size];
    file.read_exact(&mut buffer).await?;
    let cursor = io::Cursor::new(buffer);

    // Read back the written data
    let read_tombstone = cursor.get_ref()[0];
    let read_next_offset = LittleEndian::read_u32(&cursor.get_ref()[1..5]);
    let read_uuid_txid: [u8; 16] = cursor.get_ref()[5..21].try_into().unwrap();
    let read_date_start = LittleEndian::read_i64(&cursor.get_ref()[21..29]);
    let read_date_end = LittleEndian::read_i64(&cursor.get_ref()[29..37]);
    let read_row_count = LittleEndian::read_u32(&cursor.get_ref()[37..41]);
    let read_column_count = LittleEndian::read_u16(&cursor.get_ref()[41..43]);
    let read_ts_column = LittleEndian::read_u16(&cursor.get_ref()[43..45]);
    let read_column_header_size = LittleEndian::read_u32(&cursor.get_ref()[45..49]);
    let read_segment_check: [u8; 8] = cursor.get_ref()[49..57].try_into().unwrap();

    // Verify the data read matches what was written
    assert_eq!(read_tombstone, 1u8);
    assert_eq!(read_next_offset, 123);
    assert_eq!(read_uuid_txid, [0xAA; 16]);
    assert_eq!(read_date_start, 1625097600);
    assert_eq!(read_date_end, 1627689600);
    assert_eq!(read_row_count, 10);
    assert_eq!(read_column_count, 5);
    assert_eq!(read_ts_column, 3);
    assert_eq!(read_column_header_size, 0);
    assert_eq!(read_segment_check, [0xBB; 8]);

    Ok(())
  }

  #[tokio::test]
  async fn test_prepare_buffer() -> io::Result<()> {
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
    // Use byteorder to directly manipulate bytes in the expected_buffer
    let mut bytes = [0; 2]; // For u16
    LittleEndian::write_u16(&mut bytes, header.column_name_length);
    expected_buffer.extend_from_slice(&bytes);

    expected_buffer.extend_from_slice(header.column_name.as_bytes());

    LittleEndian::write_u16(&mut bytes, header.column_type as u16);
    expected_buffer.extend_from_slice(&bytes);

    let mut bytes_meta = [0; 2]; // Reuse for column_meta_length
    LittleEndian::write_u16(&mut bytes_meta, header.column_meta_length);
    expected_buffer.extend_from_slice(&bytes_meta);

    expected_buffer.push(header.column_enc as u8);
    expected_buffer.push(header.column_comp as u8);

    let mut bytes_size = [0; 8]; // For u64
    LittleEndian::write_u64(&mut bytes_size, header.column_size);
    expected_buffer.extend_from_slice(&bytes_size);

    expected_buffer.extend_from_slice(&header.column_check);

    assert_eq!(buffer, expected_buffer, "The prepared buffer does not match the expected bytes.");

    Ok(())
  }

  #[tokio::test]
  async fn test_read_full_header() -> io::Result<()> {
    let temp_file: std::fs::File = tempfile()?;
    let mut file: File = File::from_std(temp_file);

    // Example data to write to the tempfile
    let tombstone: u8 = 1;
    let next_offset: u32 = 123; // Example offset
    let uuid_txid: [u8; 16] = [0xAA; 16]; // Example UUID
    let date_start: u64 = 1625097600;
    let date_end: u64 = 1627689600;
    let row_count: u32 = 10;
    let column_count: u16 = 0;
    let ts_column: u16 = 0;
    let column_header_size: u32 = 0;
    let segment_check: [u8; 8] = [0xBB; 8];

    let mut buf = Vec::new();
    buf.push(tombstone);
    buf.extend_from_slice(&next_offset.to_le_bytes());
    buf.extend_from_slice(&uuid_txid);
    buf.extend_from_slice(&date_start.to_le_bytes());
    buf.extend_from_slice(&date_end.to_le_bytes());
    buf.extend_from_slice(&row_count.to_le_bytes());
    buf.extend_from_slice(&column_count.to_le_bytes());
    buf.extend_from_slice(&ts_column.to_le_bytes());
    buf.extend_from_slice(&column_header_size.to_le_bytes());
    buf.extend_from_slice(&segment_check);

    // Write the buffer to the tempfile
    file.write_all(&buf).await?;

    // Reset file position to the beginning before reading
    file.seek(io::SeekFrom::Start(0)).await?;

    // Attempt to read the header back from the tempfile
    let mut header: SegmentDataHeader = SegmentDataHeader {
      tombstone: false,
      next_offset: Some(0),
      uuid_txid: Some([0; 16]),
      date_start: Some(0),
      date_end: Some(0),
      row_count: 0,
      column_count: 0,
      ts_column: Some(0),
      column_header_size: 0,
      column_headers: Vec::new(),
      segment_check: Some([0; 8]),
    };
    
    header.read_segment_header(&mut file).await?;

    // Perform assertions
    assert_eq!(header.tombstone, true);
    assert_eq!(header.next_offset, Some(123));
    assert_eq!(header.uuid_txid, Some([0xAA; 16]));
    assert_eq!(header.date_start, Some(1625097600));
    assert_eq!(header.date_end, Some(1627689600));
    assert_eq!(header.row_count, 10);
    assert_eq!(header.column_count, 0);
    assert_eq!(header.ts_column, Some(0));
    assert_eq!(header.column_header_size, 0); // Simplified
    assert_eq!(header.segment_check, Some([0xBB; 8]));

    Ok(())
  }

  #[tokio::test]
  async fn test_segment_column_header_read_from_buffer() -> io::Result<()> {
    // Prepare a buffer to simulate serialized SegmentColumnHeader data
    let mut buffer: Vec<u8> = vec![];
    let column_name: &str = "TestColumn";
    let column_name_length: u16 = column_name.len() as u16;

    // Simulate writing data as it would be serialized
    buffer.extend_from_slice(&column_name_length.to_le_bytes());
    buffer.extend_from_slice(column_name.as_bytes());

    let column_type: u16 = EnumDataType::Int32 as u16;
    buffer.extend_from_slice(&column_type.to_le_bytes());

    let column_meta_length: u16 = 0u16;
    buffer.extend_from_slice(&column_meta_length.to_le_bytes());

    let column_enc: u8 = EnumDataEnc::None as u8;
    buffer.push(column_enc);
    let column_comp: u8 = EnumDataComp::None as u8;
    buffer.push(column_comp);
    
    let column_size: u64 = 123u64;
    buffer.extend_from_slice(&column_size.to_le_bytes());

    let column_check: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
    buffer.extend_from_slice(&column_check);
    
    // Now try to read this buffer back into a SegmentColumnHeader
    let mut cursor: Cursor<Vec<u8>> = Cursor::new(buffer);
    let header: SegmentColumnHeader = SegmentColumnHeader::read_from_buffer(&mut cursor)
      .map_err(|e: String| io::Error::new(io::ErrorKind::InvalidData, e))?;
    
    // Perform assertions to verify the correctness of the parsed data
    assert_eq!(header.column_name, "TestColumn");
    assert_eq!(header.column_type, EnumDataType::Int32);
    assert_eq!(header.column_enc, EnumDataEnc::None);
    assert_eq!(header.column_comp, EnumDataComp::None);
    assert_eq!(header.column_size, 123);
    assert_eq!(header.column_check, column_check);

    Ok(())
  }
}
