use std::{fs::File, io::{self, Read, Write}};

// "TSFD" in hex Magic Number
const TSFD_MAGIC_NUMBER: u32 = 0x54534644;
// Version Number
const TSFD_VERSION: u16 = 1;

#[repr(C)]
pub struct FileHeader {
  magic_number: u32,
  version: u16,
}

impl FileHeader {
  pub fn new() -> Self {
    FileHeader {
        magic_number: TSFD_MAGIC_NUMBER, 
        version: TSFD_VERSION,
    }
  }

  pub fn write_header(&self, file: &mut File) -> io::Result<()> {
    let mut bytes: Vec<u8> = Vec::new();
    bytes.extend_from_slice(&self.magic_number.to_le_bytes());
    bytes.extend_from_slice(&self.version.to_le_bytes());

    file.write_all(&bytes)
  }

  pub fn read_header(&mut self, file: &mut File) -> io::Result<()> {
    let mut buffer: [u8; 6] = [0u8; 6];
    
    file.read_exact(&mut buffer)?;

    self.magic_number = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
    self.version = u16::from_le_bytes([buffer[4], buffer[5]]);
    
    Ok(())
  }

  pub fn verify_header(&self) -> bool {
    self.magic_number == TSFD_MAGIC_NUMBER && self.version == TSFD_VERSION
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::fs::File;
  use std::io::{self, Read, Seek, SeekFrom};
  use tempfile::{tempdir, tempfile};

  #[test]
  fn test_write_header() -> io::Result<()> {
    let dir: tempfile::TempDir = tempdir()?;
    let file_path: std::path::PathBuf = dir.path().join("test.tsf");
    let mut file: File = File::create(&file_path)?;

    let header: FileHeader = FileHeader::new();
    header.write_header(&mut file)?;

    let mut file = File::open(file_path)?;
    let mut contents = vec![];
    file.read_to_end(&mut contents)?;

    let expected_bytes: Vec<u8> = vec![
      // TSFD_MAGIC_NUMBER in little endian
      0x44, 0x46, 0x53, 0x54,
      // TSFD_VERSION in little endian
      0x01, 0x00,
    ];

    assert_eq!(contents, expected_bytes);
    Ok(())
  }

  #[test]
  fn header_write_read_verify() -> io::Result<()> {
    let mut file = tempfile()?;
    let mut header = FileHeader::new();
    
    // Write header to temporary file
    header.write_header(&mut file)?;
    
    // Seek back to the start of the file before reading
    file.seek(SeekFrom::Start(0))?;

    // Reset header to default values
    header.magic_number = 0;
    header.version = 0;
    
    // Read header from file
    header.read_header(&mut file)?;
    
    // Verify the header
    assert!(header.verify_header());
    
    Ok(())
  }
}
