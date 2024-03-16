use csv::Writer;
use std::error::Error;
use std::fs::File;

fn main() -> Result<(), Box<dyn Error>> {
  // @TODO ability to change options from cli
  let file_path: &str = "test_data_10.csv";
  let mut wtr: Writer<File> = Writer::from_writer(File::create(file_path)?);

  for i in 0u64..10 {
    wtr.write_record(&[
      format!("{}", 1710555318 + i), // Timestamp
      format!("{}", i % 255), // Temperature
    ])?;
    // wtr.write_record(&[
    //     format!("{}", i * 1000), // timestamp
    //     format!("{}", i % 100),  // key
    //     format!("{:.2}", 0.01 * (i as f32)), // value
    // ])?;
  }
  wtr.flush()?;
  println!("Generated CSV file: {}", file_path);
  Ok(())
}
