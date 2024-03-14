use csv::Writer;
use std::error::Error;
use std::fs::File;

fn main() -> Result<(), Box<dyn Error>> {
    let file_path: &str = "large_data.csv";
    let mut wtr: Writer<File> = Writer::from_writer(File::create(file_path)?);

    // Example: generate 100 million rows of data
    for i in 1u64..50_000_001 {
        wtr.write_record(&[
            format!("{}", i % 255), // timestamp
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
