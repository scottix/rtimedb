use std::{fs::File, io::BufReader};
use std::io;
use tracing::info;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use csv::ReaderBuilder;
use clap::{Arg, Command};
use rtimedb::tsf::tsf_reader::TSFReader;
use rtimedb::tsf::tsf_writer::TSFWriter;
use rtimedb::tsf::segments::data::{EnumDataType,EnumDataEnc,EnumDataComp};

#[tokio::main]
async fn main() -> io::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let subscriber = FmtSubscriber::builder()
        .with_env_filter(filter)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let app: Command = Command::new("TimeSeriesDB")
        .version("1.0")
        .about("Manages a time series database")
        .subcommand(
            Command::new("create")
                .about("Creates a new time series database and populates it with some data")
                .arg(Arg::new("FILE")
                    .help("The file path of the database to create")
                    .required(true)
                    .index(1))
                .arg(Arg::new("input_file")
                    .short('i')
                    .long("input-file")
                    .value_name("INPUT FILE")
                    .help("CSV file to ingest data")
                    .required(true)),
        )
        .subcommand(
            Command::new("read")
                .about("Reads data from a time series database")
                .arg(Arg::new("FILE")
                    .help("The file path of the database to read from")
                    .required(true)
                    .index(1)),
        );

    let matches: clap::ArgMatches = app.get_matches();

    match matches.subcommand() {
        Some(("create", sub_matches)) => {
            let file_path: &String = sub_matches
                .get_one::<String>("FILE")
                .expect("FILE argument missing");
            let input_file: &String = sub_matches
                .get_one::<String>("input_file")
                .expect("input_file missing");
            return create_time_series_db(file_path, input_file);
        },
        Some(("read", sub_matches)) => {
            let file_path: &String = sub_matches
                .get_one::<String>("FILE")
                .expect("FILE argument missing");
            return read_time_series_db(file_path);
        },
        _ => Ok(()),
    }
}

fn create_time_series_db(file_path: &str, input_file: &str) -> io::Result<()> {
    // Open the input CSV file
    let csv_file: File = File::open(input_file)?;
    let mut rdr: csv::Reader<BufReader<File>> = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(BufReader::new(csv_file));

    let mut tsf_writer: TSFWriter = TSFWriter::new(file_path)?;
    tsf_writer.add_column_header("temperature", EnumDataType::Int8, EnumDataEnc::None, EnumDataComp::None)
        .map_err(|e: String| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut temperatures: Vec<i8> = Vec::new();

    for result in rdr.records() {
        let record: csv::StringRecord = result?;
        // Assuming the first column in the CSV is temperature
        if let Ok(temp) = record.get(0).unwrap_or("0").parse::<i8>() {
            temperatures.push(temp);
        }
    }

    tsf_writer.add_column_data(temperatures, EnumDataEnc::None, EnumDataComp::None)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    tsf_writer.try_save()?;

    println!("Created TimeSeriesFile");
    Ok(())
}

fn read_time_series_db(file_path: &str) -> io::Result<()> {
    info!("Reading from the database at: {}", file_path);

    let mut tsf_reader: TSFReader = TSFReader::new(file_path)?;
    tsf_reader.read_all()?;


    info!("Data read successfully.");
    Ok(())
}
