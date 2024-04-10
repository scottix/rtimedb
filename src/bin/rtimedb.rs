use std::{fs::File, io::BufReader};
use std::io;

use rtimedb::executors::physical_plan::PhysicalOperator;
use tracing::info;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use csv::ReaderBuilder;
use clap::{Arg, Command};
use rtimedb::tsf::tsf_reader::TSFReader;
use rtimedb::tsf::tsf_writer::TSFWriter;
use rtimedb::tsf::segments::types::{EnumDataType,EnumDataEnc,EnumDataComp};
use rtimedb::executors::{executor::Executor, physical_plan::PhysicalPlan};

#[tokio::main]
async fn main() -> Result<(), String> {
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
        )
        .subcommand(
            Command::new("stream")
                .about("Streams data from a time series database")
                .arg(Arg::new("FILE")
                    .help("The file path of the database to read from")
                    .required(true)
                    .index(1)),
        )
        .subcommand(
            Command::new("astream")
                .about("Async streams data from a time series database")
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
        Some(("stream", sub_matches)) => {
            let file_path: &String = sub_matches
                .get_one::<String>("FILE")
                .expect("FILE argument missing");
            return stream_time_series_db(file_path).await;
        },
        Some(("astream", sub_matches)) => {
            let file_path: &String = sub_matches
                .get_one::<String>("FILE")
                .expect("FILE argument missing");
            return astream_time_series_db(file_path).await;
        },
        _ => Ok(()),
    }
}

fn create_time_series_db(file_path: &str, input_file: &str) -> Result<(), String> {
    // Open the input CSV file
    let csv_file: File = File::open(input_file).map_err(|e| e.to_string())?;
    let mut rdr: csv::Reader<BufReader<File>> = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(BufReader::new(csv_file));

    let mut tsf_writer: TSFWriter = TSFWriter::new(file_path).map_err(|e| e.to_string())?;
    tsf_writer.add_column_header("metric_time", EnumDataType::Int32, EnumDataEnc::None, EnumDataComp::None, true)?;
    tsf_writer.add_column_header("temperature", EnumDataType::Int8, EnumDataEnc::None, EnumDataComp::None, false)?;

    let mut metric_time: Vec<i32> = Vec::new();
    let mut temperatures: Vec<i8> = Vec::new();

    for result in rdr.records() {
        let record: csv::StringRecord = result.map_err(|e: csv::Error| e.to_string())?;
    
        let time: i32 = record.get(0)
            .ok_or("Missing metric_time value".to_string())
            .and_then(|t: &str| t.parse::<i32>().map_err(|e: std::num::ParseIntError| e.to_string()))?;
    
        let temp: i8 = record.get(1)
            .ok_or("Missing temperature value".to_string())
            .and_then(|t: &str| t.parse::<i8>().map_err(|e: std::num::ParseIntError| e.to_string()))?;
    
        metric_time.push(time);
        temperatures.push(temp);
    }

    let min_date: i32 = *metric_time.iter().min().expect("Timestamp data should not be empty");
    let max_date: i32 = *metric_time.iter().max().expect("Timestamp data should not be empty");

    tsf_writer.add_column_data(metric_time, EnumDataEnc::None, EnumDataComp::None)?;
    tsf_writer.add_column_data(temperatures, EnumDataEnc::None, EnumDataComp::None)?;

    tsf_writer.update_segment_dates(min_date as i64, max_date as i64);

    tsf_writer.try_save().map_err(|e: io::Error| e.to_string())?;

    println!("Created TimeSeriesFile");
    Ok(())
}

fn read_time_series_db(file_path: &str) -> Result<(), String> {
    info!("Reading from the database at: {}", file_path);

    let mut tsf_reader: TSFReader = TSFReader::new(file_path).map_err(|e: io::Error| e.to_string())?;
    tsf_reader.read_all().map_err(|e: io::Error| e.to_string())?;


    info!("Data read successfully.");
    Ok(())
}

async fn stream_time_series_db(file_path: &str) -> Result<(), String> {
    info!("Reading from the database at: {}", file_path);

    let plan: PhysicalPlan = PhysicalPlan{
        root_operator: PhysicalOperator::Scan {
            columns: vec!("metric_time".to_string(), "temperature".to_string()),
            table_name: file_path.to_string(),
            time_range: None
        }
    };
    
    let tsf_executor: Executor = Executor{};
    let result: Vec<Vec<rtimedb::tsf::segments::types::EnumDataValue>> = tsf_executor.execute(plan).await?;

    for row in result {
        println!("{},{}", row[0], row[1]);
    }

    info!("Data read successfully.");
    Ok(())
}

async fn astream_time_series_db(file_path: &str) -> Result<(), String> {
    info!("Reading from the database at: {}", file_path);

    let plan: PhysicalPlan = PhysicalPlan{
        root_operator: PhysicalOperator::Scan {
            columns: vec!("metric_time".to_string(), "temperature".to_string()),
            table_name: file_path.to_string(),
            time_range: None
        }
    };
    
    let tsf_executor: Executor = Executor{};
    let result: Vec<Vec<rtimedb::tsf::segments::types::EnumDataValue>> = tsf_executor.execute_async(plan).await?;

    for row in result {
        println!("{},{}", row[0], row[1]);
    }

    info!("Data read successfully.");
    Ok(())
}
