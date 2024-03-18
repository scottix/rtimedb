use std::io;
use std::pin::Pin;

use chrono::{DateTime, Utc};
use futures::Stream;
use tokio_stream::StreamExt;

use crate::tsf::{segments::types::EnumDataValue, tsf_reader::{DataRow, TSFReader}};

use super::physical_plan::{PhysicalOperator, PhysicalPlan};

pub struct Executor {}

impl Executor {
  pub fn new() -> Self {
    Executor{}
  }

  pub async fn execute(&self, plan: PhysicalPlan) -> Result<Vec<Vec<EnumDataValue>>, String> {
    self.execute_operator(&plan.root_operator).await
  }
  
  pub async fn execute_operator(&self, operator: &PhysicalOperator) -> Result<Vec<Vec<EnumDataValue>>, String> {
    match operator {
      PhysicalOperator::Scan { columns, table_name, time_range } => {
        self.execute_scan(columns, table_name, time_range).await
      },
      PhysicalOperator::Aggregate { input, columns, function, time_bucket } => {Err("Not Implemented".to_string())},
      PhysicalOperator::Join { join_type, left, right, condition } => {Err("Not Implemented".to_string())}
    }
  }

  async fn execute_scan(&self, _columns: &Vec<String>, _table_name: &String, _time_range: &Option<(DateTime<Utc>, DateTime<Utc>)>) -> Result<Vec<Vec<EnumDataValue>>, String> {
    let mut reader: TSFReader = TSFReader::new(_table_name)
      .map_err(|_| "Failed to read table_name".to_string())?;

    reader.read_all().map_err(|e: io::Error| e.to_string())?;

    let mut stream: Pin<Box<dyn Stream<Item = Result<DataRow, io::Error>> + Send>> = reader.stream_rows();
    let mut result: Vec<Vec<EnumDataValue>> = vec![];
    while let Some(row_result) = stream.next().await {
      match row_result {
        Ok(data_row) => {
          let row: Vec<EnumDataValue> = data_row.values;
          result.push(row);
        },
        Err(_) => return Err("Failed to fetch row".to_string()),
      }
    }

    Ok(result)
  }
}
