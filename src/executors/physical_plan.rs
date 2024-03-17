use chrono::{DateTime, Duration, Utc};


pub enum PhysicalOperator {
  Scan {
    columns: Vec<String>,
    table_name: String,
    time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
  },
  Aggregate {
    input: Box<PhysicalOperator>,
    columns: Vec<String>,
    function: AggregationFunction,
    time_bucket: Duration,
  },
  Join {
    join_type: JoinType,
    left: Box<PhysicalOperator>,
    right: Box<PhysicalOperator>,
    condition: JoinCondition,
  }
}

pub enum AggregationFunction {
  Count,
  Sum,
  Avg,
  Max,
  Min,
}

pub enum JoinType {
  Inner,
  LeftOuter,
  RightOuter,
  FullOuter,
}

pub struct JoinCondition {
  left_column: String,
  right_column: String,
}

pub struct PhysicalPlan {
  pub root_operator: PhysicalOperator,
}
