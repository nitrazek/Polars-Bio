use arrow_array::{ArrayRef, ListArray, UInt32Array};
use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::physical_plan::Accumulator;
use datafusion::error::Result;
use datafusion::scalar::ScalarValue;
use exon::ExonSession;
use tokio::runtime::Runtime;
use pyo3::prelude::*;
use datafusion::dataframe::DataFrame;
use datafusion_python::dataframe::PyDataFrame;
use arrow::datatypes::{DataType, UInt32Type};
use std::sync::Arc;

use crate::context::PyBioSessionContext;
use crate::register_frame;

const LEFT_TABLE: &str = "s1";
const A: usize = 0;
const C: usize = 1;
const G: usize = 2;
const T: usize = 3;
const N: usize = 4;

// #[derive(Debug)]
// struct BaseSequenceContent {
//     base_count: [Vec<Option<u32>>; 5]
// }
//
// impl BaseSequenceContent {
//     pub fn new() -> Self {
//         Self {
//             base_count: [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()]
//         }
//     }
// }
//
// impl Accumulator for BaseSequenceContent {
//     fn state(&mut self) -> Result<Vec<ScalarValue>> {
//         Ok(self.base_count.iter().map(|v| {
//             ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![Some(v.clone())])))
//         }).collect())
//     }
//
//     fn evaluate(&mut self) -> Result<ScalarValue> {
//         
//     }
// }

// let sequences = df.column("sequence")?.utf8()?;
// let mut position_counts: Vec<HashMap<char, usize>> = Vec::new();
//
// for seq in sequences.into_iter().flatten() {
//     for (i, base) in seq.chars().enumerate() {
//         if position_counts.len() <= i {
//             position_counts.push(HashMap::new());
//         }
//         let counts = position_counts.get_mut(i).unwrap();
//         *counts.entry(base).or_insert(0) += 1;
//     }
// }
//
// let positions: Vec<u32> = (0..position_counts.len() as u32).collect();
// let mut a_counts = Vec::new();
// let mut t_counts = Vec::new();
// let mut g_counts = Vec::new();
// let mut c_counts = Vec::new();
//
// for counts in &position_counts {
//     let total: usize = counts.values().sum();
//     a_counts.push(*counts.get(&'A').unwrap_or(&0) as f64 / total as f64);
//     t_counts.push(*counts.get(&'T').unwrap_or(&0) as f64 / total as f64);
//     g_counts.push(*counts.get(&'G').unwrap_or(&0) as f64 / total as f64);
//     c_counts.push(*counts.get(&'C').unwrap_or(&0) as f64 / total as f64);
// }
//
// let result_df = df![
//     "position" => positions,
//     "A" => a_counts,
//     "T" => t_counts,
//     "G" => g_counts,
//     "C" => c_counts
// ]?;
//
// Ok(result_df)

pub(crate) async fn do_base_sequence_content(
    ctx: &ExonSession,
    table_name: String
)-> DataFrame {
    ctx.sql("WITH my_map AS (SELECT MAP { 'key1': 'value1', 'key2': 'value2' } AS map) SELECT map_extract(map, 'key1') AS key1, map_extract(map, 'key2') AS key2 FROM my_map;").await.unwrap()
}

pub(crate) async fn do_test_base_sequence_content(
    ctx: &ExonSession,
    table_name: String
) -> DataFrame {
    let query = "
        WITH base_sequence_map AS (
            SELECT MAP {
                'A_count': [1, 2, 3],
                'C_count': [4, 5, 6],
                'T_count': [2, 3, 4],
                'G_count': [1, 5, 4],
                'N_count': [3, 1, 2]
            } AS map
        )
        SELECT
            array_any_value(map_extract(map, 'A_count')) AS A_count,
            array_any_value(map_extract(map, 'C_count')) AS C_count,
            array_any_value(map_extract(map, 'T_count')) AS T_count,
            array_any_value(map_extract(map, 'G_count')) AS G_count,
            array_any_value(map_extract(map, 'N_count')) AS N_count
        FROM base_sequence_map;
    ";
    let df: DataFrame = ctx.sql(&query).await.unwrap();
    df.unnest_columns(&["a_count", "c_count", "t_count", "g_count", "n_count"]).unwrap()
}
