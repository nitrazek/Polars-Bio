use arrow_array::{ArrayRef, ListArray, StructArray, UInt32Array, StringArray};
use arrow_schema::{DataType, Field};
use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::physical_plan::Accumulator;
use datafusion::error::Result;
use datafusion::scalar::ScalarValue;
use exon::ExonSession;
use polars::prelude::DataType;
use tokio::runtime::Runtime;
use pyo3::prelude::*;
use datafusion::dataframe::DataFrame;
use datafusion_python::dataframe::PyDataFrame;
use arrow::datatypes::UInt32Type;
use std::collections::HashMap;
use std::sync::Arc;

use crate::context::PyBioSessionContext;
use crate::register_frame;

const LEFT_TABLE: &str = "s1";
const A: usize = 0;
const C: usize = 1;
const G: usize = 2;
const T: usize = 3;
const N: usize = 4;

fn get_list_array(v: Vec<Option<u32>>) -> Arc<ListArray> {
    Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![Some(v.clone())]))
}

fn get_struct_array(m: HashMap<&str, Vec<Option<u32>>>) -> Arc<StructArray> {
    Arc::new(StructArray::from(m.into_iter().map(|(k, v)| {
        (
            Arc::new(Field::new(k, DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))), false)),
            get_list_array(v.clone()) as ArrayRef
        )
    }).collect::<Vec<(Arc<Field>, ArrayRef)>>()))
}

#[derive(Debug)]
struct BaseSequenceContent {
    base_count: HashMap<u64, [u64; 5]>
}

impl BaseSequenceContent {
    pub fn new() -> Self {
        Self {
            base_count: HashMap::new()
        }
    }

    fn update_state(&mut self, s: &String) -> () {
        for (pos, base) in s.chars().enumerate() {
            let pos_count = self.base_count.entry(pos as u64).or_insert_with(|| [0_u64; 5]);
            let i = match base {
                'A' | 'a' => 0,
                'C' | 'c' => 1,
                'G' | 'g' => 2,
                'T' | 't' => 3,
                'N' | 'n' => 4,
                _ => panic!("Invalid base")
            };
            pos_count[i] += 1;
        }
    }
}

impl Accumulator for BaseSequenceContent {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut structs = Vec::new();
        let struct_datatype = DataType::Struct(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new("values", DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))))
        ]);
        for (i, array) in &self.base_count {
            let values: Vec<ScalarValue> = array.iter().map(|&v| ScalarValue::from(v)).collect();
            serialized_state.push(ScalarValue::List(ScalarValue::new_list(&values, &DataType::UInt64, false)));
        }
        Ok(serialized_state)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        (0..array.len()).try_for_each(|i| {
            let v = ScalarValue::try_from_array(array, i)?;
            if let ScalarValue::Utf8(Some(value)) = v {
                self.update_state(&value);
            }
            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        
    }

    // fn evaluate(&mut self) -> Result<ScalarValue> {
    //     let mut map: HashMap<&str, Vec<Option<u32>>> = HashMap::new();
    //     map.insert("A_count", self.base_count[0].clone());
    //     map.insert("C_count", self.base_count[1].clone());
    //     map.insert("T_count", self.base_count[2].clone());
    //     map.insert("G_count", self.base_count[3].clone());
    //     map.insert("N_count", self.base_count[4].clone());
    //     Ok(ScalarValue::Struct(get_struct_array(map))) 
    // }
}


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
