use arrow::array::{ArrayBuilder, ListBuilder, StructBuilder, UInt64Builder};
use arrow_array::{Array, ArrayRef, ListArray, StringArray, UInt32Array};
use arrow_schema::{DataType, Field};
use datafusion::logical_expr::{create_udaf, Volatility};
use datafusion::physical_plan::Accumulator;
use datafusion::error::{DataFusionError, Result};
use datafusion::scalar::ScalarValue;
use exon::ExonSession;
use pyo3::prelude::*;
use datafusion::dataframe::DataFrame;
use tokio::runtime::Runtime;
use std::sync::Arc;

const LEFT_TABLE: &str = "s1";
// const A: usize = 0;
// const C: usize = 1;
// const G: usize = 2;
// const T: usize = 3;
// const N: usize = 4;

// fn get_list_array(v: Vec<Option<u32>>) -> Arc<ListArray> {
//     Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![Some(v.clone())]))
// }

// fn get_struct_array(m: HashMap<&str, Vec<Option<u32>>>) -> Arc<StructArray> {
//     Arc::new(StructArray::from(m.into_iter().map(|(k, v)| {
//         (
//             Arc::new(Field::new(k, DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))), false)),
//             get_list_array(v.clone()) as ArrayRef
//         )
//     }).collect::<Vec<(Arc<Field>, ArrayRef)>>()))
// }

#[derive(Debug)]
struct BaseSequenceContent {
    a_counts: Vec<u64>,
    c_counts: Vec<u64>,
    t_counts: Vec<u64>,
    g_counts: Vec<u64>,
    n_counts: Vec<u64>,
    max_position_seen: usize
}

impl BaseSequenceContent {
    pub fn new() -> Self {
        BaseSequenceContent {
            a_counts: Vec::new(),
            c_counts: Vec::new(),
            t_counts: Vec::new(),
            g_counts: Vec::new(),
            n_counts: Vec::new(),
            max_position_seen: 0
        }
    }
    
    fn ensure_capacity(&mut self, desired_len: usize) {
        if desired_len > self.max_position_seen {
            self.a_counts.resize(desired_len, 0);
            self.c_counts.resize(desired_len, 0);
            self.g_counts.resize(desired_len, 0);
            self.t_counts.resize(desired_len, 0);
            self.max_position_seen = desired_len;
        }
    }
}

impl Accumulator for BaseSequenceContent {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(Arc::new(UInt64Array::from(self.a_counts.clone()))),
            ScalarValue::from(Arc::new(UInt64Array::from(self.c_counts.clone()))),
            ScalarValue::from(Arc::new(UInt64Array::from(self.g_counts.clone()))),
            ScalarValue::from(Arc::new(UInt64Array::from(self.t_counts.clone()))),
            ScalarValue::from(Arc::new(UInt64Array::from(self.n_counts.clone()))),
            ScalarValue::from(self.max_position_seen as u32)
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let struct_fields = vec![
            Arc::new(Field::new("A_count", DataType::UInt64, false)),
            Arc::new(Field::new("C_count", DataType::UInt64, false)),
            Arc::new(Field::new("G_count", DataType::UInt64, false)),
            Arc::new(Field::new("T_count", DataType::UInt64, false)),
            Arc::new(Field::new("N_count", DataType::UInt64, false)),
        ];
        let struct_builders: Vec<Box<dyn ArrayBuilder>> = vec![
            Box::new(UInt64Builder::new()),
            Box::new(UInt64Builder::new()),
            Box::new(UInt64Builder::new()),
            Box::new(UInt64Builder::new()),
            Box::new(UInt64Builder::new()),
        ];
        let mut struct_builder = StructBuilder::new(struct_fields, struct_builders);
        let mut list_builder = ListBuilder::new(struct_builder);

        for i in 0..self.max_position_seen {
            let a_count = self.a_counts.get(i).copied().unwrap_or(0);
            let c_count = self.c_counts.get(i).copied().unwrap_or(0);
            let g_count = self.g_counts.get(i).copied().unwrap_or(0);
            let t_count = self.t_counts.get(i).copied().unwrap_or(0);
            let n_count = self.n_counts.get(i).copied().unwrap_or(0);

            let struct_builder_ref = list_builder.values().as_any().downcast_mut::<StructBuilder>().unwrap();
            struct_builder_ref.field_builder::<UInt64Builder>(0).unwrap().append_value(a_count);
            struct_builder_ref.field_builder::<UInt64Builder>(1).unwrap().append_value(c_count);
            struct_builder_ref.field_builder::<UInt64Builder>(2).unwrap().append_value(g_count);
            struct_builder_ref.field_builder::<UInt64Builder>(3).unwrap().append_value(t_count);
            struct_builder_ref.field_builder::<UInt64Builder>(4).unwrap().append_value(n_count);
            struct_builder_ref.append(true);
            list_builder.append(true);
        }
        
        Ok(ScalarValue::List(Arc::new(list_builder.finish())))
    }
    
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() { return Ok(()); }

        let sequences = values[0].as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Internal("Argument must be string array".to_string())
        })?;

        for i in 0..sequences.len() {
            if let Some(seq) = sequences.value(i) {
                let current_seq_len = seq.len();
                self.ensure_capacity(current_seq_len);

                for (pos, base_char) in seq.chars().enumerate() {
                    if pos < self.max_position_seen {
                        match base_char.to_ascii_uppercase() {
                            'A' => self.a_counts[pos] += 1,
                            'C' => self.c_counts[pos] += 1,
                            'G' => self.g_counts[pos] += 1,
                            'T' => self.t_counts[pos] += 1,
                            'N' => self.n_counts[pos] += 1,
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() { return Ok(()); }

        let a_counts_array = states[0].as_any().downcast_ref::<ListArray>().ok_or_else(|_| {
            DataFusionError::Internal("First column must be list array of A counts".to_string())
        })?;
        let c_counts_array = states[1].as_any().downcast_ref::<ListArray>().ok_or_else(|_| {
            DataFusionError::Internal("Second column must be list array of C counts".to_string())
        })?;
        let g_counts_array = states[2].as_any().downcast_ref::<ListArray>().ok_or_else(|_| {
            DataFusionError::Internal("Third column must be list array of G counts".to_string())
        })?;
        let t_counts_array = states[3].as_any().downcast_ref::<ListArray>().ok_or_else(|_| {
            DataFusionError::Internal("Fourth column must be list array of T counts".to_string())
        })?;
        let n_counts_array = states[4].as_any().downcast_ref::<ListArray>().ok_or_else(|_| {
            DataFusionError::Internal("Fifth column must be list array of N counts".to_string())
        })?;
        let max_pos_array = states[5].as_any().downcast_ref::<UInt32Array>().ok_or_else(|_| {
            DataFusionError::Internal("Sixth column must be u32 array of max positions".to_string())
        })?;

        for i in 0..a_counts_array.len() {
            let other_a_counts = a_counts_array.value(i).as_any().downcast_ref::<UInt64Array>().unwrap();
            let other_c_counts = c_counts_array.value(i).as_any().downcast_ref::<UInt64Array>().unwrap();
            let other_g_counts = g_counts_array.value(i).as_any().downcast_ref::<UInt64Array>().unwrap();
            let other_t_counts = t_counts_array.value(i).as_any().downcast_ref::<UInt64Array>().unwrap();
            let other_max_pos = max_pos_array.value(i) as usize;

            // Upewniamy się, że nasz akumulator ma wystarczającą pojemność do połączenia stanów.
            self.ensure_capacity(other_max_pos);

            // Sumujemy zliczenia z innych akumulatorów do bieżącego stanu.
            for pos in 0..other_max_pos {
                self.a_counts[pos] += other_a_counts.value(pos);
                self.c_counts[pos] += other_c_counts.value(pos);
                self.g_counts[pos] += other_g_counts.value(pos);
                self.t_counts[pos] += other_t_counts.value(pos);
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.a_counts.capacity() * std::mem::size_of::<u64>()
            + self.c_counts.capacity() * std::mem::size_of::<u64>()
            + self.g_counts.capacity() * std::mem::size_of::<u64>()
            + self.t_counts.capacity() * std::mem::size_of::<u64>()
    }
}

fn register_base_sequence_content(ctx: &ExonSession) {
    let udaf = create_udaf(
        "base_sequence_content",
        vec![DataType::Utf8],
        Arc::new(DataType::List(Arc::new(Field::new(
            "position_content",
            DataType::Struct(vec![
                Field::new("A", DataType::UInt64, false),
                Field::new("C", DataType::UInt64, false),
                Field::new("G", DataType::UInt64, false),
                Field::new("T", DataType::UInt64, false),
                Field::new("N", DataType::UInt64, false)
            ]),
            false
        )))),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(BaseSequenceContent::new()))),
        Arc::new(vec![
            DataType::List(Arc::new(Field::new("A_counts", DataType::UInt64, false))),
            DataType::List(Arc::new(Field::new("C_counts", DataType::UInt64, false))),
            DataType::List(Arc::new(Field::new("G_counts", DataType::UInt64, false))),
            DataType::List(Arc::new(Field::new("T_counts", DataType::UInt64, false))),
            DataType::List(Arc::new(Field::new("N_counts", DataType::UInt64, false))),
            DataType::UInt32,
        ])
    );
    ctx.session.register_udaf(udaf);
}

pub(crate) async fn do_base_sequence_content(
    ctx: &ExonSession,
    table_name: String
)-> DataFrame {
    register_base_sequence_content(ctx);

    let query = format!(
        r#"
        SELECT * FROM {}
        "#,
        table_name
    );

    ctx.sql(&query).await.unwrap()
}

pub(crate) async fn do_test_base_sequence_content(
    ctx: &ExonSession,
    table_name: String,
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
    df.unnest_columns(&["a_count", "c_count", "t_count", "g_count", "n_count"])
        .unwrap()
}