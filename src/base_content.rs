use polars::prelude::*;
use std::collections::HashMap;

pub fn base_content(df: &DataFrame) -> Result<DataFrame> {
    let sequences = df.column("sequence")?.utf8()?;
    let mut position_counts: Vec<HashMap<char, usize>> = Vec::new();

    for seq in sequences.into_iter().flatten() {
        for (i, base) in seq.chars().enumerate() {
            if position_counts.len() <= i {
                position_counts.push(HashMap::new());
            }
            let counts = position_counts.get_mut(i).unwrap();
            *counts.entry(base).or_insert(0) += 1;
        }
    }

    let positions: Vec<u32> = (0..position_counts.len() as u32).collect();
    let mut a_counts = Vec::new();
    let mut t_counts = Vec::new();
    let mut g_counts = Vec::new();
    let mut c_counts = Vec::new();

    for counts in &position_counts {
        let total: usize = counts.values().sum();
        a_counts.push(*counts.get(&'A').unwrap_or(&0) as f64 / total as f64);
        t_counts.push(*counts.get(&'T').unwrap_or(&0) as f64 / total as f64);
        g_counts.push(*counts.get(&'G').unwrap_or(&0) as f64 / total as f64);
        c_counts.push(*counts.get(&'C').unwrap_or(&0) as f64 / total as f64);
    }

    let result_df = df![
        "position" => positions,
        "A" => a_counts,
        "T" => t_counts,
        "G" => g_counts,
        "C" => c_counts
    ]?;

    Ok(result_df)
}
