import polars as pl
import pandas as pd
from pathlib import Path
from typing import Union
from polars_bio import read_fastq, overlap
from polars_bio.polars_bio import py_base_sequence_content
from .context import ctx

def base_sequence_content(
    data: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str]
) -> pl.DataFrame:
    if isinstance(data, str):
        path = Path(data)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {data}")
        if not path.suffix.lower() in ['.fastq', '.fq']:
            raise ValueError(f"Expected a FASTQ file, got: {path.suffix}")

        data = read_fastq(data)
        
    if isinstance(data, pl.LazyFrame):
        df = data.collect()
        
    elif isinstance(data, pd.DataFrame):
        df = pl.from_pandas(data)
        
    elif isinstance(data, pl.DataFrame):
        df = data
        
    else:
        raise TypeError(f"Unsupported input type: {type(data)}")
    
    if "sequence" not in df.columns:
        raise ValueError("Input data must have a 'sequence' column")

    return py_base_sequence_content(
        ctx,
        df.to_arrow().to_reader()
    ).to_polars()
