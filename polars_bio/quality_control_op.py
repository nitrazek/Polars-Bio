import polars as pl
from polars_bio.polars_bio import py_base_sequence_content
from .context import ctx

def base_sequence_content(df: pl.DataFrame) -> pl.DataFrame:
    """
    Analizuje zawartość sekwencji na każdej pozycji w odczytach FASTQ.
    Przekazuje dane jako RecordBatchReader do funkcji Rust.
    """
    # arrow_table = df.to_arrow()
    # reader = pa.RecordBatchReader.from_batches(arrow_table.schema, [arrow_table])
    # result_arrow = 
    # return pl.from_arrow(result_arrow)

    return py_base_sequence_content(
        ctx,
        df.to_arrow().to_reader()
    ).to_polars()

