import polars as pl
from polars_bio.polars_bio import test_base_content
import pyarrow as pa
from .context import ctx

def base_content() -> pl.DataFrame:
    return test_base_content(ctx).to_polars()

def base_sequence_content(df: pl.DataFrame) -> pl.DataFrame:
    """
    Analizuje zawartość sekwencji na każdej pozycji w odczytach FASTQ.
    Przekazuje dane jako RecordBatchReader do funkcji Rust.
    """
    arrow_table = df.to_arrow()
    reader = pa.RecordBatchReader.from_batches(arrow_table.schema, [arrow_table])
    result_arrow = base_content(reader)
    return pl.from_arrow(result_arrow)

def plot_base_content(df: pl.DataFrame):
    """
    Generuje wykres przedstawiający proporcje nukleotydów na każdej pozycji.
    """
    import matplotlib.pyplot as plt

    positions = df["position"].to_numpy()
    plt.plot(positions, df["A"].to_numpy(), label='A')
    plt.plot(positions, df["T"].to_numpy(), label='T')
    plt.plot(positions, df["G"].to_numpy(), label='G')
    plt.plot(positions, df["C"].to_numpy(), label='C')
    plt.xlabel("Pozycja")
    plt.ylabel("Proporcja")
    plt.title("Zawartość sekwencji na pozycji")
    plt.legend()
    plt.show()
