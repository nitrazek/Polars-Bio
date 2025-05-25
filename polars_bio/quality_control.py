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
