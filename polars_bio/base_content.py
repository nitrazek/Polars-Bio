import polars as pl
from polars_bio.polars_bio import test_base_content
from .context import ctx

def base_content() -> pl.DataFrame:
    return test_base_content(ctx).to_polars()

# def base_sequence_content(df: pl.DataFrame) -> pl.DataFrame:
#     """
#     Analizuje zawartość sekwencji na każdej pozycji w odczytach FASTQ.
#     """
#     return _base_content.base_content(df)

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
