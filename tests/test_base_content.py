import polars as pl
from polars_bio.base_content import base_sequence_content

def test_base_sequence_content():
    df = pl.DataFrame({"sequence": ["ATGC", "AAGC"]})
    result = base_sequence_content(df)
    assert result.shape == (4, 5)
