import polars as pl
import polars_bio as pb

def test_base_sequence_content():
    df = pl.DataFrame({"sequence": ["ATGC", "AAGC"]})
    result = pb.base_sequence_content(df)
    print(result)
    assert result.shape == (4, 5)

print(test_base_sequence_content())
