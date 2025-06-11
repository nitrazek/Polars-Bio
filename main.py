import polars as pl
import polars_bio as pb

df = pl.DataFrame({"sequence": ["ATGC", "AAGC"]})
result = pb.base_sequence_content(df)
print(result)
