import polars as pl
import polars_bio as pb

df = pl.DataFrame({ "test": [1, 2, 3] })

out_df = pb.base_sequence_content(df)
print(out_df.head())
