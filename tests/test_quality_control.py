import polars as pl
import polars_bio as pb

FASTQ_PATH = "./data/quality_control/example.fastq"
TARGET_PATH = "./data/quality_control/target.csv"

target_df = pl.read_csv(TARGET_PATH)


class TestBaseSequenceContent:
    def test_with_path(self):
        """Test base_sequence_content with file path as input"""

        print(f"\nRunning test with file path input: {FASTQ_PATH}")
        result = pb.base_sequence_content(FASTQ_PATH)
        
        self._assert_equal(target_df, result)
        print("✓ Path input test PASSED")
    
    def test_with_lazy_frame(self):
        """Test base_sequence_content with LazyFrame as input"""

        print("\nRunning test with LazyFrame input")
        lazy_df = pb.read_fastq(FASTQ_PATH)
        
        result = pb.base_sequence_content(lazy_df)
        
        self._assert_equal(target_df, result)
        print("✓ LazyFrame input test PASSED")

    def test_with_polars_df(self):
        """Test base_sequence_content with polars DataFrame as input"""
        
        print("\nRunning test with pandas DataFrame input")
        polars_df = pb.read_fastq(FASTQ_PATH).collect()
        
        result = pb.base_sequence_content(polars_df)
        
        self._assert_equal(target_df, result)
        print("✓ pandas DataFrame input test PASSED")
    
    def test_with_pandas_df(self):
        """Test base_sequence_content with pandas DataFrame as input"""
        
        print("\nRunning test with polars DataFrame input")
        fastq_df = pb.read_fastq(FASTQ_PATH).collect().to_pandas()
        
        result = pb.base_sequence_content(fastq_df)
        
        self._assert_equal(target_df, result)
        print("✓ polars DataFrame input test PASSED")

    def _assert_equal(self, df1: pl.DataFrame, df2: pl.DataFrame):
        """Helper function to assert two DataFrames are equal"""
        assert df1.shape == df2.shape, f"Shape mismatch! Expected {df2.shape}, got {df1.shape}"
        assert df1.equals(df2), "Data content mismatch between result and target dataframes"


    def run_all_tests(self):
        """Run all tests in this class"""
        self.test_with_path()
        self.test_with_lazy_frame()
        self.test_with_polars_df()
        self.test_with_pandas_df()
        print("\n✓✓ All tests completed successfully!")

#TestBaseSequenceContent().run_all_tests()
