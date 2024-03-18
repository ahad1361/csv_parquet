import unittest
import pandas as pd
import os
from convert_to_parquet import infer_dtypes_and_columns  # Adjust import as necessary
from merge_parquet import merge_parquet_files  # Adjust import as necessary

class TestFileConversion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up any test data or state that's global to the test class."""
        # Create a sample CSV file for testing the infer_dtypes_and_columns function
        cls.test_csv_file = "test_data.csv"
        data = {'int_col': [1, 2, 3], 'float_col': [0.1, 0.2, 0.3], 'str_col': ['a', 'b', 'c']}
        df = pd.DataFrame(data)
        df.to_csv(cls.test_csv_file, index=False)
        
        # Setup for merge_parquet_files test
        cls.input_dir = 'test_input_dir'
        os.makedirs(cls.input_dir, exist_ok=True)
        
        # Create dummy Parquet files
        df.to_parquet(os.path.join(cls.input_dir, 'test1.parquet'))
        df.to_parquet(os.path.join(cls.input_dir, 'test2.parquet'))

        cls.output_dir = 'test_output_dir'
    
    @classmethod
    def tearDownClass(cls):
        """Clean up any resources after all tests have run."""
        os.remove(cls.test_csv_file)
        for root, dirs, files in os.walk(cls.input_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(cls.input_dir)
        for root, dirs, files in os.walk(cls.output_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        if os.path.exists(cls.output_dir):
            os.rmdir(cls.output_dir)

    def test_infer_dtypes_and_columns(self):
        """Test that data types and columns are correctly inferred from the CSV file."""
        dtypes, columns = infer_dtypes_and_columns(self.test_csv_file)
        expected_dtypes = {'int_col': 'int32', 'float_col': 'float32', 'str_col': 'str'}
        expected_columns = ['int_col', 'float_col', 'str_col']
        
        self.assertEqual(dtypes, expected_dtypes, "Inferred data types do not match expected values.")
        self.assertEqual(columns, expected_columns, "Inferred columns do not match expected values.")

    def test_merge_parquet_files(self):
        """Test that Parquet files are correctly merged."""
        merge_parquet_files(self.input_dir, self.output_dir)
        
        # Verify output
        merged_file_path = os.path.join(self.output_dir, 'merged_test_input_dir.parquet')
        self.assertTrue(os.path.exists(merged_file_path))
        
        # Verify content of the merged file
        df_merged = pd.read_parquet(merged_file_path)
        self.assertEqual(len(df_merged), 6)  # Should be 6 rows, as each test file has 3 rows

if __name__ == '__main__':
    unittest.main()
