import unittest
import pandas as pd
import os
from merge_parquet import merge_parquet_files
from convert_to_parquet import  infer_dtypes_and_columns  

class TestFileConversion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up for the infer_dtypes_and_columns test
        cls.test_csv_file = "temp_test_data.csv"
        data = {'int_col': [1, 2, 3], 'float_col': [0.1, 0.2, 0.3], 'str_col': ['a', 'b', 'c']}
        cls.df = pd.DataFrame(data)
        cls.df.to_csv(cls.test_csv_file, index=False)

        # Set up for the merge_parquet_files test
        cls.input_dir = 'temp_input_dir'
        cls.output_dir = 'temp_output_dir'
        os.makedirs(cls.input_dir, exist_ok=True)
        os.makedirs(cls.output_dir, exist_ok=True)
        
        # Create a subdirectory and Parquet files for testing
        sub_dir = os.path.join(cls.input_dir, 'subdir')
        os.makedirs(sub_dir, exist_ok=True)
        cls.df.to_parquet(os.path.join(sub_dir, 'file1.parquet'))
        cls.df.to_parquet(os.path.join(sub_dir, 'file2.parquet'))

    @classmethod
    def tearDownClass(cls):
        # Clean up resources created for the tests
        os.remove(cls.test_csv_file)
        for directory in [cls.input_dir, cls.output_dir]:
            for root, dirs, files in os.walk(directory, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(directory)

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
        
        # Verify the merged file exists in the output directory
        merged_file_path = os.path.join(self.output_dir, 'merged_subdir.parquet')
        self.assertTrue(os.path.exists(merged_file_path), "Merged file does not exist")

if __name__ == '__main__':
    unittest.main()
