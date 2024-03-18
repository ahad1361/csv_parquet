import pandas as pd
import os
import argparse
from tqdm.auto import tqdm

def infer_dtypes_and_columns(file_path):
    """
    Infer data types and columns to include from the first 1000 rows of the CSV file.
    """
    df_sample = pd.read_csv(file_path, nrows=1000)
    dtypes = {col: str(df_sample[col].dtypes) for col in df_sample.columns}
    dtype_mapping = {
        'int64': 'int32',
        'float64': 'float32',
        'object': 'str'
    }
    dtypes = {col: dtype_mapping.get(dtype, dtype) for col, dtype in dtypes.items()}
    column_included = list(df_sample.columns)
    return dtypes, column_included

def convert_csv_to_parquet(input_dir, output_dir):
    """
    Convert all CSV files in the input directory to Parquet format and save them in the output directory.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    
    for csv_file in tqdm(csv_files, desc="Converting CSV files to Parquet"):
        csv_file_path = os.path.join(input_dir, csv_file)
        base_name = os.path.basename(csv_file_path).replace(".csv", "")
        dtypes, column_include = infer_dtypes_and_columns(csv_file_path)

        read_dtypes = {col: 'object' if dtypes[col].startswith('float') or dtypes[col].startswith('int') else dtypes[col] 
                       for col in column_include}
        chunksize = 10000000
        chunk_number = 0

        with tqdm(total=os.path.getsize(csv_file_path) // chunksize, desc="Processing chunks") as pbar:
            for chunk in pd.read_csv(csv_file_path, usecols=column_include, dtype=read_dtypes, chunksize=chunksize, low_memory=False):
                for col, dtype in dtypes.items():
                    if dtype.startswith('float') or dtype.startswith('int'):
                        chunk[col] = pd.to_numeric(chunk[col], errors='coerce').astype(dtype)
                
                output_path = os.path.join(output_dir, f"{base_name}_chunk_{chunk_number}.parquet")
                chunk.to_parquet(output_path, index=False)
                chunk_number += 1
                pbar.update(1)

        print(f"All chunks for {csv_file_path} have been processed and saved.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert CSV files to Parquet')
    parser.add_argument('input_dir', type=str, help='Input directory containing CSV files')
    parser.add_argument('output_dir', type=str, help='Output directory for Parquet files')
    
    args = parser.parse_args()
    convert_csv_to_parquet(args.input_dir, args.output_dir)
