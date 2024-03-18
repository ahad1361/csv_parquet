import pandas as pd
import os
import argparse

def merge_parquet_files(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    for item in os.listdir(input_dir):
        item_path = os.path.join(input_dir, item)
        
        if os.path.isdir(item_path):
            print(f"Processing folder: {item}")
            parquet_files = [os.path.join(item_path, f) for f in os.listdir(item_path) if f.endswith('.parquet')]
            
            dfs = []
            
            for file in parquet_files:
                df = pd.read_parquet(file)
                dfs.append(df)
            
            if dfs:
                merged_df = pd.concat(dfs, ignore_index=True)
                output_file = os.path.join(output_dir, f"merged_{item}.parquet")
                merged_df.to_parquet(output_file, index=False)
                print(f"Merged Parquet file for {item} saved to {output_file}")
            else:
                print(f"No Parquet files found in {item_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Merge Parquet files from subdirectories')
    parser.add_argument('input_dir', type=str, help='Input directory containing subdirectories with Parquet files')
    parser.add_argument('output_dir', type=str, help='Output directory for merged Parquet files')
    
    args = parser.parse_args()
    merge_parquet_files(args.input_dir, args.output_dir)
