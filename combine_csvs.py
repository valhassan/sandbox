import os
import csv
import time
import argparse
import pandas as pd

def combine_csvs(source_folder: str) -> None:
    subfolders = ['trn', 'val', 'tst']

    # Loop through each subfolder
    for subfolder in subfolders:
        folder_path = os.path.join(source_folder, subfolder)
        csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]
        combines_csvs_file_path = os.path.join(source_folder, f"{subfolder}.csv")
        combined_dfs = []
        
        # Loop through the CSV files in the subfolder and append their data to the combined_data DataFrame
        for file in csv_files:
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path, header=None)
            combined_dfs.append(df)
        
        # Concatenate all DataFrames into one
        combined_data = pd.concat(combined_dfs, ignore_index=True)
        # Save the combined data to a new CSV file
        combined_data.to_csv(combines_csvs_file_path, index=False, header=False, quoting=csv.QUOTE_NONE)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combine csvs into trn, val, tst, single files")
    parser.add_argument("source_folder", help="Path to the source folder containing CSV files.")
    args = parser.parse_args()
    
    start_time = time.time()
    print(f'\n\nCombining CSVs....\n\n')
    combine_csvs(args.source_folder)
    print("Elapsed time:{}".format(time.time() - start_time))