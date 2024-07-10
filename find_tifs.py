import time
import argparse
import pandas as pd
from pathlib import Path


def main(dir_path, csv_path):
    """Find all TIF files in a directory and its subdirectories"""
    in_path = Path(dir_path)
    out_path = Path(csv_path)

    tif_files = []
    for path in in_path.rglob('*.tif'):
        tif_files.append(str(path))

    # Save list of TIF files to CSV file
    out_file = out_path.joinpath('tifs.csv')
    df = pd.DataFrame(tif_files)
    df.to_csv(out_file, index=False, header=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Find all TIF files in a directory and its subdirectories')
    parser.add_argument('dir_path', help='Directory')
    parser.add_argument('csv_path', help='Csv output path')
    args = parser.parse_args()
    start_time = time.time()
    print(f'\n\nFinding all TIF files in directory and creating output csv.....\n\n')
    main(args.dir_path, args.csv_path)
    print("Elapsed time:{}".format(time.time() - start_time))
