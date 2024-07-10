import time
import tqdm
import argparse
import pandas as pd
from pathlib import Path

import logging
logging.getLogger().setLevel(logging.INFO)

def main(csv_file):
    in_path = Path(csv_file)
    df = pd.read_csv(in_path, header=None)
    tiff_column = df[df.columns[0]]
    dir_column = []
    for raster_str in tqdm.tqdm(tiff_column):
        raster_path = Path(raster_str)
        # # out_pth = f"{raster_path.parent}/{raster_path.stem.split('_')[0]}_NRGB_8bit.tif"
        # out_pth = f"{raster_path.parent}/{raster_path.stem}_NRGB.tif"
        # gdal_translate_cmd = f"gdal_translate -b 4 -b 3 -b 2 -b 1 -ot Byte -of GTiff {raster_path} {out_pth}"
        # os.system(gdal_translate_cmd)
        if raster_path.is_file():
            dir_column.append(raster_path)
        else:
            print(f"Does not Exist: {raster_path}")
    # df[df.columns[0]] = dir_column
    df = pd.DataFrame(dir_column)
    # out_file = in_path.parent.joinpath('planetscope_trn_NRGB.csv')
    out_file = in_path.parent.joinpath('planetscope_CSPP_validated.csv')
    df.to_csv(out_file, index=False, header=False)
    logging.info(f' Complete!!! saved to {out_file}')
    logging.info(f'Files checks finished!!!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check file existence')
    parser.add_argument('csvFile', help='Path to CSV file')
    args = parser.parse_args()
    start_time = time.time()
    print(f'\n\nBegin file check ....\n\n')
    main(args.csvFile)
    print("Elapsed time:{}".format(time.time() - start_time))