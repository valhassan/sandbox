import os
import time
import tqdm
import argparse
import rasterio
import numpy as np
import pandas as pd
from pathlib import Path
from skimage import exposure

import logging
logging.getLogger().setLevel(logging.INFO)
# logging.basicConfig(filename='stac_image_download.log', filemode='w', level=logging.INFO)


def main(csv_file):
    in_path = Path(csv_file)
    df = pd.read_csv(in_path, header=None)
    tiff_column = df[df.columns[0]]
    dir_column = []
    for raster_str in tqdm.tqdm(tiff_column):
        raster_path = Path(raster_str)
        out_pth = f"{raster_path.parent}/{raster_path.stem.split('_')[0]}_3bands_8bit.tif"
        gdal_translate_cmd = f"gdal_translate -b 1 -b 1 -b 1 -ot Byte -of GTiff {raster_path} {out_pth}"
        os.system(gdal_translate_cmd)
        dir_column.append(out_pth)
    df[df.columns[0]] = dir_column
    out_file = in_path.parent.joinpath('ep20_AOI_LowRes.csv_3bands.csv')
    df.to_csv(out_file, index=False, header=False)
    logging.info(f' Complete!!! saved to {out_file}')
    logging.info(f'GRAY to RGB Complete!!!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='BGR to RGB')
    parser.add_argument('csvFile', help='Path to CSV file')
    args = parser.parse_args()
    start_time = time.time()
    print(f'\n\nBegin GRAY to RGB ....\n\n')
    main(args.csvFile)
    print("Elapsed time:{}".format(time.time() - start_time))