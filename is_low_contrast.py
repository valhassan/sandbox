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
    bands = ['R', 'G', 'B', 'N']
    df = pd.read_csv(in_path, header=None)
    tiff_column = df[df.columns[0]]

    imgs = []
    for raster_str in tqdm.tqdm(tiff_column):
        img_dict = {'tiff': raster_str}
        raster_path = Path(raster_str)
        for band in bands:
            raster_path_band = raster_path / f'{raster_path.stem}-{band}.tif'
            with rasterio.open(raster_path_band, 'r') as raster:
                img = raster.read()
                high_or_low_contrast = exposure.is_low_contrast(img, fraction_threshold=0.3)
                print(high_or_low_contrast)
                img_dict[f'{band}'] = high_or_low_contrast
        imgs.append(img_dict)
    new_df = pd.DataFrame(imgs)
    out_file = in_path.parent.joinpath('is_low_contrast.csv')
    new_df.to_csv(out_file, index=False)
    logging.info(f' Check Complete!!! ')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check low contrast image bands')
    parser.add_argument('csvFile', help='Path to CSV file')
    args = parser.parse_args()
    start_time = time.time()
    print(f'\n\nBegin low or high contrast check....\n\n')
    main(args.csvFile)
    print("Elapsed time:{}".format(time.time() - start_time))