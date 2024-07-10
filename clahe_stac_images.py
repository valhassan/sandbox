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


def clahe(out_img):
    img_adapteq = []
    for band in range(out_img.shape[0]):
        if out_img.shape[1] > 30000 and out_img.shape[2] > 20000:
            new_img = np.zeros((out_img.shape[1], out_img.shape[2]), dtype=np.uint8)
            step_h = int(np.ceil(out_img.shape[1] / 4))
            step_w = int(np.ceil(out_img.shape[2] / 4))
            for row in range(0, out_img.shape[1], step_h):
                for col in range(0, out_img.shape[2], step_w):
                    sub_out = out_img[band, row:row + step_h, col:col + step_w]
                    sub_out = exposure.equalize_adapthist(sub_out, clip_limit=0.1)
                    sub_out = exposure.rescale_intensity(sub_out, out_range=(0, 255)).astype(np.uint8)
                    new_img[row:row + step_h, col:col + step_w] = sub_out
        else:
            new_img = exposure.equalize_adapthist(out_img[band, :, :], clip_limit=0.1)
            new_img = exposure.rescale_intensity(new_img, out_range=(0, 255)).astype(np.uint8)
        img_adapteq.append(new_img)
    out_img = np.stack(img_adapteq, axis=0)
    return out_img


def main(csv_file):
    in_path = Path(csv_file)
    bands = ['R', 'G', 'B', 'N']
    df = pd.read_csv(in_path, header=None)
    tiff_column = df[df.columns[0]]
    for raster_str in tqdm.tqdm(tiff_column):
        raster_path = Path(raster_str)
        for band in bands:
            raster_path_band = raster_path / f'{raster_path.stem}-{band}.tif'
            band_out_pth = raster_path / f'{raster_path.stem}-{band}_enhanced.tif'
            with rasterio.open(raster_path_band, 'r') as raster:
                out_img = raster.read()
                logging.info(f'image shape and dtype before CLAHE: {out_img.shape} , {out_img.dtype}')
                out_img = clahe(out_img)
                logging.info(f'image shape and dtype after CLAHE: {out_img.shape} , {out_img.dtype}')
                out_meta = raster.meta.copy()
                out_meta.update({"driver": "GTiff",
                                 "height": out_img.shape[1],
                                 "width": out_img.shape[2]})
            with rasterio.open(band_out_pth, "w", **out_meta) as dest:
                logging.info(f"writing clahe enhanced raster to {band_out_pth}")
                dest.write(out_img)
    logging.info(f' CLAHE Enhancement Complete!!! ')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CLAHE Enhancement of Stac Images')
    parser.add_argument('csvFile', help='Path to CSV file')
    args = parser.parse_args()
    start_time = time.time()
    print(f'\n\nBegin Enhancement of Stac Images....\n\n')
    main(args.csvFile)
    print("Elapsed time:{}".format(time.time() - start_time))