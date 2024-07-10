import rasterio
import time
import argparse
from rasterio.windows import Window
from pathlib import Path

import logging
logging.getLogger().setLevel(logging.INFO)

def read_image_parts(image_path:Path):
    image_part_pth = image_path.parent / f'{image_path.stem}_part.tif'
    with rasterio.open(image_path, 'r') as src:
        # window = Window.from_slices(slice(0, 5000),
        #                             slice(0, 5000))
        src_meta = src.meta
        w = src.read(window=Window(0, 0, 5000, 5000))
        src_meta.update({"driver": "GTiff",
                         "height": w.shape[1],
                         "width": w.shape[2],
                         "count": w.shape[0],
                         "compress": 'lzw'})
        logging.info(f'Writing to file: {image_part_pth}')
        with rasterio.open(image_part_pth, 'w+', **src_meta) as dest:
            dest.write(w)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='read and save image part using rasterio window')
    parser.add_argument('imagePath', help='Path to tif image')
    args = parser.parse_args()
    start_time = time.time()
    print(f'\n\nReading and saving image parts....\n\n')
    read_image_parts(Path(args.imagePath))
    print("Elapsed time:{}".format(time.time() - start_time))