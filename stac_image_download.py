import time
import tqdm
import argparse
import pandas as pd
from pathlib import Path
from torchvision.datasets.utils import download_url
import logging
logging.getLogger().setLevel(logging.INFO)
# logging.basicConfig(filename='stac_image_download.log', filemode='w', level=logging.INFO)


def main(csv_file, save_dir):
    in_path = Path(csv_file)
    out_path = Path(save_dir)
    bands = ['R', 'G', 'B', 'N']
    df = pd.read_csv(in_path, header=None)
    stac_column = df[df.columns[0]]
    dir_column = []
    for stac_url in tqdm.tqdm(stac_column):
        save_dir = out_path / stac_url.split('/')[-1]
        if not save_dir.exists():
            logging.info(f'{save_dir} does not exist, creating directory')
            save_dir.mkdir()
        for band in bands:
            stac_url_b = stac_url + f'-{band}.tif'
            logging.info(f'{stac_url_b}')
            download_url(stac_url_b, save_dir)
            # logging.info(f'Downloaded {stac_url_b.split("/")[-1]} to {save_dir}')
        logging.info(f'Downloaded each {bands} band to {save_dir}')
        dir_column.append(save_dir)
    df[df.columns[0]] = dir_column
    out_file = in_path.parent.joinpath('stac_images_downloaded.csv')
    df.to_csv(out_file, index=False, header=False)
    logging.info(f' Complete!!! saved to {out_file}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download Stac Images')
    parser.add_argument('csvFile', help='Path to CSV file')
    parser.add_argument('saveDIR', help='Save Directory')
    args = parser.parse_args()
    start_time = time.time()
    print(f'\n\nDownloading Stac Images .....\n\n')
    main(args.csvFile, args.saveDIR)
    print("Elapsed time:{}".format(time.time() - start_time))


