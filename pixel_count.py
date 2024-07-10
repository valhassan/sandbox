import tqdm
import time
import argparse
import rasterio
import numpy as np
import pandas as pd
from pathlib import Path

def calculate_stats(image_path, label_path, label_class):
    # Load raster image and label data
    with rasterio.open(image_path) as src:
        image = src.read()
        image_width = src.width
        image_height = src.height
        transform = src.transform  # get affine transformation
    with rasterio.open(label_path) as src:
        label = src.read(1)
    
    # Get pixel resolution
    pixel_resolution_x = transform[0]
    pixel_resolution_y = transform[4]
    pixel_resolution = abs(pixel_resolution_x * pixel_resolution_y)  # area of a single pixel
    
    # Calculate area of image in square meters
    image_area_m2 = pixel_resolution * image_height * image_width
    image_area_km2 = image_area_m2 / 1e6
    
    # Calculate area of label class in square meters
    label_area_m2 = np.sum(label == label_class) * pixel_resolution  # Assuming waterbody label is 1
    label_area_km2 = label_area_m2 / 1e6
    
    # Count of raster pixels
    raster_pixel_count = image_height * image_width
    
    # Count of label pixels
    label_pixel_count = np.sum(label == label_class)
    
    # Calculate percentage of label representation
    # label_percentage = (label_pixel_count / raster_pixel_count) * 100
    
    return image_area_km2, label_area_km2, raster_pixel_count, label_pixel_count


def main(csv_list_path, image_label_parent_path, label_class_number, class_name):
    
    df = pd.read_csv(csv_list_path, sep=';', header=None, usecols=[0, 1])
    
    # Initialize variables to accumulate statistics
    total_image_processed = 0
    total_image_area_km2 = 0
    total_label_area_km2 = 0
    total_raster_pixel_count = 0
    total_label_pixel_count = 0
    image_label_parent_path = Path(image_label_parent_path)
    
    # Iterate through each row in the DataFrame
    for index, row in tqdm.tqdm(df.iterrows(), desc="Processing images"):
        image_path, label_path = row
        image_path = image_label_parent_path.joinpath(image_path)
        label_path = image_label_parent_path.joinpath(label_path)
        
        # Calculate waterbody statistics for each image-label pair
        stats = calculate_stats(image_path, label_path, label_class_number)
        
        # Accumulate statistics
        total_image_area_km2 += stats[0]
        total_label_area_km2 += stats[1]
        total_raster_pixel_count += stats[2]
        total_label_pixel_count += stats[3]
        total_image_processed += 1
    
    # Calculate percentage of label representation for all images
    total_label_percentage = (total_label_pixel_count / total_raster_pixel_count) * 100
    
    # Print total statistics
    print(f"Total number of images processed : {total_image_processed}")
    
    print(f"Total area covered by image in kilometer square : {total_image_area_km2}")
    
    print(f"Total area covered by {class_name} class in kilometer square : {total_label_area_km2}")
    
    print(f"Total number of pixels in all raster images : {total_raster_pixel_count}")
    
    print(f"Total number of pixels labeled as {class_name} : {total_label_pixel_count}")
    
    print(f"Percentage of {class_name} label representation in all raster images % : {total_label_percentage}")
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pixel stats for images and labels')
    parser.add_argument('csvFile', help='Path to CSV file')
    parser.add_argument('parentPath', help='Parent path to images and labels')
    parser.add_argument('classNumber', help='Label class number')
    parser.add_argument('className', help='Label class name:')
    args = parser.parse_args()
    start_time = time.time()
    print(f'\n\nBegin Pixel Stats for images and labels ....\n\n')
    main(args.csvFile, args.parentPath, int(args.classNumber), args.className)
    print("Elapsed time:{}".format(time.time() - start_time))