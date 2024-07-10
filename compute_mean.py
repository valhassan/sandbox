import os
import csv
import numpy as np
import rasterio
from multiprocessing import Pool, cpu_count

import argparse

# Define a function to calculate statistics for a single image
def calculate_band_stats(image_file_path):
    band_means = []
    band_stds = []

    # Open the image using rasterio
    with rasterio.open(image_file_path) as src:
        for band in src.read():
            # Calculate the mean and standard deviation for each band
            band_mean = np.nanmean(band)
            band_std = np.nanstd(band)
            band_means.append(band_mean)
            band_stds.append(band_std)

    return band_means, band_stds

# Define command-line arguments
parser = argparse.ArgumentParser(description="Calculate statistics for TIFF files listed in a CSV.")
parser.add_argument("source_folder", help="Path to the folder containing the copied TIFF files.")
parser.add_argument("csv_file", help="Name of the CSV file containing TIFF file paths.")
args = parser.parse_args()

source_folder = args.source_folder
csv_file_path = os.path.join(source_folder, args.csv_file)
# Create a multiprocessing pool with the number of CPU cores available
num_cores = cpu_count()

# Check if the CSV file exists
if not os.path.exists(csv_file_path):
    print(f"CSV file not found: {csv_file_path}")
    exit(1)

# Read the CSV file
with open(csv_file_path, "r", newline="") as file:
    csv_reader = csv.reader(file, delimiter=";")
    image_paths = [os.path.join(source_folder, row[0]) for row in csv_reader if len(row) >= 1]

# Create a multiprocessing Pool
with Pool(processes=num_cores) as pool:
    # Use the Pool to calculate statistics for all images in parallel
    results = pool.map(calculate_band_stats, image_paths)

# Combine the results
band_means = []
band_stds = []
for result in results:
    means, stds = result
    band_means.extend(means)
    band_stds.extend(stds)

# Calculate and print the mean and standard deviation for each band
num_bands = len(results[0][0])
print("Band Statistics:")
for band_num in range(num_bands):
    band_mean = np.nanmean(band_means[band_num::num_bands]) 
    band_std = np.nanmean(band_stds[band_num::num_bands])
    scaled_band_mean = band_mean / 255.0
    scaled_band_std = band_std / 255.0
    print(f"Band {band_num + 1}: Mean = {band_mean}, Standard Deviation = {band_std}")
    print(f"Band {band_num + 1}: Scaled Mean = {scaled_band_mean}, Scaled Standard Deviation = {scaled_band_std}")
