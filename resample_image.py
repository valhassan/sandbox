import time
import argparse
import rasterio
from rasterio.enums import Resampling

def resample_raster(input_path, output_path, target_resolution):
    with rasterio.open(input_path) as src:
        
        xres, yres = target_resolution, target_resolution
        scale_factor_x = src.res[0]/xres
        scale_factor_y = src.res[1]/yres
        profile = src.profile.copy()
        
        data = src.read(
            out_shape=(src.count, 
                       int(src.height * scale_factor_y), 
                       int(src.width * scale_factor_x)
                       ),
            resampling=Resampling.bilinear
        )
        
        # scale image transform
        transform = src.transform * src.transform.scale(
            (1 / scale_factor_x),
            (1 / scale_factor_y)
        )
        
        profile.update({
            'transform': transform,
            'width': data.shape[-2],
            'height': data.shape[-1]
        })
        
        # Write the resampled data to the new file
        with rasterio.open(output_path, 'w', **profile) as dst:
            dst.write(data)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Resample raster image to a target resolution')
    parser.add_argument('input_path', help='Path to input raster image')
    parser.add_argument('output_path', help='Path to output resampled raster image')
    parser.add_argument('target_resolution', help='Target resolution in meters')
    args = parser.parse_args()
    start_time = time.time()
    print(f'\n\nBegin resampling of raster image to target resolution {args.target_resolution} ....\n\n')
    resample_raster(args.input_path, args.output_path, float(args.target_resolution))
    print("Elapsed time:{}".format(time.time() - start_time))