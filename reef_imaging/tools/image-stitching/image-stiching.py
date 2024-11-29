import pandas as pd
import numpy as np
import cv2
import os
import psutil
import gc
from pathlib import Path
import tqdm
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import warnings
warnings.filterwarnings('ignore')

def create_stitching_canvas(x_pos, x_neg, y_pos, y_neg, pixel_size_um):
  """Create empty canvas based on stage limits"""
  # Convert mm to μm
  width_um = (x_pos - x_neg) * 1000
  height_um = (y_pos - y_neg) * 1000

  # Calculate canvas size in pixels
  canvas_width = int(width_um / pixel_size_um)
  canvas_height = int(height_um / pixel_size_um)

  return canvas_width, canvas_height

def get_pixel_position(x_mm, y_mm, x_neg, y_neg, pixel_size_um):
  """Convert stage position (mm) to pixel coordinates"""
  x_um = (x_mm - x_neg) * 1000
  y_um = (y_mm - y_neg) * 1000

  x_pixel = int(x_um / pixel_size_um)
  y_pixel = int(y_um / pixel_size_um)

  return x_pixel, y_pixel

import cv2

def save_standard_size_image(input_image, output_filename, max_size=4096):
    """Save a resized version of the image with a maximum size while keeping the original aspect ratio."""
    # Get the original dimensions
    height, width = input_image.shape[:2]

    # Calculate the aspect ratio
    aspect_ratio = width / height

    # Determine new dimensions
    if width > height:
        new_width = max_size
        new_height = int(max_size / aspect_ratio)
    else:
        new_height = max_size
        new_width = int(max_size * aspect_ratio)

    # Resize the image to the new dimensions
    resized_image = cv2.resize(input_image, (new_width, new_height), interpolation=cv2.INTER_AREA)

    # Save the resized image
    cv2.imwrite(output_filename, resized_image)

# Suggested modification
def normalize_image(img):
    """Normalize image to 8-bit while preserving dynamic range"""
    if img.dtype == np.uint16:
        # Scale based on actual dynamic range
        img_min = np.min(img)
        img_max = np.max(img)
        if img_max > img_min:
            img = ((img - img_min) * 255.0 / (img_max - img_min)).astype(np.uint8)
        else:
            img = np.zeros_like(img, dtype=np.uint8)
    return img
      
def create_memmap_canvas(output_file, shape, dtype=np.uint8):
    """Create a memory-mapped file for the canvas"""
    return np.memmap(output_file, dtype=dtype, mode='w+', shape=shape)

def process_tile(args):
    """Process a single tile of the canvas"""
    # Unpack arguments
    df_chunk, channel, stage_limits, image_shape, output_file, tile_bounds, pixel_size_um = args

    # Unpack tile bounds
    y_start, y_end, x_start, x_end = tile_bounds

    # Create or open memmap for this tile
    tile_shape = (y_end - y_start, x_end - x_start)
    tile = np.memmap(output_file, dtype=np.uint16, mode='r+', shape=tile_shape)

    # Process images in this tile
    for _, row in df_chunk.iterrows():
        filename = f"{row['region']}_{row['i']}_0_0_Fluorescence_{channel}_nm_Ex.bmp"
        image_path = Path(df_chunk['csv_file'].iloc[0]).parent / filename

        if image_path.exists():
            # Read image
            img = cv2.imread(str(image_path), cv2.IMREAD_UNCHANGED)
            if img.dtype == np.uint16:
              img = (img/256).astype(np.uint8)
            # Get position in pixels
            x_pixel, y_pixel = get_pixel_position(
                row['x (mm)'],
                row['y (mm)'],
                stage_limits['x_negative'],
                stage_limits['y_negative'],
                pixel_size_um
            )

            # Calculate image placement coordinates relative to tile
            img_y_start = y_pixel - y_start
            img_y_end = img_y_start + image_shape[0]
            img_x_start = x_pixel - x_start
            img_x_end = img_x_start + image_shape[1]

            # Check if image falls within tile bounds
            if (img_y_start >= 0 and img_x_start >= 0 and 
                img_y_end <= tile_shape[0] and img_x_end <= tile_shape[1]):

                # Write image data to the memmap
                tile[img_y_start:img_y_end, img_x_start:img_x_end] = img

    # Explicitly delete the memmap object to ensure changes are written to disk
    del tile
    gc.collect()

    return tile_bounds

def stitch_channel_parallel(csv_file, channel, stage_limits, image_shape=(3000, 3000), 
                        tile_size=(10000, 10000), pixel_size_um=0.04625):
    """Stitch images for a specific channel using parallel processing and tiles"""
    print(f"Initial memory usage: {psutil.Process().memory_info().rss / 1024 / 1024:.2f} MB")

    df = pd.read_csv(csv_file)
    df = df[df['region'] == 'R0'].copy()
    df['csv_file'] = csv_file

    # Print dataset size
    print(f"Processing {len(df)} images for R0")


    # Calculate canvas dimensions
    canvas_width, canvas_height = create_stitching_canvas(
        stage_limits['x_positive'],
        stage_limits['x_negative'],
        stage_limits['y_positive'],
        stage_limits['y_negative'],
        pixel_size_um
    )

    # Create output memmap file
    output_file = f"R0_stitched_{channel}nm_temp.mmap"
    canvas = create_memmap_canvas(output_file, (canvas_height, canvas_width))

    # Calculate tiles
    n_tiles_y = int(np.ceil(canvas_height / tile_size[0]))
    n_tiles_x = int(np.ceil(canvas_width / tile_size[1]))

    # Prepare tasks for parallel processing
    tasks = []
    for ty in range(n_tiles_y):
        for tx in range(n_tiles_x):
            y_start = ty * tile_size[0]
            y_end = min((ty + 1) * tile_size[0], canvas_height)
            x_start = tx * tile_size[1]
            x_end = min((tx + 1) * tile_size[1], canvas_width)

            # Filter DataFrame for images that might fall in this tile
            tile_bounds = (y_start, y_end, x_start, x_end)
            tasks.append((df, channel, stage_limits, image_shape, output_file, tile_bounds, pixel_size_um))

    # Process tiles in parallel
    n_cores = max(1, multiprocessing.cpu_count() - 1)
    with ProcessPoolExecutor(max_workers=n_cores) as executor:
        list(tqdm.tqdm(executor.map(process_tile, tasks), 
                    total=len(tasks), 
                    desc=f"Processing {channel}nm tiles"))

    # Convert memmap to final TIFF file
    output_filename = f"R0_stitched_{channel}nm.tiff"
    cv2.imwrite(output_filename, np.array(canvas))
    
    standard_size_filename = f"R0_stitched_{channel}nm_4096x4096.tiff"
    save_standard_size_image(np.array(canvas), standard_size_filename)
    print(f"Saved standard size image: {standard_size_filename}")
    # Clean up temporary memmap file
    del canvas
    os.remove(output_file)

    return output_filename

def main():
  # Define stage limits
  # stage_limits = {
  #     'x_positive': 112.5,
  #     'x_negative': 10,
  #     'y_positive': 76,
  #     'y_negative': 6
  # }

  stage_limits = {
      'x_positive': 100,
      'x_negative': 85,
      'y_positive': 15,
      'y_negative': 5
  }
  # Define channels
  channels = ['405', '488', '561', '638']

  # Get CSV file path
  csv_file = '/media/reef/harddisk/hpa-2_2024-11-12_17-03-42.994649/0/coordinates.csv'
  pixel_size_um = 0.1665 #CAMERA_PIXEL_SIZE_UM / (TUBE_LENS_MM / (OBJECTIVE_TUBE_LENS_MM / MAGNIFICATION)) = 1.85/(50/(180/40))= 0.1665
  # Process each channel
  created_files = []
  for channel in channels:
      print(f"Processing channel {channel}nm...")
      output_file = stitch_channel_parallel(csv_file, channel, stage_limits, pixel_size_um=pixel_size_um)
      created_files.append(output_file)
      print(f"Saved {output_file}")

  # Print created files
  print("\nCreated files:")
  for file in created_files:
      print(file)

if __name__ == "__main__":
  main()

# The get_pixel_position and create_stitching_canvas functions remain the same as in your code