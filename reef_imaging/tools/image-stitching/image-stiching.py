import pandas as pd
import numpy as np
import cv2
import os
from pathlib import Path
import tqdm
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import warnings
warnings.filterwarnings('ignore')

def create_stitching_canvas(x_pos, x_neg, y_pos, y_neg, pixel_size_um=0.04625):
  """Create empty canvas based on stage limits"""
  # Convert mm to μm
  width_um = (x_pos - x_neg) * 1000
  height_um = (y_pos - y_neg) * 1000

  # Calculate canvas size in pixels
  canvas_width = int(width_um / pixel_size_um)
  canvas_height = int(height_um / pixel_size_um)

  return canvas_width, canvas_height

def get_pixel_position(x_mm, y_mm, x_neg, y_neg, pixel_size_um=0.04625):
  """Convert stage position (mm) to pixel coordinates"""
  x_um = (x_mm - x_neg) * 1000
  y_um = (y_mm - y_neg) * 1000

  x_pixel = int(x_um / pixel_size_um)
  y_pixel = int(y_um / pixel_size_um)

  return x_pixel, y_pixel

def create_memmap_canvas(output_file, shape, dtype=np.uint16):
  """Create a memory-mapped file for the canvas"""
  return np.memmap(output_file, dtype=dtype, mode='w+', shape=shape)

def process_tile(args):
  """Process a single tile of the canvas"""
  df_chunk, channel, stage_limits, image_shape, output_file, tile_bounds = args
  folder_path = Path(df_chunk['csv_file'].iloc[0]).parent

  # Create or open memmap for this tile
  y_start, y_end, x_start, x_end = tile_bounds
  tile_shape = (y_end - y_start, x_end - x_start)

  # Process images in this tile
  for _, row in df_chunk.iterrows():
      filename = f"{row['region']}_{row['i']}_0_0_Fluorescence_{channel}_nm_Ex.bmp"
      image_path = folder_path / filename

      if image_path.exists():
          # Read image
          img = cv2.imread(str(image_path), cv2.IMREAD_UNCHANGED)

          # Get position in pixels
          x_pixel, y_pixel = get_pixel_position(
              row['x (mm)'],
              row['y (mm)'],
              stage_limits['x_negative'],
              stage_limits['y_negative']
          )

          # Calculate image placement coordinates relative to tile
          img_y_start = y_pixel - y_start
          img_y_end = img_y_start + image_shape[0]
          img_x_start = x_pixel - x_start
          img_x_end = img_x_start + image_shape[1]

          # Check if image falls within tile bounds
          if (img_y_start >= 0 and img_x_start >= 0 and 
              img_y_end <= tile_shape[0] and img_x_end <= tile_shape[1]):

              # Open memmap in read-write mode
              with np.memmap(output_file, dtype=np.uint16, mode='r+', 
                           shape=(y_end - y_start, x_end - x_start)) as tile:
                  tile[img_y_start:img_y_end, img_x_start:img_x_end] = img

  return tile_bounds

def stitch_channel_parallel(csv_file, channel, stage_limits, image_shape=(3000, 3000), 
                        tile_size=(10000, 10000)):
  """Stitch images for a specific channel using parallel processing and tiles"""
  # Read position data
  df = pd.read_csv(csv_file)
  df['csv_file'] = csv_file  # Add csv_file path to DataFrame

  # Calculate canvas dimensions
  canvas_width, canvas_height = create_stitching_canvas(
      stage_limits['x_positive'],
      stage_limits['x_negative'],
      stage_limits['y_positive'],
      stage_limits['y_negative']
  )

  # Create output memmap file
  output_file = f"stitched_{channel}nm_temp.mmap"
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
          tasks.append((df, channel, stage_limits, image_shape, output_file, tile_bounds))

  # Process tiles in parallel
  n_cores = max(1, multiprocessing.cpu_count() - 1)
  with ProcessPoolExecutor(max_workers=n_cores) as executor:
      list(tqdm.tqdm(executor.map(process_tile, tasks), 
                    total=len(tasks), 
                    desc=f"Processing {channel}nm tiles"))

  # Convert memmap to final TIFF file
  output_filename = f"stitched_{channel}nm.tiff"
  cv2.imwrite(output_filename, np.array(canvas))

  # Clean up temporary memmap file
  del canvas
  os.remove(output_file)

  return output_filename

def main():
  # Define stage limits
  stage_limits = {
      'x_positive': 112.5,
      'x_negative': 10,
      'y_positive': 76,
      'y_negative': 6
  }

  # Define channels
  channels = ['405', '488', '561', '638']

  # Get CSV file path
  csv_file = '/media/reef/harddisk/hpa-2_2024-11-12_17-03-42.994649/0/coordinates.csv'

  # Process each channel
  created_files = []
  for channel in channels:
      print(f"Processing channel {channel}nm...")
      output_file = stitch_channel_parallel(csv_file, channel, stage_limits)
      created_files.append(output_file)
      print(f"Saved {output_file}")

  # Print created files
  print("\nCreated files:")
  for file in created_files:
      print(file)

if __name__ == "__main__":
  main()

# The get_pixel_position and create_stitching_canvas functions remain the same as in your code