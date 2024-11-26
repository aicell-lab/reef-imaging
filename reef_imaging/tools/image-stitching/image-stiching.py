import pandas as pd
import numpy as np
import cv2
import os
from pathlib import Path
import tqdm

def create_stitching_canvas(x_pos, x_neg, y_pos, y_neg, pixel_size_um=0.65):
  """Create empty canvas based on stage limits"""
  # Convert mm to μm
  width_um = (x_pos - x_neg) * 1000
  height_um = (y_pos - y_neg) * 1000

  # Calculate canvas size in pixels
  canvas_width = int(width_um / pixel_size_um)
  canvas_height = int(height_um / pixel_size_um)

  return canvas_width, canvas_height

def get_pixel_position(x_mm, y_mm, x_neg, y_neg, pixel_size_um=0.65):
  """Convert stage position (mm) to pixel coordinates"""
  x_um = (x_mm - x_neg) * 1000
  y_um = (y_mm - y_neg) * 1000

  x_pixel = int(x_um / pixel_size_um)
  y_pixel = int(y_um / pixel_size_um)

  return x_pixel, y_pixel

def stitch_channel(csv_file, channel, stage_limits, image_shape=(2048, 2048)):
  """Stitch images for a specific channel"""
  # Read position data
  df = pd.read_csv(csv_file)

  # Create canvas
  canvas_width, canvas_height = create_stitching_canvas(
      stage_limits['x_positive'],
      stage_limits['x_negative'],
      stage_limits['y_positive'],
      stage_limits['y_negative']
  )
  canvas = np.zeros((canvas_height, canvas_width), dtype=np.uint16)

  # Get folder path
  folder_path = Path(csv_file).parent

  # Process each position
  for _, row in tqdm.tqdm(df.iterrows(), total=len(df)):
      region = row['region']
      i = row['i']

      # Construct image filename
      filename = f"{region}_{i}_0_0_Fluorescence_{channel}_nm_Ex.bmp"
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

          # Calculate placement coordinates
          y_start = y_pixel
          y_end = y_start + image_shape[0]
          x_start = x_pixel
          x_end = x_start + image_shape[1]

          # Place image on canvas
          canvas[y_start:y_end, x_start:x_end] = img

  return canvas

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
  csv_file = 'path_to_your_csv_file.csv'  # Replace with actual path

  # Process each channel
  for channel in channels:
      print(f"Processing channel {channel}nm...")
      stitched_image = stitch_channel(csv_file, channel, stage_limits)

      # Save stitched image
      output_filename = f"stitched_{channel}nm.tiff"
      cv2.imwrite(output_filename, stitched_image)
      print(f"Saved {output_filename}")

if __name__ == "__main__":
  main()

# Created/Modified files during execution:
print("Created files:")
for channel in ['405', '488', '561', '638']:
  print(f"stitched_{channel}nm.tiff")