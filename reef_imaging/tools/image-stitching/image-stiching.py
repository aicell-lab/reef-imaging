import os
import cv2
import numpy as np
from glob import glob
import json
from tqdm import tqdm
import tifffile  # Add this import

def rotate_flip_image(image, angle, flip=False):
    """Rotate an image by a specified angle."""
    (h, w) = image.shape[:2]
    center = (w // 2, h // 2)

    # Get the rotation matrix
    rotation_matrix = cv2.getRotationMatrix2D(center, angle, 1.0)

    # Perform the rotation
    rotated = cv2.warpAffine(image, rotation_matrix, (w, h))
    if flip:
      rotated = cv2.flip(rotated, 1)
    return rotated
  
def load_imaging_parameters(parameter_file):
    with open(parameter_file, "r") as f:
        parameters = json.load(f)
    return parameters

def get_image_positions(parameters):
    dx = parameters["dx(mm)"] * 1000  # Convert mm to µm
    dy = parameters["dy(mm)"] * 1000  # Convert mm to µm
    Nx = parameters["Nx"]
    Ny = parameters["Ny"]
    return dx, dy, Nx, Ny

def get_stage_limits():
    limits = {
        "x_positive": 112.5,
        "x_negative": 10,
        "y_positive": 76,
        "y_negative": 6,
    }
    return limits

def parse_image_filenames(image_folder):
    image_files = glob(os.path.join(image_folder, "*.bmp"))
    image_info = []
    channels = set()

    for image_file in image_files:
        filename = os.path.basename(image_file)
        parts = filename.split('_')
        if len(parts) >= 6:
            R, x_idx, y_idx, z_idx, channel_name = parts[0], parts[1], parts[2], parts[3], '_'.join(parts[4:-1])
            extension = parts[-1]
            channels.add(channel_name)
            image_info.append({
                "filepath": image_file,
                "x_idx": int(x_idx),
                "y_idx": int(y_idx),
                "z_idx": int(z_idx),
                "channel_name": channel_name,
                "extension": extension
            })
    return image_info, list(channels)

def create_preview_image(canvas, max_size=4000):
    """Create a preview image with maximum dimension of max_size while maintaining aspect ratio"""
    height, width = canvas.shape

    # Calculate scaling factor
    scale = min(max_size / width, max_size / height)

    # Calculate new dimensions
    new_width = int(width * scale)
    new_height = int(height * scale)

    # Resize image
    preview = cv2.resize(canvas, (new_width, new_height), interpolation=cv2.INTER_AREA)

    # Convert to 8-bit for preview
    preview_8bit = cv2.normalize(preview, None, 0, 255, cv2.NORM_MINMAX).astype(np.uint8)

    return preview_8bit

def process_channel(channel_name, image_info, parameters, output_folder, rotation_angle=0):
    """Process one channel at a time to reduce memory usage, with optional rotation."""
    dx, dy, Nx, Ny = get_image_positions(parameters)

    # Filter images for current channel
    channel_images = [info for info in image_info if info["channel_name"] == channel_name]

    if not channel_images:
        return

    # Get image dimensions from first image
    sample_image = cv2.imread(channel_images[0]["filepath"])
    img_height, img_width, _ = sample_image.shape

    # Calculate canvas size
    canvas_width = img_width * Nx
    canvas_height = img_height * Ny

    # Create output file paths
    output_path = os.path.join(output_folder, f"stitched_{channel_name}.tiff")
    preview_path = os.path.join(output_folder, f"preview_{channel_name}.png")

    # Initialize empty canvas
    canvas = np.zeros((canvas_height, canvas_width), dtype=np.uint16)

    # Process images for this channel
    for info in tqdm(channel_images, desc=f"Processing {channel_name}"):
        x_idx = info["x_idx"]
        y_idx = info["y_idx"]

        # Calculate position on canvas
        x_pos = x_idx * img_width
        y_pos = (Ny - y_idx - 1) * img_height

        # Read image and convert to grayscale if it's not already
        image = cv2.imread(info["filepath"], cv2.IMREAD_GRAYSCALE)

        # Rotate the image if a rotation angle is specified
        if rotation_angle != 0:
            image = rotate_flip_image(image, rotation_angle,flip=True)
            

        # Place the (rotated) image on the canvas
        canvas[y_pos:y_pos + img_height, x_pos:x_pos + img_width] = image

    # Save the full resolution stitched image as TIFF
    try:
        tifffile.imwrite(
            output_path,
            canvas,
            compression='zlib',
            compressionargs={'level': 6}
        )
        print(f"Successfully saved {output_path}")

        # Create and save preview image
        preview = create_preview_image(canvas)
        cv2.imwrite(preview_path, preview)
        print(f"Successfully saved preview image: {preview_path}")

    except Exception as e:
        print(f"Error saving images for {channel_name}: {str(e)}")

    # Clear memory
    del canvas


def main():
    # Paths and parameters
    image_folder = "/media/reef/harddisk/test_stitching"
    parameter_file = os.path.join(image_folder, "acquisition parameters.json")
    output_folder = os.path.join(image_folder, "stitched_output")
    os.makedirs(output_folder, exist_ok=True)

    # Load imaging parameters
    parameters = load_imaging_parameters(parameter_file)

    # Parse image filenames and get unique channels
    image_info, channels = parse_image_filenames(image_folder)

    print(f"Found {len(channels)} channels: {channels}")
    print(f"Total images: {len(image_info)}")

    # Process each channel separately
    for channel in channels:
        try:
            process_channel(channel, image_info, parameters, output_folder, rotation_angle=90)
        except Exception as e:
            print(f"Error processing channel {channel}: {str(e)}")

if __name__ == "__main__":
    main()