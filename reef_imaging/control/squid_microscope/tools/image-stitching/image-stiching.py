import os
import cv2
import numpy as np
from glob import glob
import json
from tqdm import tqdm
import tifffile  # Add this import

def compute_overlap_percent(deltaX, deltaY, image_width, image_height, pixel_size_xy, min_overlap=0):
    """Helper function to calculate percent overlap between images in a grid."""
    shift_x = deltaX / pixel_size_xy
    shift_y = deltaY / pixel_size_xy
    overlap_x = max(0, image_width - shift_x)
    overlap_y = max(0, image_height - shift_y)
    overlap_x = overlap_x * 100.0 / image_width
    overlap_y = overlap_y * 100.0 / image_height
    overlap = max(min_overlap, overlap_x, overlap_y)
    return overlap

# CAMERA_PIXEL_SIZE_UM = 1.85, TUBE_LENS_MM = 50, OBJECTIVE_TUBE_LENS_MM = 180, MAGNIFICATION = 40
def get_pixel_size(parameters, default_pixel_size=1.85, default_tube_lens_mm=50.0, default_objective_tube_lens_mm=180.0, default_magnification=40.0):
    """Calculate pixel size based on imaging parameters."""
    try:
        tube_lens_mm = float(parameters.get('tube_lens_mm', default_tube_lens_mm))
        pixel_size_um = float(parameters.get('sensor_pixel_size_um', default_pixel_size))
        objective_tube_lens_mm = float(parameters['objective'].get('tube_lens_f_mm', default_objective_tube_lens_mm))
        magnification = float(parameters['objective'].get('magnification', default_magnification))
    except KeyError:
        raise ValueError("Missing required parameters for pixel size calculation.")

    pixel_size_xy = pixel_size_um / (magnification / (objective_tube_lens_mm / tube_lens_mm))
    return pixel_size_xy

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
    pixel_size_xy = get_pixel_size(parameters)
    return dx, dy, Nx, Ny, pixel_size_xy

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
    
    dx, dy, Nx, Ny, pixel_size_xy = get_image_positions(parameters)

    # Filter images for current channel
    channel_images = [info for info in image_info if info["channel_name"] == channel_name]

    if not channel_images:
        return

    # Get image dimensions from first image
    sample_image = cv2.imread(channel_images[0]["filepath"])
    img_height, img_width, _ = sample_image.shape
    overlap_percent = min(100, compute_overlap_percent(dx, dy, img_width, img_height, pixel_size_xy))
    print(f"dx: {dx}, dy: {dy}, pixel_size_xy: {pixel_size_xy}")
    print(f"Image dimensions: width={img_width}, height={img_height}")
    print(f"Calculated overlap_percent: {overlap_percent}")

    # Calculate effective step size (accounting for overlap)
    x_step = img_width * (1 - overlap_percent / 100)
    y_step = img_height * (1 - overlap_percent / 100)

    # Calculate canvas size
    canvas_width = int(img_width + (Nx - 1) * x_step + 10)  # Add a small buffer
    canvas_height = int(img_height + (Ny - 1) * y_step + 10)

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
        x_pos = round(x_idx * x_step)
        y_pos = round((Ny - y_idx - 1) * y_step) # origin (0, 0) is at the top-left corner when taking images, but at the bottom-left corner in the canvas
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
    image_folder = "/media/reef/harddisk/test_stitching/stitching_dataset"
    parameter_file = os.path.join(image_folder, "acquisition parameters.json")
    output_folder = "/media/reef/harddisk/test_stitching/stitched_output"
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