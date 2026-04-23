import cv2
import os
from datetime import datetime, timezone, timedelta

# Configuration
CAMERA1_DIR = '/media/reef/harddisk/lab_video/camera_1'
CAMERA2_DIR = '/media/reef/harddisk/lab_video/camera_2'
OUTPUT_PATH_1 = '/home/tao/workspace/reef-imaging/lab_camera_1_20260416_175122_181847.mp4'
OUTPUT_PATH_2 = '/home/tao/workspace/reef-imaging/lab_camera_2_20260416_175122_181847.mp4'

# Interval (inclusive start, inclusive end)
START_TIME = datetime(2026, 4, 16, 17, 49, 22, tzinfo=timezone(timedelta(hours=2)))
END_TIME = datetime(2026, 4, 16, 18, 16, 47, tzinfo=timezone(timedelta(hours=2)))

# Each timelapse file represents 30 minutes of real time
INTERVAL_SECONDS = 30 * 60  # 1800 seconds
FPS = 24.0
TOTAL_FRAMES_PER_FILE = 1438

def parse_filename_timestamp(filename):
    """Extract datetime from time_lapse_YYYYMMDD_HHMMSS.mp4"""
    basename = os.path.basename(filename)
    parts = basename.replace('.mp4', '').split('_')
    dt_str = parts[2] + parts[3]
    return datetime.strptime(dt_str, '%Y%m%d%H%M%S').replace(tzinfo=timezone(timedelta(hours=2)))

def get_files_for_camera(camera_dir, start_time, end_time):
    """Find video files that overlap with the requested interval."""
    files = sorted([f for f in os.listdir(camera_dir) if f.endswith('.mp4')])
    overlapping = []
    for f in files:
        filepath = os.path.join(camera_dir, f)
        file_start = parse_filename_timestamp(f)
        file_end = file_start + timedelta(seconds=INTERVAL_SECONDS)
        if file_end >= start_time and file_start <= end_time:
            overlapping.append((filepath, file_start, file_end))
    return overlapping

def real_time_to_frame_offset(real_offset_seconds):
    """Convert real time offset within a file to frame number."""
    ratio = real_offset_seconds / INTERVAL_SECONDS
    frame = int(round(ratio * TOTAL_FRAMES_PER_FILE))
    return max(0, min(TOTAL_FRAMES_PER_FILE - 1, frame))

def extract_frames(filepath, start_frame, end_frame):
    """Extract frames from a video file."""
    cap = cv2.VideoCapture(filepath)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {filepath}")
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
    frames = []
    for _ in range(start_frame, end_frame + 1):
        ret, frame = cap.read()
        if not ret:
            break
        frames.append(frame)
    cap.release()
    return frames

def write_video(frames, output_path, fps):
    if not frames:
        print(f"No frames to write for {output_path}")
        return
    h, w = frames[0].shape[:2]
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_path, fourcc, fps, (w, h))
    if not out.isOpened():
        raise RuntimeError(f"Cannot open video writer for {output_path}")
    for frame in frames:
        out.write(frame)
    out.release()
    print(f"Wrote {len(frames)} frames ({len(frames)/fps:.2f}s) to {output_path}")

def process_camera(camera_dir, output_path, label):
    files = get_files_for_camera(camera_dir, START_TIME, END_TIME)
    print(f"{label} files: {[os.path.basename(f[0]) for f in files]}")
    all_frames = []
    for filepath, file_start, file_end in files:
        seg_start = max(START_TIME, file_start)
        seg_end = min(END_TIME, file_end)
        start_offset = (seg_start - file_start).total_seconds()
        end_offset = (seg_end - file_start).total_seconds()
        start_frame = real_time_to_frame_offset(start_offset)
        end_frame = real_time_to_frame_offset(end_offset)
        cap = cv2.VideoCapture(filepath)
        actual_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        cap.release()
        end_frame = min(end_frame, actual_frames - 1)
        print(f"  {os.path.basename(filepath)}: frames {start_frame}-{end_frame}")
        frames = extract_frames(filepath, start_frame, end_frame)
        all_frames.extend(frames)
        print(f"    Extracted {len(frames)} frames")
    write_video(all_frames, output_path, FPS)
    return all_frames

def main():
    process_camera(CAMERA1_DIR, OUTPUT_PATH_1, "Camera 1")
    process_camera(CAMERA2_DIR, OUTPUT_PATH_2, "Camera 2")

if __name__ == '__main__':
    main()
