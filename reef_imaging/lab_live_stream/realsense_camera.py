import os
import cv2
import time
import logging
import uvicorn
import numpy as np
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from threading import Thread, Event
from datetime import datetime, timedelta
import asyncio
from hypha_rpc import connect_to_server, login
import pyrealsense2 as rs

# Get the absolute path to the directory where the script is located
base_dir = os.path.dirname(os.path.abspath(__file__))

app = FastAPI()
templates = Jinja2Templates(directory=os.path.join(base_dir, "templates"))
app.mount("/static", StaticFiles(directory=os.path.join(base_dir, "static")), name="static")

import dotenv
dotenv.load_dotenv()

token = os.getenv("REEF_WORKSPACE_TOKEN")

# Configure logging
logging.basicConfig(level=logging.INFO)

def list_realsense_devices():
    devices = rs.context().query_devices()
    info = []
    for device in devices:
        name = device.get_info(rs.camera_info.name)
        serial = device.get_info(rs.camera_info.serial_number)
        info.append((name, serial))
    return info

def start_realsense_pipeline():
    devices = list_realsense_devices()
    if not devices:
        raise RuntimeError("No RealSense device detected")

    logging.info(
        "Detected RealSense devices: %s",
        ", ".join([f"{name} ({serial})" for name, serial in devices]),
    )

    # Always pick the first connected RealSense device automatically.
    selected_name, selected_serial = devices[0]
    logging.info("Using RealSense device: %s (%s)", selected_name, selected_serial)

    # Try common color profiles in order of preference.
    # Keep combinations valid for D4xx family to avoid "Couldn't resolve requests".
    candidate_profiles = [
        ("default", rs.format.bgr8, 30),
        (640, 480, rs.format.bgr8, 30),
        (640, 480, rs.format.rgb8, 30),
        (848, 480, rs.format.bgr8, 10),
        (1280, 720, rs.format.bgr8, 15),
    ]

    for profile in candidate_profiles:
        pipeline = rs.pipeline()
        config = rs.config()
        config.enable_device(selected_serial)
        if profile[0] == "default":
            _, color_format, fps = profile
            config.enable_stream(rs.stream.color, color_format, fps)
            profile_desc = f"default {color_format} @ {fps}fps"
        else:
            width, height, color_format, fps = profile
            config.enable_stream(rs.stream.color, width, height, color_format, fps)
            profile_desc = f"{width}x{height} {color_format} @ {fps}fps"
        try:
            pipeline.start(config)
            # Validate that frames are actually flowing; startup can succeed while stream stalls.
            # Do not fail on the first timeout because some devices need a short settle period.
            got_frame = False
            warmup_attempts = 6
            for _ in range(warmup_attempts):
                try:
                    frames = pipeline.wait_for_frames(timeout_ms=3000)
                    if frames.get_color_frame():
                        got_frame = True
                        break
                except RuntimeError:
                    continue
            if not got_frame:
                raise RuntimeError("Stream started but no color frame received during warm-up")
            logging.info(
                "RealSense stream started: %s",
                profile_desc,
            )
            return pipeline, color_format
        except RuntimeError as exc:
            try:
                pipeline.stop()
            except Exception:
                pass
            logging.warning(
                "Failed to start profile %s: %s",
                profile_desc,
                exc,
            )

    raise RuntimeError("Unable to start RealSense color stream with supported profiles")

video_dir = '/media/reef/harddisk/dorna_video'
os.makedirs(video_dir, exist_ok=True)

recording_event = Event()
recording_event.set()  # Automatically start recording
frame_bytes = None

camera = None
camera_color_format = None

def stop_camera():
    global camera, camera_color_format
    if camera is not None:
        try:
            camera.stop()
        except Exception:
            pass
    camera = None
    camera_color_format = None

def get_camera_instance():
    global camera, camera_color_format
    if camera is None:
        try:
            camera, camera_color_format = start_realsense_pipeline()
            logging.info("RealSense camera connected")
        except Exception as exc:
            logging.error(f"Failed to initialize RealSense camera: {exc}")
            camera = None
            camera_color_format = None
    return camera

def capture_frames():
    global frame_bytes, camera_color_format
    consecutive_failures = 0
    max_failures = 12
    while recording_event.is_set():
        cam = get_camera_instance()
        if cam is None:
            frame_bytes = None
            time.sleep(1.0)
            continue

        try:
            frames = cam.wait_for_frames(timeout_ms=5000)
            color_frame = frames.get_color_frame()
            if not color_frame:
                raise RuntimeError("No color frame returned by RealSense pipeline")

            frame = np.asanyarray(color_frame.get_data())

            if camera_color_format == rs.format.rgb8:
                frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)

            # Rotate 180 degrees for RealSense camera
            frame = cv2.rotate(frame, cv2.ROTATE_180)

            # Add date and time timestamp to the frame
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            cv2.putText(frame, timestamp, (frame.shape[1] - 390, 30), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2, cv2.LINE_AA)

            # Compress the image by adjusting the JPEG quality
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 50]  # Adjust quality as needed (0-100)
            ret, buffer = cv2.imencode('.jpg', frame, encode_param)
            if not ret:
                logging.error("Failed to encode image")
                frame_bytes = None  # Clear frame_bytes on error
            else:
                frame_bytes = buffer.tobytes()
                consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            logging.error(f"Error capturing frame ({consecutive_failures}/{max_failures}): {e}")
            frame_bytes = None
            if consecutive_failures >= max_failures:
                logging.warning("Restarting RealSense pipeline after repeated capture failures")
                stop_camera()
                consecutive_failures = 0
                time.sleep(2.0)
        time.sleep(0.1)  # Reduce CPU load

def gen_frames():
    global frame_bytes
    while True:
        if frame_bytes:
            yield (b'--frame\r\n'
                   b'Content-Type: image/jpeg\r\n\r\n' + frame_bytes + b'\r\n')
        time.sleep(0.1)  # Reduce CPU load

def record_time_lapse():
    global frame_bytes
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    interval = 1 / 24 * 15  # 15x speed up

    while recording_event.is_set():
        timestamp = time.strftime('%Y%m%d_%H%M%S', time.localtime())
        filename = f"time_lapse_{timestamp}.mp4"
        out = cv2.VideoWriter(os.path.join(video_dir, filename), fourcc, 24, (640, 480))

        start_time = time.time()
        duration = 30 * 60  # 30 minutes

        while time.time() - start_time < duration:
            if recording_event.is_set() and frame_bytes:
                frame = cv2.imdecode(np.frombuffer(frame_bytes, np.uint8), cv2.IMREAD_COLOR)
                if frame is not None and frame.size > 0:
                    out.write(frame)
                time.sleep(interval)
            else:
                break

        out.release()
        #logging.info(f"Time-lapse recording saved: {filename}")

    logging.info("Time-lapse recording finished")

def clean_old_videos():
    now = datetime.now()
    cutoff = now - timedelta(hours=72)
    for filename in os.listdir(video_dir):
        filepath = os.path.join(video_dir, filename)
        if os.path.isfile(filepath):
            file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
            if file_time < cutoff:
                os.remove(filepath)
                logging.info(f"Deleted old video: {filename}")

@app.get('/home')
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get('/')
async def video_feed(request: Request):
    async def generator():
        try:
            for frame in gen_frames():
                if await request.is_disconnected():
                    logging.info("Client disconnected, stopping the generator")
                    break
                yield frame
        except Exception as e:
            logging.error(f"Error in video feed: {e}")

    return StreamingResponse(generator(), media_type='multipart/x-mixed-replace; boundary=frame')

# Commented out the old standalone server code
# if __name__ == '__main__':
#     # Start a background thread to clean old videos periodically
#     def periodic_cleaning():
#         while True:
#             clean_old_videos()
#             time.sleep(3600)  # Run every hour

#     cleaning_thread = Thread(target=periodic_cleaning, daemon=True)
#     cleaning_thread.start()

#     # Start the frame capture in a background thread
#     capture_thread = Thread(target=capture_frames, daemon=True)
#     capture_thread.start()

#     # Start the time-lapse recording in a background thread
#     recording_thread = Thread(target=record_time_lapse, daemon=True)
#     recording_thread.start()

#     uvicorn.run(app, host='0.0.0.0', port=8001)  # Running on a different port

async def serve_fastapi(args, context=None):
    # context can be used for authorization, e.g., checking the user's permission
    # e.g., check user id against a list of allowed users
    scope = args["scope"]
    print(f'{context["user"]["id"]} - {scope["client"]} - {scope["method"]} - {scope["path"]}')
    await app(args["scope"], args["receive"], args["send"])

async def main():
    # Connect to Hypha server
    server = await connect_to_server({"server_url": "https://hypha.aicell.io","workspace": "reef-imaging", "token": token})

    svc_info = await server.register_service({
        "id": "reef-realsense-feed",
        "name": "reef-realsense-feed",
        "type": "asgi",
        "serve": serve_fastapi,
        "config": {"visibility": "public", "require_context": True}
    })

    print(f"Access your app at:  {server.config.public_base_url}/{server.config.workspace}/apps/{svc_info['id'].split(':')[1]}")
    await server.serve()

if __name__ == "__main__":
    # Start the frame capture in a background thread
    capture_thread = Thread(target=capture_frames, daemon=True)
    capture_thread.start()

    # Start the time-lapse recording in a background thread
    recording_thread = Thread(target=record_time_lapse, daemon=True)
    recording_thread.start()

    # Use the same pattern as other Hypha services
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
