import os
import cv2
import time
import logging
import numpy as np
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from threading import Thread, Event
from datetime import datetime, timedelta
import asyncio
from hypha_rpc import connect_to_server

# Get the absolute path to the directory where the script is located
base_dir = os.path.dirname(os.path.abspath(__file__))

app = FastAPI()
templates = Jinja2Templates(directory=os.path.join(base_dir, "templates"))
static_dir = os.path.join(base_dir, "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

import dotenv

env_path = os.path.abspath(os.path.join(base_dir, "..", "..", ".env"))
if os.path.exists(env_path):
    dotenv.load_dotenv(env_path)
else:
    dotenv.load_dotenv()

token = os.getenv("REEF_WORKSPACE_TOKEN")
if not token:
    logging.error("REEF_WORKSPACE_TOKEN is not set. Expected in %s or environment.", env_path)

# Configure logging
logging.basicConfig(level=logging.INFO)

def list_camera_indexes(max_tested=10):
    available = []
    for i in range(max_tested):
        cap = cv2.VideoCapture(i, cv2.CAP_DSHOW)
        if cap.isOpened():
            available.append(i)
            cap.release()
    return available

def find_camera_index():
    preferred = os.getenv("HAMILTON_CAMERA_INDEX")
    if preferred is not None:
        try:
            return int(preferred)
        except ValueError:
            logging.warning("Invalid HAMILTON_CAMERA_INDEX=%s, falling back to auto-detect", preferred)

    available = list_camera_indexes()
    if not available:
        logging.error("No available cameras detected via OpenCV")
        return 0
    logging.info("Available camera indexes: %s", available)
    return available[0]

print("Number of available cameras:", len(list_camera_indexes()))

def get_camera():
    index = find_camera_index()
    logging.info("Opening camera index %s", index)
    cam = cv2.VideoCapture(index, cv2.CAP_DSHOW)
    if cam.isOpened():
        cam.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        return cam
    cam.release()
    raise RuntimeError(f"Could not open camera index {index}")

video_dir = os.getenv("HAMILTON_VIDEO_DIR", r"C:\reef\hamilton_video")
os.makedirs(video_dir, exist_ok=True)

recording_event = Event()
recording_event.set()  # Automatically start recording
frame_bytes = None

camera = None

def get_camera_instance():
    """Get or recreate camera instance with reconnection logic."""
    global camera
    if camera is None or not camera.isOpened():
        try:
            if camera is not None:
                camera.release()
            camera = get_camera()
            logging.info("Camera reconnected successfully")
        except Exception as e:
            logging.error("Failed to get camera: %s", e)
            camera = None
    return camera

def capture_frames():
    global frame_bytes, camera
    consecutive_failures = 0
    max_failures = 10

    while recording_event.is_set():
        cam = get_camera_instance()
        if cam is None:
            time.sleep(1)
            consecutive_failures += 1
            if consecutive_failures >= max_failures:
                logging.error("Too many camera failures, waiting before retry...")
                time.sleep(5)
                consecutive_failures = 0
            continue

        success, frame = cam.read()
        if not success:
            consecutive_failures += 1
            logging.error("Failed to capture image (failure %s/%s)", consecutive_failures, max_failures)
            frame_bytes = None
            if consecutive_failures >= max_failures:
                logging.warning("Reconnecting camera due to repeated failures...")
                camera = None
                consecutive_failures = 0
                time.sleep(2)
        else:
            consecutive_failures = 0

            # Add date and time timestamp to the frame
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            cv2.putText(frame, timestamp, (frame.shape[1] - 390, 30), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2, cv2.LINE_AA)

            # Compress the image by adjusting the JPEG quality
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 50]  # Adjust quality as needed (0-100)
            ret, buffer = cv2.imencode(".jpg", frame, encode_param)
            if not ret:
                logging.error("Failed to encode image")
                frame_bytes = None
            else:
                frame_bytes = buffer.tobytes()
        time.sleep(0.1)  # Reduce CPU load

def gen_frames():
    global frame_bytes
    while True:
        if frame_bytes:
            yield (b"--frame\r\n"
                   b"Content-Type: image/jpeg\r\n\r\n" + frame_bytes + b"\r\n")
        time.sleep(0.1)  # Reduce CPU load

def record_time_lapse():
    global frame_bytes
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    interval = 1 / 24 * 30  # 30x speed up

    while recording_event.is_set():
        timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
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
                logging.info("Deleted old video: %s", filename)

@app.get("/home")
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/")
async def video_feed(request: Request):
    async def generator():
        try:
            for frame in gen_frames():
                if await request.is_disconnected():
                    logging.info("Client disconnected, stopping the generator")
                    break
                yield frame
        except Exception as e:
            logging.error("Error in video feed: %s", e)

    return StreamingResponse(generator(), media_type="multipart/x-mixed-replace; boundary=frame")

async def serve_fastapi(args, context=None):
    # context can be used for authorization, e.g., checking the user's permission
    # e.g., check user id against a list of allowed users
    scope = args["scope"]
    print(f'{context["user"]["id"]} - {scope["client"]} - {scope["method"]} - {scope["path"]}')
    await app(args["scope"], args["receive"], args["send"])

async def ping(args, context=None):
    return("pong")

async def main():
    # Connect to Hypha server
    server = await connect_to_server({"server_url": "https://hypha.aicell.io", "workspace": "reef-imaging", "token": token})

    svc_info = await server.register_service({
        "id": "reef-hamilton-feed",
        "name": "reef-hamilton-feed",
        "type": "asgi",
        "serve": serve_fastapi,
        "ping" : ping,
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
