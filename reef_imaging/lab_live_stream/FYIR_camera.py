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
#list all available cameras
def count_available_cameras():
    """Count cameras by checking /sys/class/video4linux devices"""
    import glob
    count = 0
    video_devices = sorted(glob.glob('/sys/class/video4linux/video*'))
    for device_path in video_devices:
        try:
            name_file = os.path.join(device_path, 'name')
            if os.path.exists(name_file):
                count += 1
        except Exception:
            pass
    return count

def get_camera_by_name(name_pattern):
    """Find camera device by name pattern (e.g., 'RealSense', 'HD USB Camera')"""
    import glob
    video_devices = sorted(glob.glob('/sys/class/video4linux/video*'))
    for device_path in video_devices:
        try:
            name_file = os.path.join(device_path, 'name')
            if os.path.exists(name_file):
                with open(name_file, 'r') as f:
                    device_name = f.read().strip()
                    if name_pattern.lower() in device_name.lower():
                        device_num = device_path.split('video')[-1]
                        device_path = f"/dev/video{device_num}"
                        logging.info(f"Found camera '{device_name}' at {device_path}")
                        cam = cv2.VideoCapture(device_path)
                        if cam.isOpened():
                            cam.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                            return cam
                        cam.release()
        except Exception as e:
            logging.warning(f"Error checking device {device_path}: {e}")
    
    raise RuntimeError(f"Could not find camera matching '{name_pattern}'")

print("Number of available cameras:", count_available_cameras())

def get_camera():
    return get_camera_by_name("HD USB Camera")

video_dir = '/media/reef/harddisk/lab_video'
os.makedirs(video_dir, exist_ok=True)

recording_event = Event()
recording_event.set()  # Automatically start recording
frame_bytes = None

camera = None

def get_camera_instance():
    """Get or recreate camera instance with reconnection logic"""
    global camera
    if camera is None or not camera.isOpened():
        try:
            if camera is not None:
                camera.release()
            camera = get_camera()
            logging.info("Camera reconnected successfully")
        except Exception as e:
            logging.error(f"Failed to get camera: {e}")
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
            logging.error(f"Failed to capture image (failure {consecutive_failures}/{max_failures})")
            frame_bytes = None
            if consecutive_failures >= max_failures:
                logging.warning("Reconnecting camera due to repeated failures...")
                camera = None
                consecutive_failures = 0
                time.sleep(2)
        else:
            consecutive_failures = 0
            # Convert to grayscale (infrared cameras often work best in grayscale)
            gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

            # Add date and time timestamp to the frame
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            cv2.putText(gray_frame, timestamp, (gray_frame.shape[1] - 390, 30), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2, cv2.LINE_AA)

            # Compress the image by adjusting the JPEG quality
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 50]  # Adjust quality as needed (0-100)
            ret, buffer = cv2.imencode('.jpg', gray_frame, encode_param)
            if not ret:
                logging.error("Failed to encode image")
                frame_bytes = None  # Clear frame_bytes on error
            else:
                frame_bytes = buffer.tobytes()
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
    interval = 1 / 24 * 30  # 30x speed up

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
    return templates.TemplateResponse("index_FYIR.html", {"request": request})

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

#     uvicorn.run(app, host='0.0.0.0', port=8002)  # Running on a different port

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
        "id": "reef-live-feed",
        "name": "reef-live-feed",
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