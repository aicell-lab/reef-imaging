"""
Lab camera livestream service for the Linux workstation.

Registers 2 USB cameras as separate Hypha ASGI services:
  - reef-lab-camera-1
  - reef-lab-camera-2

Each service streams MJPEG video, records time-lapse MP4s, and exposes a /health endpoint.

Camera device paths are set via LAB_CAMERA_1 and LAB_CAMERA_2 env vars
(defaults: /dev/video0, /dev/video2).
"""

import os
import cv2
import time
import logging
import asyncio
import numpy as np
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
from threading import Thread, Event
from datetime import datetime, timedelta
from hypha_rpc import connect_to_server

import dotenv
dotenv.load_dotenv()

token = os.getenv("REEF_WORKSPACE_TOKEN")

VIDEO_BASE_DIR = os.getenv("LAB_VIDEO_DIR", "/media/reef/harddisk/lab_video")
HYPHA_SERVER_URL = "https://hypha.aicell.io"
HYPHA_WORKSPACE = "reef-imaging"

CAMERA_DEVICES = [
    os.getenv("LAB_CAMERA_1", "/dev/video0"),
    os.getenv("LAB_CAMERA_2", "/dev/video2"),
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class LabCamera:
    """Encapsulates state and services for a single lab camera."""

    def __init__(self, camera_index: int, device_path: str):
        self.camera_index = camera_index
        self.device_path = device_path
        self.service_id = f"reef-lab-camera-{camera_index}"
        self.video_dir = os.path.join(VIDEO_BASE_DIR, f"camera_{camera_index}")
        os.makedirs(self.video_dir, exist_ok=True)

        self.frame_bytes = None
        self.camera = None
        self.recording_event = Event()
        self.recording_event.set()
        self.connected = False

        self.app = self._create_app()

    # ------------------------------------------------------------------
    # Camera access
    # ------------------------------------------------------------------

    def _open_camera(self):
        cam = cv2.VideoCapture(self.device_path, cv2.CAP_V4L2)
        if cam.isOpened():
            cam.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            return cam
        cam.release()
        raise RuntimeError(f"Cannot open camera at {self.device_path}")

    def _get_camera_instance(self):
        if self.camera is None or not self.camera.isOpened():
            try:
                if self.camera is not None:
                    self.camera.release()
                self.camera = self._open_camera()
                self.connected = True
                logger.info(f"Camera {self.camera_index} connected at {self.device_path}")
            except Exception as e:
                logger.error(f"Camera {self.camera_index}: failed to open: {e}")
                self.camera = None
                self.connected = False
        return self.camera

    # ------------------------------------------------------------------
    # Capture thread
    # ------------------------------------------------------------------

    def capture_frames(self):
        consecutive_failures = 0
        max_failures = 10

        while self.recording_event.is_set():
            cam = self._get_camera_instance()
            if cam is None:
                time.sleep(1)
                consecutive_failures += 1
                if consecutive_failures >= max_failures:
                    logger.error(f"Camera {self.camera_index}: too many failures, waiting before retry")
                    time.sleep(5)
                    consecutive_failures = 0
                continue

            success, frame = cam.read()
            if not success:
                consecutive_failures += 1
                logger.error(f"Camera {self.camera_index}: capture failed ({consecutive_failures}/{max_failures})")
                self.frame_bytes = None
                self.connected = False
                if consecutive_failures >= max_failures:
                    logger.warning(f"Camera {self.camera_index}: reconnecting due to repeated failures")
                    self.camera = None
                    consecutive_failures = 0
                    time.sleep(2)
            else:
                consecutive_failures = 0
                self.connected = True
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                cv2.putText(
                    gray, timestamp,
                    (gray.shape[1] - 390, 30),
                    cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2, cv2.LINE_AA,
                )
                encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 50]
                ret, buffer = cv2.imencode(".jpg", gray, encode_param)
                self.frame_bytes = buffer.tobytes() if ret else None
            time.sleep(0.1)

    # ------------------------------------------------------------------
    # MJPEG generator
    # ------------------------------------------------------------------

    def _gen_frames(self):
        while True:
            if self.frame_bytes:
                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n\r\n" + self.frame_bytes + b"\r\n"
                )
            time.sleep(0.1)

    # ------------------------------------------------------------------
    # Time-lapse recording thread
    # ------------------------------------------------------------------

    def record_time_lapse(self):
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        interval = 1 / 24 * 30  # 30x speed-up

        while self.recording_event.is_set():
            timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
            filename = f"time_lapse_{timestamp}.mp4"
            out = cv2.VideoWriter(
                os.path.join(self.video_dir, filename), fourcc, 24, (640, 480)
            )
            start_time = time.time()
            duration = 30 * 60  # 30 minutes

            while time.time() - start_time < duration:
                if self.recording_event.is_set() and self.frame_bytes:
                    frame = cv2.imdecode(
                        np.frombuffer(self.frame_bytes, np.uint8), cv2.IMREAD_COLOR
                    )
                    if frame is not None and frame.size > 0:
                        out.write(frame)
                    time.sleep(interval)
                else:
                    break

            out.release()
        logger.info(f"Camera {self.camera_index}: time-lapse recording stopped")

    # ------------------------------------------------------------------
    # Video cleanup
    # ------------------------------------------------------------------

    def clean_old_videos(self):
        cutoff = datetime.now() - timedelta(hours=72)
        for filename in os.listdir(self.video_dir):
            filepath = os.path.join(self.video_dir, filename)
            if os.path.isfile(filepath):
                if datetime.fromtimestamp(os.path.getmtime(filepath)) < cutoff:
                    os.remove(filepath)
                    logger.info(f"Camera {self.camera_index}: deleted old video {filename}")

    # ------------------------------------------------------------------
    # FastAPI app
    # ------------------------------------------------------------------

    def _create_app(self):
        app = FastAPI()

        @app.get("/")
        async def video_feed(request: Request):
            async def generator():
                try:
                    for frame in self._gen_frames():
                        if await request.is_disconnected():
                            break
                        yield frame
                except Exception as e:
                    logger.error(f"Camera {self.camera_index}: video feed error: {e}")

            return StreamingResponse(
                generator(), media_type="multipart/x-mixed-replace; boundary=frame"
            )

        @app.get("/home")
        def home(request: Request):
            html = f"""<!DOCTYPE html>
<html>
<body>
  <h3>Live Streaming: Lab Camera {self.camera_index}</h3>
  <img src="./" width="50%">
</body>
</html>"""
            from fastapi.responses import HTMLResponse
            return HTMLResponse(html)

        @app.get("/health")
        def health():
            return JSONResponse({
                "status": "ok" if self.connected else "error",
                "camera_index": self.camera_index,
                "device_path": self.device_path,
                "connected": self.connected,
            })

        return app

    # ------------------------------------------------------------------
    # Hypha ASGI handler
    # ------------------------------------------------------------------

    async def serve_fastapi(self, args, context=None):
        scope = args["scope"]
        logger.debug(
            f"Camera {self.camera_index}: {context['user']['id']} - "
            f"{scope['method']} {scope['path']}"
        )
        await self.app(args["scope"], args["receive"], args["send"])

    # ------------------------------------------------------------------
    # Thread management
    # ------------------------------------------------------------------

    def start_threads(self):
        Thread(target=self.capture_frames, daemon=True).start()
        Thread(target=self.record_time_lapse, daemon=True).start()
        logger.info(f"Camera {self.camera_index}: threads started ({self.device_path})")

        def periodic_cleanup():
            while True:
                time.sleep(3600)
                self.clean_old_videos()

        Thread(target=periodic_cleanup, daemon=True).start()


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

async def main():
    cameras = [LabCamera(i + 1, path) for i, path in enumerate(CAMERA_DEVICES)]

    for cam in cameras:
        cam.start_threads()

    server = await connect_to_server({
        "server_url": HYPHA_SERVER_URL,
        "workspace": HYPHA_WORKSPACE,
        "token": token,
    })

    for cam in cameras:
        svc_info = await server.register_service({
            "id": cam.service_id,
            "name": cam.service_id,
            "type": "asgi",
            "serve": cam.serve_fastapi,
            "config": {"visibility": "public", "require_context": True},
        })
        url = f"{server.config.public_base_url}/{server.config.workspace}/apps/{svc_info['id'].split(':')[1]}"
        logger.info(f"Camera {cam.camera_index} registered: {url}")

    logger.info(f"All {len(cameras)} lab camera(s) registered. Serving...")
    await server.serve()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
