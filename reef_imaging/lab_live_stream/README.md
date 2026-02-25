# Lab Live Stream Cameras

This folder contains the camera services used by the reef imaging setup.

Notes:
- FYIR camera is for the lab room. This service runs on the reef local server.
- RealSense camera is on the robotic arm. This service runs on the reef local server.
- The Hamilton camera service runs on the Hamilton Windows computer.

## Hamilton Windows Camera

The Hamilton camera service is implemented in `hamilton_camera.py`.

Configuration:
- `HAMILTON_CAMERA_INDEX` sets the OpenCV camera index (default: first available).
- `HAMILTON_VIDEO_DIR` sets where time-lapse videos are stored (default: `C:\reef\hamilton_video`).

### Windows Service (NSSM)

Service name: `reef-hamilton-camera`

Install command (run as Administrator):
```powershell
& "C:\Program Files\nssm\nssm.exe" install reef-hamilton-camera "C:\Miniconda3\envs\reef-imaging\python.exe" "reef_imaging\lab_live_stream\hamilton_camera.py"
& "C:\Program Files\nssm\nssm.exe" set reef-hamilton-camera AppDirectory "C:\Users\Hamilton\workspace\reef-imaging"
& "C:\Program Files\nssm\nssm.exe" set reef-hamilton-camera Start SERVICE_AUTO_START
```

Start the service:
```bat
sc start reef-hamilton-camera
```

Stop the service:
```bat
sc stop reef-hamilton-camera
```

Check status:
```bat
sc query reef-hamilton-camera
```
