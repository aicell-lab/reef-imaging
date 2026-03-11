# Lab Live Stream Cameras

This folder contains camera services for the reef imaging lab setup.

## Services Overview

| File | Camera | Machine | Hypha Service ID |
|------|--------|---------|-----------------|
| `lab_cameras.py` | 2× USB lab cameras (Linux) | reef-server | `reef-lab-camera-1`, `reef-lab-camera-2` |
| `realsense_camera.py` | RealSense RGB-D (robotic arm) | reef-server | `reef-realsense-feed` |
| `hamilton_camera.py` | USB camera (Hamilton) | Hamilton Windows PC | `reef-hamilton-feed` |

## Lab Cameras (Linux — `lab_cameras.py`)

Auto-detects up to 2 USB cameras matching the name pattern `"HD USB Camera"` (configurable via `LAB_CAMERA_NAME_PATTERN` env var) and registers each as a separate Hypha ASGI service.

Each service exposes:
- `/` — MJPEG live video stream
- `/home` — HTML viewer page
- `/health` — JSON health status (`{"status": "ok/error", "connected": true/false, ...}`)

Time-lapse MP4s are recorded to `/media/reef/harddisk/lab_video/camera_1` and `camera_2` (configurable via `LAB_VIDEO_DIR`). Videos older than 72 hours are automatically deleted.

### Linux Systemd Service

Service name: `lab-cameras`

Install (run once as root):
```bash
sudo tee /etc/systemd/system/lab-cameras.service << 'EOF'
[Unit]
Description=Reef Lab Cameras Livestream Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=tao
WorkingDirectory=/home/tao/workspace/reef-imaging
EnvironmentFile=/home/tao/workspace/reef-imaging/.env
ExecStart=/home/tao/home/tao/software/miniconda3/envs/reef-imaging/bin/python3 reef_imaging/lab_live_stream/lab_cameras.py
Restart=on-failure
RestartSec=15

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable lab-cameras
sudo systemctl start lab-cameras
```

Check status / logs:
```bash
systemctl status lab-cameras
journalctl -u lab-cameras -f
```

### Watchdog (`lab_cameras_watchdog.py`)

Polls `/health` on both `reef-lab-camera-1` and `reef-lab-camera-2` every 60 seconds. If either fails, runs `systemctl restart lab-cameras`. Configurable via `LAB_CAMERAS_SERVICE_NAME` env var.

Install watchdog service:
```bash
sudo tee /etc/systemd/system/lab-cameras-watchdog.service << 'EOF'
[Unit]
Description=Reef Lab Cameras Watchdog
After=network-online.target lab-cameras.service
Wants=network-online.target

[Service]
Type=simple
User=tao
WorkingDirectory=/home/tao/workspace/reef-imaging
EnvironmentFile=/home/tao/workspace/reef-imaging/.env
ExecStart=/home/tao/home/tao/software/miniconda3/envs/reef-imaging/bin/python3 reef_imaging/lab_live_stream/lab_cameras_watchdog.py
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable lab-cameras-watchdog
sudo systemctl start lab-cameras-watchdog
```

---

## Hamilton Camera (Windows — `hamilton_camera.py`)

Configuration:
- `HAMILTON_CAMERA_INDEX` — OpenCV camera index (default: first available)
- `HAMILTON_VIDEO_DIR` — time-lapse storage directory (default: `C:\reef\hamilton_video`)

### Windows Service (NSSM)

Service name: `reef-hamilton-camera`

Install (run as Administrator):
```powershell
& "C:\Program Files\nssm\nssm.exe" install reef-hamilton-camera "C:\Users\Hamilton\Miniconda3\envs\reef-imaging\python.exe" "reef_imaging\lab_live_stream\hamilton_camera.py"
& "C:\Program Files\nssm\nssm.exe" set reef-hamilton-camera AppDirectory "C:\Users\Hamilton\workspace\reef-imaging"
& "C:\Program Files\nssm\nssm.exe" set reef-hamilton-camera Start SERVICE_AUTO_START
```

```bat
sc start reef-hamilton-camera
sc stop reef-hamilton-camera
sc query reef-hamilton-camera
```

### Watchdog (`hamilton_watchdog.py`)

Polls `https://hypha.aicell.io/reef-imaging/apps/reef-hamilton-feed/health` every 60 seconds and restarts `reef-hamilton-camera` via `sc stop/start` if the check fails.

**Must run as Administrator.**

Service name: `reef-hamilton-watchdog`

Install (run as Administrator):
```powershell
& "C:\Program Files\nssm\nssm.exe" install reef-hamilton-watchdog "C:\Users\Hamilton\Miniconda3\envs\reef-imaging\python.exe" "reef_imaging\lab_live_stream\hamilton_watchdog.py"
& "C:\Program Files\nssm\nssm.exe" set reef-hamilton-watchdog AppDirectory "C:\Users\Hamilton\workspace\reef-imaging"
& "C:\Program Files\nssm\nssm.exe" set reef-hamilton-watchdog Start SERVICE_AUTO_START
& "C:\Program Files\nssm\nssm.exe" set reef-hamilton-watchdog ObjectName LocalSystem
```

```bat
sc start reef-hamilton-watchdog
```

---

## RealSense Camera (`realsense_camera.py`)

RealSense RGB-D camera mounted on the Dorna robotic arm. Runs on the Linux reef-server alongside the lab cameras.

Requires `pyrealsense2`. Time-lapses saved to `/media/reef/harddisk/dorna_video`.

---

## Getting Stream URLs

Use the orchestrator tool `get_lab_video_stream_urls` to retrieve all current public Hypha URLs for every stream service.
