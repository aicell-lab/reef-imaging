# CLAUDE.md — Reef Imaging

Context for AI assistants working in this repository.

## What this project is

Automated microscopy platform for biological time-lapse experiments. Hardware (SQUID microscope, Dorna robotic arm, Cytomat incubator, lab cameras) is controlled via Hypha RPC services. The orchestrator schedules imaging tasks, coordinates plate transport, and manages health monitoring.

## Key conventions

### Hypha service registration

All services follow the same pattern:

```python
server = await connect_to_server({
    "server_url": "https://hypha.aicell.io",
    "workspace": "reef-imaging",
    "token": os.getenv("REEF_WORKSPACE_TOKEN"),
    "ping_interval": 30,
})
svc_info = await server.register_service({
    "id": "service-id",
    "name": "service-id",
    "type": "asgi",          # for FastAPI/MJPEG streams
    "serve": serve_fastapi,
    "config": {"visibility": "public", "require_context": True},
})
await server.serve()  # blocks
```

Public URL pattern: `https://hypha.aicell.io/reef-imaging/apps/{service_id}`

### Orchestrator tools

Tools exposed by the orchestrator (`reef_imaging/orchestrator/`) are decorated with `@schema_function(skip_self=True)` and registered in `service_api` inside `_register_self_as_hypha_service()`. Always add both the method and the `service_api` dict entry when adding a new tool. The orchestrator is split into mixins (`core.py`, `health.py`, `transport.py`, `tasks.py`, `api.py`) assembled in `__init__.py`.

Orchestrator service visibility is `"protected"`. Camera services are `"public"`.

### Python environment

- Conda env: `reef-imaging`
- Python: `/home/tao/home/tao/software/miniconda3/envs/reef-imaging/bin/python3`
- Project root: `/home/tao/workspace/reef-imaging`
- Environment variables loaded from `.env` at project root

### Systemd services (Linux reef-server)

| Service | Script | Purpose |
|---------|--------|---------|
| `lab-cameras` | `reef_imaging/lab_live_stream/lab_cameras.py` | 2× USB lab camera streams |
| `lab-cameras-watchdog` | `reef_imaging/lab_live_stream/lab_cameras_watchdog.py` | Restarts lab-cameras if unhealthy |

## Hypha service IDs

| Service | ID | Machine |
|---------|-----|---------|
| Orchestrator | `orchestrator-manager` | reef-server |
| Lab camera 1 | `reef-lab-camera-1` | reef-server |
| Lab camera 2 | `reef-lab-camera-2` | reef-server |
| RealSense arm cam | `reef-realsense-feed` | reef-server |
| Hamilton Executor | `hamilton-script-executor` | Hamilton workstation via local Hypha / cloud mirror |


## File structure highlights

```
reef_imaging/
├── orchestrator/                # Orchestration package (6 modules)
│   ├── __init__.py              # Assembles mixins; entry point via `python -m reef_imaging`
│   ├── core.py                  # Base class: init, config I/O, admission helpers
│   ├── health.py                # Health checks, reconnection logic
│   ├── transport.py             # Plate transport operations
│   ├── tasks.py                 # Time-lapse scheduling, cycle execution
│   └── api.py                   # @schema_function Hypha endpoints
├── hypha_service.py
├── lab_live_stream/
│   ├── lab_cameras.py           # Auto-detects + registers 2 USB lab cameras
│   ├── realsense_camera.py      # RealSense for robotic arm
│   ├── lab_cameras_watchdog.py  # Linux watchdog (systemctl restart)
│   └── README.md
├── control/
│   ├── squid-control/           # SQUID microscope (git submodule / separate package)
│   ├── dorna-control/           # Dorna robotic arm
│   ├── cytomat-control/         # Cytomat incubator
│   └── mirror-services/         # Cloud↔local proxies for arm, incubator, Hamilton
└── hypha_tools/
    ├── artifact_manager/
    ├── automated_treatment_uploader.py
    └── automated_stitch_uploader.py
```

## Video storage paths (Linux)

| Source | Path |
|--------|------|
| Lab camera 1 | `/media/reef/harddisk/lab_video/camera_1` |
| Lab camera 2 | `/media/reef/harddisk/lab_video/camera_2` |
| RealSense | `/media/reef/harddisk/dorna_video` |
| Hamilton | `C:\reef\hamilton_video` (Windows) |

Videos older than 72 hours are auto-deleted by each camera service.
