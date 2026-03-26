# REEF Imaging - AI Agent Guide

This document provides essential information for AI coding agents working on the REEF Imaging platform.

## Project Overview

REEF Imaging is an automated microscopy platform for biological time-lapse experiments. It integrates hardware (SQUID microscopes, Dorna robotic arms, Cytomat incubators, lab cameras) via Hypha RPC services to enable fully automated imaging workflows.

### Key Capabilities
- **Automated imaging cycles**: Load → Scan → Unload samples
- **Multi-microscope support**: Up to 3 microscopes (squid-1, squid-2, squid-plus-3)
- **Time-lapse scheduling**: Configurable imaging intervals and time points
- **Live monitoring**: Multiple camera streams (USB, RealSense, Hamilton)
- **Cloud integration**: Hypha platform for remote access and data management

## Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.11 |
| RPC Framework | Hypha (hypha-rpc 0.20.80) |
| Web Server | FastAPI/ASGI for camera streams |
| Infrastructure | Docker, Docker Compose |
| Reverse Proxy | Traefik |
| Storage | MinIO (S3-compatible) |
| Cache/Queue | Redis |
| OS Services | systemd (Linux), NSSM (Windows) |

## Project Structure

```
reef-imaging/
├── reef_imaging/                      # Main Python package
│   ├── orchestrator.py                # Main orchestration engine (~2100 lines)
│   ├── config.json                    # Task and microscope configuration
│   ├── control/                       # Hardware control modules
│   │   ├── cytomat-control/           # Cytomat incubator control
│   │   ├── dorna-control/             # Dorna robotic arm control
│   │   ├── squid-control/             # SQUID microscope (submodule)
│   │   └── mirror-services/           # Cloud-to-local proxies
│   ├── hypha_tools/                   # Data management utilities
│   │   ├── artifact_manager/          # Hypha artifact management
│   │   ├── automated_treatment_uploader.py
│   │   └── automated_stitch_uploader.py
│   ├── lab_live_stream/               # Camera streaming services
│   │   ├── lab_cameras.py             # 2× USB cameras (Linux)
│   │   ├── realsense_camera.py        # RealSense arm camera
│   │   ├── hamilton_camera.py         # Hamilton Windows camera
│   │   ├── lab_cameras_watchdog.py    # Linux watchdog
│   │   └── hamilton_watchdog.py       # Windows watchdog
│   └── utils/                         # Common utilities
├── docker/                            # Docker Compose for Hypha stack
├── traefik/                           # Reverse proxy configuration
├── docs/                              # Documentation and images
├── pyproject.toml                     # Python package configuration
├── .env-template                      # Environment variables template
└── README.md                          # Human-facing documentation
```

## Environment Setup

### Conda Environment
```bash
conda create -n reef-imaging python=3.11 -y
conda activate reef-imaging
pip install -e .
```

### Required Environment Variables

Create `.env` file in project root:

```bash
# Cloud operation (Hypha: hypha.aicell.io)
REEF_WORKSPACE_TOKEN=your_cloud_token

# Local operation (lab server: reef.dyn.scilifelab.se:9527)
REEF_LOCAL_TOKEN=your_local_token
REEF_LOCAL_WORKSPACE=your_local_workspace

# Optional: Cytomat serial port
CYPOMAT_SERIAL_PORT=/dev/ttyUSB0

# Optional: Camera name pattern
LAB_CAMERA_NAME_PATTERN="HD USB Camera"
LAB_VIDEO_DIR=/media/reef/harddisk/lab_video
```

## Key Configuration Files

### config.json
Located at `reef_imaging/config.json`. Defines samples and microscopes:

```json
{
    "samples": [
        {
            "name": "experiment-name",
            "settings": {
                "scan_mode": "full_automation",
                "saved_data_type": "raw_images_well_plate",
                "incubator_slot": 3,
                "allocated_microscope": "microscope-squid-1",
                "pending_time_points": ["2025-01-01T10:00:00"],
                "imaged_time_points": [],
                "wells_to_scan": ["A1", "B2"],
                "Nx": 3,
                "Ny": 3,
                "dx": 0.3,
                "dy": 0.3,
                "illumination_settings": [...],
                "do_contrast_autofocus": true,
                "do_reflection_af": false,
                "focus_map_points": [[x,y,z], [x,y,z], [x,y,z]]
            },
            "operational_state": {"status": "pending"}
        }
    ],
    "microscopes": [
        {"id": "microscope-squid-1"},
        {"id": "microscope-squid-2"},
        {"id": "microscope-squid-plus-3"}
    ]
}
```

**Scan Modes:**
- `full_automation`: Uses incubator + robotic arm + microscope
- `microscope_only`: Imaging only, no transport operations

**Data Types:**
- `raw_images_well_plate`: Grid-based well plate scanning
- `raw_image_flexible`: Custom position-based imaging

**Task Status Values:** `pending`, `active`, `completed`, `uploading`, `paused`, `error`

## Hypha Service Registration Pattern

All services follow this registration pattern:

```python
from hypha_rpc import connect_to_server
from hypha_rpc.utils.schema import schema_function

server = await connect_to_server({
    "server_url": "https://hypha.aicell.io",
    "workspace": "reef-imaging",
    "token": os.getenv("REEF_WORKSPACE_TOKEN"),
    "ping_interval": 30,
})

svc_info = await server.register_service({
    "id": "service-id",
    "name": "Service Name",
    "config": {"visibility": "protected", "run_in_executor": True},
    "ping": self.ping,
    "method_name": self.method_name,  # Add @schema_function(skip_self=True)
})

await server.serve()  # blocks
```

### Service Visibility
- `"protected"`: Orchestrator, hardware controls
- `"public"`: Camera streams (ASGI/FastAPI)

### Hypha Service IDs

| Service | ID | Machine |
|---------|-----|---------|
| Orchestrator | `orchestrator-manager` | reef-server |
| Incubator | `incubator-control` | reef-server |
| Robotic Arm | `robotic-arm-control` | reef-server |
| Microscope 1 | `microscope-squid-1` | reef-server |
| Microscope 2 | `microscope-squid-2` | reef-server |
| Microscope 3 | `microscope-squid-plus-3` | reef-server |
| Lab Camera 1 | `reef-lab-camera-1` | reef-server |
| Lab Camera 2 | `reef-lab-camera-2` | reef-server |
| RealSense | `reef-realsense-feed` | reef-server |
| Hamilton Cam | `reef-hamilton-feed` | Hamilton Windows PC |

### Microscope Busy-State Management

The microscope service implements centralized busy guards to prevent conflicting operations. The service tracks active operations by scope (`hardware`, `processing`) and rejects conflicting calls.

**New RPC Methods:**
- `get_busy_status()` - Returns the current busy state without full status
  ```python
  {
      "busy_status": "idle",  # or "hardware", "processing", "both"
      "hardware_busy": False,
      "processing_busy": False
  }
  ```

**Enhanced Methods:**
- `get_status()` now includes:
  ```python
  {
      # ... existing fields ...
      "busy_status": "idle",      # "idle", "hardware", "processing", "both"
      "hardware_busy": False,
      "processing_busy": False
  }
  ```
- `scan_get_status()` now includes:
  ```python
  {
      "success": True,
      "state": "running",         # "idle", "running", "completed", "failed"
      "busy_status": "processing", # Current busy state during scan
      "error_message": None,
      "saved_data_type": "raw_images_well_plate"
  }
  ```

**Error Handling:**
Conflicting operations now fail with `MicroscopeBusyError` whose message starts with `MICROSCOPE_BUSY`. Clients should check for this prefix to handle busy states appropriately.

**Scan Cancellation Behavior Change:**
`scan_cancel()` no longer force-cancels the asyncio task immediately. Instead:
1. Returns `"Scan cancellation requested; scan is stopping in the background"`
2. The scan continues running in the background until it actually exits
3. Clients should poll `scan_get_status()` to confirm the scan has stopped
4. Do not assume cancel means immediate idle - the microscope may still be busy

## Running Services

### Start Infrastructure
```bash
# Create Docker network
docker network create hypha-app-engine

# Start Hypha stack (Redis, MinIO, Hypha server)
cd docker && docker-compose up -d

# Start Traefik (if using external access)
cd traefik && chmod 600 acme/acme.json && docker-compose up -d
```

### Start Orchestrator
```bash
cd reef_imaging
python orchestrator.py  # Production (cloud mode)
python orchestrator.py --local  # Local development
python orchestrator_simulation.py --local  # No hardware simulation
```

### Critical Hardware Smoke Test
```bash
reef-hardware-smoke-test
```

- This is a real hardware verification workflow, not a background diagnostic.
- It exercises incubator access, robotic arm transport, and a short scan on each configured microscope.
- A responsible person MUST remain physically on site in the lab for the entire run.
- The script is intended for post-integration checks, post-maintenance checks, and safety validation after orchestration changes.
- It stops on the first failure and offers emergency actions to cancel a scan or halt the robot.

### Start Hardware Services
```bash
# Incubator
cd reef_imaging/control/cytomat-control
python start_hypha_service_incubator.py --local

# Robotic Arm
cd reef_imaging/control/dorna-control
python start_hypha_service_robotic_arm.py --local

# Mirror Services (cloud operation)
cd reef_imaging/control/mirror-services
python mirror_incubator.py &
python mirror_robotic_arm.py &
```

### Start Camera Services
```bash
# Lab cameras (Linux)
python reef_imaging/lab_live_stream/lab_cameras.py

# Via systemd
sudo systemctl start lab-cameras
sudo systemctl start lab-cameras-watchdog
```

## Code Conventions

### Logging
All services use rotating file handlers:
```python
import logging.handlers

def setup_logging(log_file="service.log", max_bytes=10*1024*1024, backup_count=5):
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger
```

### Exposing Methods as Hypha Tools
Use `@schema_function(skip_self=True)` decorator:
```python
from hypha_rpc.utils.schema import schema_function

@schema_function(skip_self=True)
async def my_method(self, param: str = Field(..., description="Description")):
    """Docstring appears in Hypha service schema."""
    return result
```

When adding to orchestrator, also add to `service_api` dict in `_register_self_as_hypha_service()`.

### Environment Variable Loading
```python
import dotenv

dotenv.load_dotenv()
ENV_FILE = dotenv.find_dotenv()
if ENV_FILE:
    dotenv.load_dotenv(ENV_FILE)
```

### Atomic File Writes
```python
# Write to temp file, then atomic rename
with open('config.json.tmp', 'w') as f:
    json.dump(data, f, indent=4)
os.replace('config.json.tmp', 'config.json')
```

## Testing & Monitoring

### Service Health Checks
- GitHub Actions workflow runs every 9 minutes (`.github/workflows/service_check.yml`)
- Orchestrator performs health checks every 30 seconds
- Camera watchdogs restart services if health checks fail
- Real hardware smoke tests are manual and operator-attended; they must not be treated as unattended automation

### Log Files
| Log File | Service |
|----------|---------|
| `orchestrator.log` | Orchestrator |
| `incubator_service.log` | Incubator control |
| `robotic_arm_service.log` | Robotic arm control |
| `mirror_incubator_service.log` | Incubator mirror |
| `mirror_robotic_arm_service.log` | Robotic arm mirror |

### Local Test URLs
```
http://reef.dyn.scilifelab.se:9527/reef-imaging/services/orchestrator-manager/ping
http://reef.dyn.scilifelab.se:9527/reef-imaging/services/incubator-control/ping
http://reef.dyn.scilifelab.se:9527/reef-imaging/services/robotic-arm-control/ping
```

### Cloud URLs
```
https://hypha.aicell.io/reef-imaging/services/orchestrator-manager/ping
https://hypha.aicell.io/reef-imaging/apps/reef-lab-camera-1
https://hypha.aicell.io/reef-imaging/apps/reef-lab-camera-2
```

## Security Considerations

1. **Token Management**: Never commit tokens to git. Use `.env` file (gitignored).

2. **Service Visibility**: 
   - Hardware controls: `protected`
   - Camera streams: `public` (for viewing)

3. **Critical Operations**: Orchestrator marks robotic arm movements and microscope scans as critical. Health check failures during critical operations cause program exit (safety measure).

4. **HTTPS/TLS**: Traefik handles SSL termination. Ensure `acme.json` has 600 permissions.

5. **S3 Credentials**: MinIO credentials in `.env` should be strong in production.

## Development Guidelines

### Adding New Orchestrator Tools
1. Create method in `OrchestrationSystem` class with `@schema_function(skip_self=True)`
2. Add entry to `service_api` dict in `_register_self_as_hypha_service()`
3. Include comprehensive docstring (appears in Hypha schema)
4. Add validation using pydantic `Field` for parameters

### Adding New Microscope Support
1. Add microscope ID to `config.json` under `"microscopes"`
2. Map robot target ID in `_get_robot_microscope_id()` method
3. Ensure microscope service implements required APIs (`scan_start`, `scan_get_status`, `home_stage`, `return_stage`)

### Handling Critical Operations
Mark services as critical during dangerous operations:
```python
self.in_critical_operation = True
critical_services = [
    ('robotic_arm', self.robotic_arm_id),
    ('microscope', microscope_id),
]
self._mark_critical_services(critical_services)

try:
    # Perform operation
    pass
finally:
    self.in_critical_operation = False
    self._unmark_critical_services(critical_services)
```

## Common Issues

1. **Health check failures during critical operations**: System exits for safety. Check hardware state before restarting.

2. **Config file corruption**: System uses atomic writes (`config.json.tmp` → `config.json`). If corruption occurs, restore from backup.

3. **Service registration failures**: Check REEF_WORKSPACE_TOKEN and network connectivity to Hypha server.

4. **Microscope connection issues**: Ensure squid-control package is installed in editable mode with mirror support.

## Communication Flow

```
Cloud (Hypha: hypha.aicell.io)
    ↕️ (RPC)
Mirror Services (robotic arm, incubator)
    ↕️ (RPC)
Local Hypha Server (reef.dyn.scilifelab.se:9527)
    ↕️ (RPC)
Orchestrator ← Hardware Services (microscope, robotic arm, incubator)
    ↕️
Physical Hardware
```

Note: Microscope has built-in mirror functionality in squid-control package.

## Video Storage Paths

| Source | Path |
|--------|------|
| Lab Camera 1 | `/media/reef/harddisk/lab_video/camera_1` |
| Lab Camera 2 | `/media/reef/harddisk/lab_video/camera_2` |
| RealSense | `/media/reef/harddisk/dorna_video` |
| Hamilton | `C:\reef\hamilton_video` (Windows) |

Videos older than 72 hours are auto-deleted by each camera service.

## Documentation References

- Main README: `/home/tao/workspace/reef-imaging/README.md`
- Code-level docs: `/home/tao/workspace/reef-imaging/CLAUDE.md`
- Control systems: `/home/tao/workspace/reef-imaging/reef_imaging/control/README.md`
- Lab cameras: `/home/tao/workspace/reef-imaging/reef_imaging/lab_live_stream/README.md`
- Hypha platform: https://hypha.aicell.io
