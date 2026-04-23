# REEF Imaging - AI Agent Guide

This document provides essential information for AI coding agents working on the REEF Imaging platform.

## Project Overview

REEF Imaging is an automated microscopy platform for biological time-lapse experiments. It integrates hardware (SQUID microscopes, Dorna robotic arms, Cytomat incubators, lab cameras) via Hypha RPC services to enable fully automated imaging workflows.

### Key Capabilities
- **Automated imaging cycles**: Load → Scan → Unload samples
- **Multi-microscope support**: Up to 3 microscopes (squid-1, squid-2, squid-plus-3)
- **Time-lapse scheduling**: Configurable imaging intervals and time points
- **Live monitoring**: Multiple camera streams (USB, RealSense)
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
│   ├── orchestrator/                  # Orchestration package (6 modules)
│   │   ├── __init__.py                # Assembles mixins; entry point
│   │   ├── core.py                    # Base class: init, config I/O, admission
│   │   ├── health.py                  # Health checks, reconnection logic
│   │   ├── transport.py               # Plate transport operations
│   │   ├── tasks.py                   # Time-lapse scheduling, cycles
│   │   └── api.py                     # @schema_function Hypha endpoints
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
│   │   └── lab_cameras_watchdog.py    # Linux watchdog
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
| Hamilton Executor | `hamilton-script-executor` | Hamilton workstation via local Hypha / cloud mirror |
| Microscope 1 | `microscope-squid-1` | reef-server |
| Microscope 2 | `microscope-squid-2` | reef-server |
| Microscope 3 | `microscope-squid-plus-3` | reef-server |
| Lab Camera 1 | `reef-lab-camera-1` | reef-server |
| Lab Camera 2 | `reef-lab-camera-2` | reef-server |
| RealSense | `reef-realsense-feed` | reef-server |


### Orchestrator APIs

The orchestrator exposes the following Hypha service methods:

| Method | Description |
|--------|-------------|
| `ping()` | Health check, returns "pong" |
| `add_imaging_task(task_definition)` | Add/update an imaging task |
| `delete_imaging_task(task_name)` | Delete an imaging task |
| `pause_imaging_task(task_name)` | Pause a task |
| `resume_imaging_task(task_name)` | Resume a paused task |
| `get_all_imaging_tasks()` | Get all task configurations |
| `get_runtime_status()` | Get full runtime snapshot |
| `cancel_microscope_scan(microscope_id)` | Emergency scan cancellation |
| `halt_robotic_arm()` | Emergency robot halt |
| `scan_microscope_only(microscope_id, scan_config)` | Run scan without load/unload |
| `process_timelapse_offline(experiment_id)` | Offline stitching and upload |
| `get_lab_video_stream_urls()` | Get camera stream URLs |
| `transport_plate(from_device, to_device, slot)` | **Unified transport API** |
| `get_hamilton_status()` | Get Hamilton executor connectivity, executor status, and active Hamilton-related operations |
| `move_hamilton_plate_rail(position="hamilton")` | Move the Hamilton slide rail to the `hamilton` side (`j7=457`) or `robotic-arm` side (`j7≈30`) |
| `run_hamilton_protocol(script_content, timeout=3600)` | Start simple Hamilton script content without any built-in transport and return immediately |

**Unified Transport API:**
The `transport_plate()` method provides a single interface for all plate transport operations.

```python
# Transport between devices (slot identifies which plate when incubator involved)
await transport_plate("incubator", "microscope-squid-1", slot=5)
await transport_plate("microscope-squid-1", "incubator", slot=5)
await transport_plate("incubator", "hamilton", slot=5)
await transport_plate("hamilton", "microscope-squid-2", slot=5)
await transport_plate("microscope-squid-1", "hamilton", slot=5)
```

**Parameters:**
- `from_device`: Source device service ID (`'incubator'`, `'hamilton'`, or microscope ID)
- `to_device`: Target device service ID (`'incubator'`, `'hamilton'`, or microscope ID)
- `slot`: Incubator slot number (1-42), required when incubator is involved

**Supported device IDs:**
- `'incubator'` - The Cytomat incubator
- `'hamilton'` - The Hamilton liquid handler
- `'microscope-squid-1'` - Microscope 1
- `'microscope-squid-2'` - Microscope 2
- `'microscope-squid-plus-3'` - Microscope 3

**Robotic Arm Service API:**
```python
await robotic_arm.transport_plate(from_device="incubator", to_device="microscope-squid-1")
await robotic_arm.move_plate_rail(position="hamilton")
```

**Hamilton Execution API:**
Keep Hamilton execution separate from plate movement.

```python
# 1. Move the plate onto Hamilton
await orchestrator.transport_plate("incubator", "hamilton", slot=5)

# 2. Reassert the Hamilton-side rail position (j7=457)
await orchestrator.move_hamilton_plate_rail(position="hamilton")

# 3. Execute simple Hamilton script content on the existing executor
result = await orchestrator.run_hamilton_protocol(script_content=script_text, timeout=3600)

# 4. Poll Hamilton status until the executor is idle again
status = await orchestrator.get_hamilton_status()

# 5. Move the plate away explicitly
await orchestrator.transport_plate("hamilton", "microscope-squid-1", slot=5)
```

`run_hamilton_protocol(...)` assumes the plate is already on Hamilton. It does not load from incubator, return the plate afterward, or wait for protocol completion. It now does reassert the Hamilton-side slide-rail position before the protocol starts. The intended `script_content` should stay very simple: constants plus direct staged helper calls, with imports and helper wiring handled server-side. Use `get_hamilton_status()` to poll executor state.

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

### Active tmux Sessions (Lab Server)

The REEF lab server uses tmux sessions to manage running services:

| Session | Pane | Service | Purpose |
|---------|------|---------|---------|
| `reef` | 0 | Incubator control | Cytomat incubator service |
| `reef` | 1 | Robotic arm control | Dorna robotic arm service |
| `reef` | 2 | Orchestrator | Main orchestration engine |
| `reef` | 3-4 | Lab cameras | USB camera streams |
| `reef-mirror` | - | Mirror services | Cloud-to-local proxy for all hardware |

**Accessing sessions:**
```bash
# Attach to main reef session
tmux attach -t reef

# Attach to mirror services session
tmux attach -t reef-mirror

# Detach (keep session running): Press Ctrl+B then D
```

**Restarting services after code changes:**
When modifying robotic arm, incubator, or orchestrator code, restart the respective service in its tmux pane:
1. `tmux attach -t reef`
2. Navigate to the correct pane (Ctrl+B + arrow keys)
3. Ctrl+C to stop the service
4. Re-run the start command (e.g., `python start_hypha_service_robotic_arm.py --local`)

### Start Orchestrator
```bash
python -m reef_imaging  # Production (auto-connects local + cloud)
python -m reef_imaging.orchestrator_simulation  # No hardware simulation
```

### Critical Hardware Smoke Test
```bash
reef-hardware-smoke-test
```

**⚠️ Agent Note:** The hardware smoke test is an interactive CLI tool and cannot be run by AI agents. The user must execute this command manually in their terminal.

- This is a real hardware verification workflow, not a background diagnostic.
- It exercises incubator access, robotic arm transport, and a short scan on each configured microscope.
- Hamilton modes in this smoke test validate transport only. They do not execute Hamilton liquid-handling scripts.
- A responsible person MUST remain physically on site in the lab for the entire run.
- The script is intended for post-integration checks, post-maintenance checks, and safety validation after orchestration changes.
- It stops on the first failure and offers emergency actions to cancel a scan or halt the robot.

**Test Modes:**
The smoke test now supports multiple test modes selected via interactive prompt:

| Mode | Description |
|------|-------------|
| 1. Microscope only | Tests incubator ↔ microscope transport + scanning (default) |
| 2. Hamilton only (incubator) | Tests incubator ↔ Hamilton transport |
| 3. Hamilton only (microscope) | Tests microscope ↔ Hamilton transport |
| 4. Hamilton full cycle | Tests incubator → Hamilton → microscope → Hamilton → incubator |
| 5. Combined | Runs microscope tests followed by Hamilton full cycle |
| 6. Transportation only | Tests all transport combinations without scanning |

**Hamilton Test Flows:**
- **Incubator ↔ Hamilton:** `transport_plate("incubator", "hamilton", slot=5)` → `transport_plate("hamilton", "incubator", slot=5)`
- **Microscope ↔ Hamilton:** `transport_plate("hamilton", "microscope-squid-1", slot=5)` → `transport_plate("microscope-squid-1", "hamilton", slot=5)`
- **Full cycle:** Incubator(slot 5) → Hamilton → Microscope(squid-1) → Hamilton → Incubator(slot 5)

### Start Hardware Services
```bash
# Incubator
cd reef_imaging/control/cytomat-control
python start_hypha_service_incubator.py --local

# Robotic Arm
cd reef_imaging/control/dorna-control
python start_hypha_service_robotic_arm.py --local

# Mirror Services (cloud operation)
# These run in the 'reef-mirror' tmux session and proxy cloud requests to local services
cd reef_imaging/control/mirror-services
python mirror_incubator.py &
python mirror_robotic_arm.py &
python mirror_hamilton.py &

# Microscope mirrors (squid-control package has built-in mirroring)
# These are configured in microscope configs and run automatically with microscope services
```

**Mirror Services Summary:**
| Mirror Service | Script | Proxies to Local |
|----------------|--------|------------------|
| Incubator mirror | `mirror_incubator.py` | `incubator-control` |
| Robotic arm mirror | `mirror_robotic_arm.py` | `robotic-arm-control` |
| Hamilton mirror | `mirror_hamilton.py` | `hamilton-script-executor` |
| Microscope 1 mirror | Built-in squid-control | `microscope-squid-1` |
| Microscope 2 mirror | Built-in squid-control | `microscope-squid-2` |
| Microscope 3 mirror | Built-in squid-control | `microscope-squid-plus-3` |

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
| `mirror_hamilton_service.log` | Hamilton mirror |

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
Mirror Services (robotic arm, incubator, Hamilton)
    ↕️ (RPC)
Local Hypha Server (reef.dyn.scilifelab.se:9527)
    ↕️ (RPC)
Orchestrator ← Hardware Services (microscope, robotic arm, incubator, Hamilton)
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


Videos older than 72 hours are auto-deleted by each camera service.

## Documentation References

- Main README: `/home/tao/workspace/reef-imaging/README.md`
- Code-level docs: `/home/tao/workspace/reef-imaging/CLAUDE.md`
- Control systems: `/home/tao/workspace/reef-imaging/reef_imaging/control/README.md`
- Lab cameras: `/home/tao/workspace/reef-imaging/reef_imaging/lab_live_stream/README.md`
- Hypha platform: https://hypha.aicell.io
