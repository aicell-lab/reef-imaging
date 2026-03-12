# Reef Imaging

This is the main Python package for the REEF Imaging platform, containing orchestration logic, hardware control services, data management utilities, and supplementary tools.

## Key Components

### 1. Orchestration System

#### `orchestrator.py`
The main orchestration engine that coordinates all hardware and manages time-lapse experiments.

**Core Classes**:
- `OrchestrationSystem`: Main system class with task management and service coordination

**Key Features**:
- Task loading and scheduling from `config.json`
- Service proxy management (microscope, robotic arm, incubator)
- Transport queue for serialized load/unload operations
- Health monitoring with automatic reconnection (30-second intervals)
- Critical operation protection to prevent mid-operation shutdowns
- Atomic config writes (`config.json.tmp` → `config.json`) to prevent corruption
- Comprehensive error handling and logging

**Exposed Hypha Tool APIs** (decorated with `@schema_function`):
- `ping()` — health check
- `add_imaging_task(task_definition)` — add or update a task in `config.json`
- `delete_imaging_task(task_name)` — remove a task
- `pause_imaging_task(task_name)` / `resume_imaging_task(task_name)` — pause/resume scheduling
- `get_all_imaging_tasks()` — list all tasks from `config.json`
- `get_transport_queue_status()` — queue depth and worker state
- `get_lab_video_stream_urls()` — public Hypha URLs for all camera feeds
- `process_timelapse_offline_api(experiment_id)` — offline stitch + upload
- `scan_microscope_only_api(microscope_id, scan_config)` — scan without transport

#### `orchestrator_simulation.py`
Simulation version for testing without hardware. Provides mock responses for all hardware operations.

### 2. Hardware Control (`control/`)

#### Robotic Arm (`dorna-control/`)

**Main Files**:
- `start_hypha_service_robotic_arm.py` — Hypha service wrapper
- `dorna_controller.py` — Direct robot control interface
- `paths/` — JSON files with predefined movement paths

**Hypha service ID**: `robotic-arm-control` (hardcoded in orchestrator)

**Key APIs**:
- `incubator_to_microscope(incubator_slot)`
- `microscope_to_incubator(incubator_slot)`
- `get_status()`

#### Incubator (`cytomat-control/`)

**Main Files**:
- `start_hypha_service_incubator.py` — Hypha service wrapper
- `samples.json` — Sample metadata and tracking database

**Hypha service ID**: `incubator-control` (hardcoded in orchestrator)

**Key APIs**:
- `put_sample_from_transfer_station_to_slot(slot_id, sample_info)`
- `get_sample_from_slot_to_transfer_station(slot_id)`
- `get_status()` — system health and current operations
- `get_temperature()` / `get_co2_level()`
- `add_sample(slot_id, sample_info)` / `remove_sample(slot_id)`

#### Microscope (`squid-control/`)
External package with built-in mirror functionality. See: https://github.com/aicell-lab/squid-control

**Hypha service IDs** (configured in `config.json`):
- `microscope-squid-1`
- `microscope-squid-2`
- `microscope-squid-plus-3`

#### Mirror Services (`mirror-services/`)

**Files**:
- `mirror_robotic_arm.py` — Cloud-to-local proxy for robotic arm
- `mirror_incubator.py` — Cloud-to-local proxy for incubator

Note: The microscope no longer needs a mirror service — `squid_control` includes built-in mirror functionality.

### 3. Data Management (`hypha_tools/`)

#### Artifact Manager (`artifact_manager/`)
Tools for uploading and organizing data on the Hypha platform.

**Key Features**:
- Concurrent batch file uploads
- Gallery and dataset creation
- Zarr file channel-based uploads
- Resume capability for interrupted transfers

#### Automated Uploaders
- `automated_treatment_uploader.py` — Uploads time-lapse experiment data with metadata
- `automated_stitch_uploader.py` — Processes and uploads stitched images

### 4. Lab Live Stream (`lab_live_stream/`)

**Files**:
- `lab_cameras.py` — Auto-detects + registers 2 USB lab cameras
- `realsense_camera.py` — RealSense camera for robotic arm
- `hamilton_camera.py` — Hamilton Windows camera
- `lab_cameras_watchdog.py` — Linux watchdog (systemctl restart)
- `hamilton_watchdog.py` — Windows watchdog (sc stop/start)

### 5. Supplementary Tools (`tools/`)
Image stitching, multi-resolution pyramid generation, format conversion (TIFF, OME-Zarr), channel merging, intensity normalization.

### 6. Utilities (`utils/`)
Common helper functions used across the package.

## Configuration

### Task Configuration (`config.json`)

The orchestrator reads from `reef_imaging/config.json`. This file is gitignored and must exist at runtime.

```json
{
    "samples": [
        {
            "name": "my-experiment",
            "settings": {
                "scan_mode": "full_automation",
                "saved_data_type": "raw_images_well_plate",
                "incubator_slot": 3,
                "allocated_microscope": "microscope-squid-1",
                "time_start_imaging": "2025-01-01T10:00:00",
                "time_end_imaging": "2025-01-07T10:00:00",
                "imaging_interval": 3600,
                "wells_to_scan": ["A1", "B2"],
                "Nx": 3,
                "Ny": 3,
                "dx": 0.3,
                "dy": 0.3,
                "illumination_settings": [...],
                "do_contrast_autofocus": true,
                "do_reflection_af": false
            },
            "operational_state": {
                "status": "pending"
            }
        }
    ],
    "microscopes": [
        { "id": "microscope-squid-1" },
        { "id": "microscope-squid-2" },
        { "id": "microscope-squid-plus-3" }
    ]
}
```

**Scan Modes**:
- `full_automation` — uses incubator + robotic arm + microscope
- `microscope_only` — imaging only, no transport

**Data Types**:
- `raw_images_well_plate` — grid-based well plate scanning
- `raw_image_flexible` — custom position-based imaging

**Task Status Values**: `pending`, `started`, `completed`, `uploading`, `paused`, `error`

**Notes**:
- `incubator-control` and `robotic-arm-control` IDs are hardcoded in orchestrator — not in `config.json`
- Microscope IDs must match entries in the `"microscopes"` array

## Usage

### Starting the Orchestrator

**Production** (cloud operation):
```bash
cd reef_imaging
python orchestrator.py
```

**Local Development**:
```bash
python orchestrator.py --local
```

**Simulation** (no hardware):
```bash
python orchestrator_simulation.py --local
```

### Starting Hardware Services

**Incubator**:
```bash
cd reef_imaging/control/cytomat-control
python start_hypha_service_incubator.py
```

**Robotic Arm**:
```bash
cd reef_imaging/control/dorna-control
python start_hypha_service_robotic_arm.py
```

**Mirror Services** (for cloud operation):
```bash
cd reef_imaging/control/mirror-services
python mirror_incubator.py &
python mirror_robotic_arm.py &
```

## Logging & Monitoring

Rotating log files (10 MB max, 5 backups):

| Log File | Service |
|----------|---------|
| `orchestrator.log` | Orchestrator |
| `incubator_service.log` | Incubator control |
| `robotic_arm_service.log` | Robotic arm control |
| `mirror_incubator_service.log` | Incubator mirror |
| `mirror_robotic_arm_service.log` | Robotic arm mirror |

- Health checks every 30 seconds with automatic reconnection
- Critical operation protection prevents shutdown mid-transfer
