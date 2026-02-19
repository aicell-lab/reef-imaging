# Reef Imaging

This is the main Python package for the REEF Imaging platform, containing orchestration logic, hardware control services, data management utilities, and supplementary tools.

## Key Components

### 1. Orchestration System

#### `orchestrator.py` (2115 lines)
The main orchestration engine that coordinates all hardware and manages time-lapse experiments.

**Core Classes**:
- `OrchestrationSystem`: Main system class with task management and service coordination
- `TransportQueue`: Async queue for serializing sample transport operations

**Key Features**:
- Task loading and scheduling from `config.json`
- Service proxy management (microscope, robotic arm, incubator)
- Transport queue for load/unload operations
- Health monitoring with automatic reconnection (30-second intervals)
- Critical operation protection to prevent mid-operation shutdowns
- Configuration persistence and state tracking
- Comprehensive error handling and logging

**Main APIs**:
- `start_orchestration()` - Main workflow loop
- `handle_task()` - Execute imaging for a specific task
- `load_sample()` / `unload_sample()` - Sample transport operations
- `get_task_list()` - Retrieve all configured tasks
- `get_status()` - System status and health check

#### `orchestrator_simulation.py`
Simulation version for testing without hardware. Provides mock responses for all hardware operations.

### 2. Hardware Control (`control/`)

#### Robotic Arm (`dorna-control/`)

**Main Files**:
- `start_hypha_service_robotic_arm.py` - Hypha service wrapper (569 lines)
- `dorna_controller.py` - Direct robot control interface
- `paths/` - JSON files with predefined movement paths

**Features**:
- Preconfigured paths for safety and repeatability
- Transport operations between incubator and microscope
- Status monitoring and error recovery
- Task status tracking (`not_started`, `started`, `finished`, `failed`)

**Key APIs**:
- `incubator_to_microscope(incubator_slot)`
- `microscope_to_incubator(incubator_slot)`
- `transport_from_incubator_to_microscope1(incubator_slot)`
- `transport_to_incubator(incubator_slot)`
- `get_status()`

#### Incubator (`cytomat-control/`)

**Main Files**:
- `start_hypha_service_incubator.py` - Hypha service wrapper (588 lines)
- `samples.json` - Sample metadata and tracking database

**Features**:
- 80-slot sample storage management
- Temperature and CO2 monitoring
- Sample metadata tracking (type, incubation start time, status)
- Error handling with retry logic

**Key APIs**:
- `put_sample_from_transfer_station_to_slot(slot_id, sample_info)`
- `get_sample_from_slot_to_transfer_station(slot_id)`
- `get_status()` - System health and current operations
- `get_temperature()` - Current temperature reading
- `get_co2_level()` - Current CO2 level
- `add_sample(slot_id, sample_info)` - Register sample
- `remove_sample(slot_id)` - Unregister sample

#### Microscope (`squid-control/`)
External package with built-in mirror functionality. See: https://github.com/aicell-lab/squid-control

#### Mirror Services (`mirror-services/`)

**Files**:
- `mirror_robotic_arm.py` - Cloud-to-local proxy for robotic arm
- `mirror_incubator.py` - Cloud-to-local proxy for incubator

**Features**:
- Automatic method discovery and mirroring
- Health monitoring with auto-reconnection
- Secure remote operation
- Comprehensive error handling and logging

**Pattern**: Uses `GenericMirrorService` base class for common functionality

### 3. Data Management (`hypha_tools/`)

#### Artifact Manager (`artifact_manager/`)
Tools for uploading and organizing data on the Hypha platform.

**Key Features**:
- Concurrent batch file uploads
- Gallery and dataset creation
- Zarr file channel-based uploads
- Resume capability for interrupted transfers
- Progress tracking and logging

#### Automated Uploaders
- `automated_treatment_uploader.py` - Uploads time-lapse experiment data with metadata
- `automated_stitch_uploader.py` - Processes and uploads stitched images

#### Chatbot (`chatbot/`)
AI assistant integration for user guidance and experiment support.

### 4. Lab Live Stream (`lab_live_stream/`)

**Files**:
- `FYIR_camera.py` - FYIR camera interface with FastAPI web server
- `realsense_camera.py` - RealSense camera interface

**Features**:
- Real-time HTTP video streaming
- Camera auto-detection and configuration
- Video recording capabilities
- Multiple camera support

### 5. Supplementary Tools (`tools/`)

#### Image Processing
Utilities for image manipulation, stitching, and format conversion for microscopy workflows.

**Features**:
- Image stitching with overlap handling
- Multi-resolution pyramid generation
- Format conversion (TIFF, OME-Zarr, etc.)
- Channel merging and splitting
- Intensity normalization and adjustment

### 6. Utilities (`utils/`)
Common helper functions and utilities used across the package.

## Configuration

### Task Configuration (`config.json`)

The orchestrator reads task configuration from `config.json`:

```json
{
  "tasks": [
    {
      "name": "experiment_1",
      "operational_state": "pending",
      "pending_time_points": ["2024-01-01T10:00:00", "2024-01-01T12:00:00"],
      "imaged_time_points": [],
      "settings": {
        "scan_mode": "full_automation",
        "data_type": "raw_images_well_plate",
        "wells": ["A1", "A2", "B1", "B2"],
        "num_x": 3,
        "num_y": 3,
        "dx_mm": 0.3,
        "dy_mm": 0.3,
        "illumination_source_configurations": [
          "BF_LED_matrix_full",
          "Fluorescence_488_nm_Ex",
          "Fluorescence_561_nm_Ex"
        ],
        "z_offset": 0,
        "sample_info": {
          "sample_type": "cell_culture",
          "cell_line": "HeLa",
          "plate_id": "plate_001"
        }
      }
    }
  ]
}
```

**Configuration Fields**:
- `name`: Unique task identifier
- `operational_state`: Task status (`pending`, `started`, `completed`, `paused`, `error`)
- `pending_time_points`: ISO 8601 datetime strings for scheduled acquisitions
- `imaged_time_points`: Completed acquisition timestamps
- `settings`: Imaging parameters and sample metadata

**Scan Modes**:
- `full_automation`: Uses incubator + robotic arm + microscope
- `microscope_only`: Imaging only (no transport)

**Data Types**:
- `raw_images_well_plate`: Grid-based well plate scanning
- `raw_image_flexible`: Custom position-based imaging

## Design Patterns

### Service Registration Pattern
All services follow a consistent registration pattern with Hypha:

```python
server = await connect_to_server({
    "name": "service-name",
    "server_url": server_url,
    "token": token
})

await server.register_service({
    "id": "service-id",
    "config": {"visibility": "public"},
    "method1": method1,
    "method2": method2,
})
```

### Mirror Service Pattern
Mirror services use a generic base class to proxy all methods from local to cloud:

```python
class GenericMirrorService:
    async def mirror_all_methods(self, local_service, cloud_service_name)
    async def health_check_loop(self)
    async def reconnect_services(self)
```

### Task Status Tracking
All hardware operations track status:

```python
task_status = {
    "status": "not_started",  # or "started", "finished", "failed"
    "message": "",
    "timestamp": datetime.now().isoformat()
}
```

### Transport Queue Pattern
Serializes sample transport operations to prevent conflicts:

```python
transport_queue = asyncio.Queue()
await transport_queue.put(("load", slot_id))
await transport_queue.put(("unload", slot_id))
```

### Health Monitoring Pattern
All services implement periodic health checks:

```python
async def health_check_loop(self):
    while True:
        await asyncio.sleep(30)
        if not await self.ping_service():
            await self.reconnect()
```

## Logging & Monitoring

### Log Files

The system uses rotating log files (10MB max, 5 backups):

- `orchestrator.log` - Main orchestrator logs
- `incubator_service.log` - Incubator control logs
- `robotic_arm_service.log` - Robotic arm control logs
- `mirror_incubator_service.log` - Incubator mirror logs
- `mirror_robotic_arm_service.log` - Robotic arm mirror logs

### Log Format

```
[YYYY-MM-DD HH:MM:SS] [LEVEL] message
```

Levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

### Monitoring Features

- Service health checks every 30 seconds
- Automatic reconnection on failures
- Critical operation protection
- Task progress tracking
- Error reporting with stack traces

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
cd control/cytomat-control
python start_hypha_service_incubator.py --local
```

**Robotic Arm**:
```bash
cd control/dorna-control
python start_hypha_service_robotic_arm.py --local
```

**Mirror Services** (for cloud operation):
```bash
cd control/mirror-services
python mirror_incubator.py &
python mirror_robotic_arm.py &
```