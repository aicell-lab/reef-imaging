# REEF Imaging

A platform for automated microscope control, image acquisition, data management, and analysis for reef biological experiments.

## Overview

REEF Imaging provides a comprehensive system for automated microscopy workflows, integrating hardware control, cloud-based data management, and real-time monitoring. The system enables fully automated time-lapse experiments with:

- **Hardware Control**: Seamless integration with SQUID microscopes, Dorna robotic arms, and Cytomat incubators
- **Image Acquisition**: Multi-channel fluorescence and brightfield imaging with automated well plate scanning
- **Data Management**: Cloud-based storage and organization through the Hypha platform with artifact management
- **Orchestration**: Task-driven workflow automation with real-time status tracking and error recovery
- **Remote Operation**: Mirror service architecture enabling secure cloud-to-local hardware control
- **Live Monitoring**: Real-time camera streaming and experiment status visualization
- **Image Processing**: Utilities for image manipulation, stitching, and format conversion

## Architecture

### System Components

The REEF Imaging system is built on a modular architecture with four main layers:

1. **Orchestration Layer** (`orchestrator.py`)
   - Task scheduling and management from `config.json`
   - Hardware coordination (microscope, robotic arm, incubator)
   - Transport queue for serialized sample handling
   - Health monitoring with automatic reconnection
   - Critical operation protection and error recovery

2. **Hardware Control Layer** (`control/`)
   - **Microscope Service**: Stage positioning, multi-channel imaging, autofocus
   - **Robotic Arm Service**: Sample transport with preconfigured paths
   - **Incubator Service**: Sample storage, environmental monitoring (temperature, CO2)
   - All services expose standardized APIs through Hypha RPC

3. **Mirror Service Layer** (`mirror-services/`)
   - Cloud-to-local service proxies for remote operation
   - Automatic method mirroring for robotic arm and incubator
   - Health checks with auto-reconnection
   - Note: Microscope has built-in mirror functionality

4. **Data Management Layer** (`hypha_tools/`)
   - Artifact management for cloud storage organization
   - Automated uploaders for experiment data
   - Gallery and dataset creation
   - Concurrent batch uploads with resume capability

### Communication Flow

```
Cloud (Hypha Server: hypha.aicell.io)
    ↕️ (RPC)
Mirror Services (robotic arm, incubator)
    ↕️ (RPC)
Local Hypha Server (reef.dyn.scilifelab.se:9527)
    ↕️ (RPC)
Orchestrator ← Hardware Services (microscope, robotic arm, incubator)
    ↕️
Physical Hardware
```

## Lab Setup

![Lab Overview](docs/lab_overview.jpg)

Check out our system demonstration video:
[REEF Imaging System Demo Video](https://drive.google.com/file/d/1nQLgzMsSR3JCzMfe99mdpwYSvpYZAS7q/view?usp=sharing)

## Project Structure

- **reef_imaging/** - Main package
- **control/** - Hardware control modules
  - **dorna-control/** - Control for Dorna robotic arm
  - **cytomat-control/** - Control for Cytomat incubator
  - **squid-control/** - Control for SQUID microscope (includes built-in mirror functionality)
  - **mirror-services/** - Services for mirroring data between cloud and local systems (robotic arm and incubator only)
  - **hypha_tools/** - Utilities for working with the Hypha platform
    - **artifact_manager/** - Tools for interacting with Hypha's artifact management system
    - **automated_treatment_uploader.py** - Uploads time-lapse experiment data
    - **automated_stitch_uploader.py** - Processes and uploads stitched images
  - **orchestrator.py** - Main orchestration system
  - **hypha_service.py** - Hypha service integration

## Installation

First, clone the repository and set up the environment:

```bash
git clone git@github.com:aicell-lab/reef-imaging.git
cd reef-imaging
conda create -n reef-imaging python=3.11 -y
conda activate reef-imaging

# Install squid-control in editable mode (includes built-in mirror functionality)
git clone git@github.com:aicell-lab/squid-control.git
pip install -e squid-control

# Install the package and its dependencies
pip install -e .
```

## Usage

### Start Hypha Server

Before starting, make sure you've installed Docker and docker-compose.

1. **IMPORTANT**: Set permissions for HTTPS
   ```bash
   chmod 600 traefik/acme/acme.json
   ```

2. Create an `.env` file based on the template in `.env-template`

3. Configure your settings in `docker/docker-compose.yaml`

4. Create the Docker network
   ```bash
   docker network create hypha-app-engine
   ```

5. Start the application containers
   ```bash
   cd docker && docker-compose up -d
   ```

6. Start the traefik service
   ```bash
   cd traefik && docker-compose up -d
   ```

7. After a few minutes, your site should be running at https://reef.aicell.io

### Start Hypha Services

To run the main service:
```bash
python -m reef_imaging.hypha_service
```

### Running the Orchestrator

**Production Mode** (with real hardware):
```bash
cd reef_imaging
python orchestrator.py
```

For local development and testing:
```bash
# Simulates hardware responses for safe testing
python orchestrator_simulation.py --local
```

The orchestrator will:
1. Connect to the Hypha server (local or cloud)
2. Discover and connect to hardware services
3. Load tasks from `config.json`
4. Begin processing pending time points
5. Monitor service health and automatically reconnect on failures

### Starting Individual Hardware Services

**Incubator Control**:
```bash
cd reef_imaging/control/cytomat-control
python start_hypha_service_incubator.py --local
```

**Robotic Arm Control**:
```bash
cd reef_imaging/control/dorna-control
python start_hypha_service_robotic_arm.py --local
```

**Microscope Control**:
```bash
cd squid-control  # External package
python start_hypha_service_squid_control.py --local
```

**Mirror Services** (for cloud operation):
```bash
cd reef_imaging/control/mirror-services
python mirror_incubator.py
python mirror_robotic_arm.py
```

## Environment Setup

### Environment Variables

The system requires environment variables for authentication and configuration. Create a `.env` file in the project root:

```bash
# Cloud Operation (Hypha: hypha.aicell.io)
REEF_WORKSPACE_TOKEN=your_cloud_token_here
SQUID_WORKSPACE_TOKEN=your_squid_token_here

# For local development
REEF_LOCAL_TOKEN=your_local_token
REEF_LOCAL_WORKSPACE=your_local_workspace
```

## Hardware Control Services

The system integrates with multiple hardware components:

- **Microscope Control**: Manages SQUID microscope for imaging, stage positioning, and illumination (includes built-in mirror functionality)
- **Robotic Arm Control**: Handles sample transfer between microscope and incubator
- **Incubator Control**: Manages sample storage and environmental conditions
- **Mirror Services**: Proxies requests between cloud and local systems (for robotic arm and incubator only)

### Using the squid_control Package

The `squid_control` package now includes built-in mirror functionality, eliminating the need for a separate `mirror_squid_control.py` service. This simplifies the setup and provides better integration between local and cloud operations.

For more information about the `squid_control` package and its mirror features, visit: https://github.com/aicell-lab/squid-control

## Restart Hypha

```
cd docker && docker-compose restart hypha
```

## Documentation

For more detailed information, see the README files in each subdirectory:
- `reef_imaging/README.md` - Main codebase overview
- `reef_imaging/control/README.md` - Hardware control systems
- `reef_imaging/hypha_tools/README.md` - Hypha integration tools
- `reef_imaging/hypha_tools/artifact_manager/README.md` - Artifact management utilities