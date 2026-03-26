# Dorna Robotic Arm Control

This module provides control and Hypha service integration for the **Dorna 2S** robotic arm used in the REEF Imaging platform for automated sample handling between the incubator, microscopes, and Hamilton liquid handler.

## Hardware Overview

### Dorna 2S Robotic Arm

The [Dorna 2S](https://dorna.ai/blog/introducing-dorna-2s/) is a 5-DOF robotic arm with additional linear axes for the REEF Imaging platform:

| Axis | Description | Range/Notes |
|------|-------------|-------------|
| **0-4** | Robotic arm joints (5-DOF) | Standard arm joints |
| **5** | Gripper | `j5 = 0` (loose/open), `j5 ≈ -90` (grab/closed) |
| **6** | Long slide rail (3.6m) | Carries the robotic arm along the microscope line |
| **7** | Hamilton slide rail | Transports samples between optical table and Hamilton liquid handler |

### System Layout

```
┌─────────────────────────────────────────────────────────────────┐
│                     REEF Imaging Platform                        │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────────┐  │
│  │Incubator│    │Scope 1  │    │Scope 2  │    │Scope 3      │  │
│  │         │    │         │    │         │    │(squid+1)    │  │
│  └────┬────┘    └────┬────┘    └────┬────┘    └──────┬──────┘  │
│       │              │              │                 │         │
│       └──────────────┴──────────────┴─────────────────┘         │
│                         ▲                                       │
│                    [Slide Rail Axis 6]                          │
│                         │                                       │
│                    [Dorna 2S Arm]                               │
│                         │                                       │
│       ┌─────────────────┴─────────────────┐                     │
│       │      Hamilton Slide Rail          │                     │
│       │          (Axis 7)                 │                     │
│       └─────────────────┬─────────────────┘                     │
│                         ▼                                       │
│                  [Hamilton Liquid Handler]                      │
└─────────────────────────────────────────────────────────────────┘
```

## Documentation References

- **Official Dorna Documentation**: https://doc.dorna.ai/
- **Dorna 2S Introduction**: https://dorna.ai/blog/introducing-dorna-2s/
- **Python API GitHub**: https://github.com/dorna-robotics/dorna2-python

## Module Structure

```
dorna-control/
├── dorna2/                          # Dorna Python API library
│   ├── __init__.py
│   ├── dorna.py                     # Main Dorna class
│   ├── motion.py                    # Motion control
│   └── ...                          # Other API modules
├── paths/                           # Motion script files
│   ├── grab_from_incubator.txt      # Grab sample from incubator
│   ├── put_on_incubator.txt         # Place sample on incubator
│   ├── grab_from_microscope1.txt    # Grab from microscope 1
│   ├── put_on_microscope1.txt       # Place on microscope 1
│   ├── grab_from_microscope2.txt    # Grab from microscope 2
│   ├── put_on_microscope2.txt       # Place on microscope 2
│   ├── grab_from_squid+1.txt        # Grab from squid+1 microscope
│   ├── put_on_squid+1.txt           # Place on squid+1 microscope
│   ├── grab_from_hamilton.txt       # Grab from Hamilton handler
│   ├── put_on_hamilton.txt          # Place on Hamilton handler
│   ├── transport_from_incubator_to_microscope1.txt
│   ├── transport_from_incubator_to_microscope2.txt
│   ├── transport_from_incubator_to_squid+1.txt
│   ├── transport_to_incubator.txt   # Unified transport to incubator
│   ├── incubator_to_microscope1.txt # Complete sequence
│   └── microscope1_to_incubator.txt # Complete sequence
├── dorna_controller.py              # Basic controller wrapper
├── start_hypha_service_robotic_arm.py  # Hypha RPC service
├── dorna.log                        # Robot communication log
└── robotic_arm_service.log          # Service log
```

## Installation

The Dorna 2 API is included in this directory (`dorna2/`). To install:

```bash
cd reef_imaging/control/dorna-control/dorna2
pip install -r requirements.txt
pip install . --upgrade --force-reinstall
```

## Configuration

### Network Configuration

The robot controller runs a WebSocket server on `ws://192.168.2.20:443` (default port).

Ensure the robot is accessible at:
- **IP Address**: `192.168.2.20`
- **Port**: `443`

### Environment Variables

No specific environment variables are required for the dorna-control module itself. The Hypha service uses standard REEF environment variables:

```bash
# For cloud operation
REEF_WORKSPACE_TOKEN=your_cloud_token

# For local operation
REEF_LOCAL_TOKEN=your_local_token
```

## Usage

### Starting the Hypha Service

```bash
cd reef_imaging/control/dorna-control

# Local mode (development)
python start_hypha_service_robotic_arm.py --local

# Cloud mode (production)
python start_hypha_service_robotic_arm.py

# Simulation mode (no hardware)
python start_hypha_service_robotic_arm.py --local --simulation
```

### Direct Controller Usage

```python
from dorna_controller import DornaController

# Create controller instance
controller = DornaController(ip="192.168.2.20")

# Connect to robot
controller.connect()

# Check if robot is busy
is_busy = controller.is_busy()

# Execute sample transfer
controller.transport_from_incubator_to_microscope1()

# Turn on work light
controller.light_on()

# Emergency halt
controller.halt()

# Disconnect
controller.disconnect()
```

## Motion Scripts

Motion scripts are JSON line-based files where each line represents a robot command:

### Script Format

```json
{"cmd":"jmove","rel":0,"j0":0,"j1":106.46,"j2":-116.61,"j3":-79.50,"j4":0.66,"vel":60}
```

### Command Types

| Command | Description |
|---------|-------------|
| `jmove` | Joint move - moves joints to absolute or relative positions |
| `lmove` | Linear move - TCP moves in straight line |
| `cmove` | Circle move - circular interpolation |
| `halt` | Emergency stop |

### Key Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `rel` | Relative (1) or absolute (0) positioning | `"rel": 0` |
| `j0-j7` | Joint positions in degrees or mm | `"j0": 90` |
| `vel` | Velocity percentage | `"vel": 60` |
| `accel` | Acceleration | `"accel": 100` |
| `jerk` | Jerk limit | `"jerk": 500` |

### Gripper Control

The gripper is controlled via joint 5 (`j5`):

```json
// Open gripper (loose)
{"cmd":"jmove","rel":0,"j5":0}

// Close gripper (grab)
{"cmd":"jmove","rel":0,"j5":-90}
```

## Hypha Service API

The robotic arm exposes the following methods via Hypha RPC:

### Connection Management

| Method | Description |
|--------|-------------|
| `ping()` | Health check, returns `"pong"` |
| `connect()` | Connect to robot controller |
| `disconnect()` | Disconnect from robot controller |

### Sample Transport Operations

| Method | Description |
|--------|-------------|
| `incubator_to_microscope(microscope_id)` | Complete transport from incubator to microscope (1, 2, or 3) |
| `microscope_to_incubator(microscope_id)` | Complete transport from microscope to incubator |
| `transport_from_incubator_to_microscope1()` | Transport from incubator to microscope 1 |
| `transport_to_incubator()` | Transport sample to incubator (unified for all microscopes) |

### Granular Operations

| Method | Description |
|--------|-------------|
| `grab_sample_from_incubator()` | Grab sample from incubator |
| `put_sample_on_incubator()` | Place sample on incubator |
| `grab_sample_from_microscope1()` | Grab sample from microscope 1 |
| `put_sample_on_microscope1()` | Place sample on microscope 1 |

### Status and Control

| Method | Description |
|--------|-------------|
| `get_all_joints()` | Get current joint positions |
| `get_all_positions()` | Get current TCP position (Cartesian) |
| `halt()` | Emergency stop all movements |
| `set_alarm(state)` | Enable (1) or disable (0) alarm |
| `light_on()` | Turn on work light (output 7) |
| `light_off()` | Turn off work light (output 7) |

### Action Management

| Method | Description |
|--------|-------------|
| `get_actions()` | List all predefined actions with positions |
| `execute_action(action_id)` | Execute a predefined action by ID |

Available action IDs:
- `grab_from_incubator`
- `put_on_incubator`
- `grab_from_microscope1`
- `put_on_microscope1`
- `transport_from_incubator_to_microscope1`
- `transport_to_incubator`

### Calling from Python

```python
from hypha_rpc import connect_to_server
import asyncio

async def move_sample():
    server = await connect_to_server({
        "server_url": "https://hypha.aicell.io",
        "workspace": "reef-imaging",
        "token": "your_token"
    })
    
    # Get the robotic arm service
    robot = await server.get_service("robotic-arm-control")
    
    # Check health
    result = await robot.ping()
    print(result)  # "pong"
    
    # Transport sample from incubator to microscope 1
    await robot.incubator_to_microscope(1)
    
    # Get current joint positions
    joints = await robot.get_all_joints()
    print(f"Joints: {joints}")

asyncio.run(move_sample())
```

## Safety Considerations

⚠️ **WARNING**: The robotic arm moves heavy equipment and delicate samples. Always follow these guidelines:

1. **Emergency Stop**: Know the location of the physical emergency stop button
2. **Clearance**: Ensure adequate clearance around the robot's workspace
3. **Microscope State**: Microscope stages must be homed before robot operations
4. **Sample Security**: Verify samples are properly secured in carriers
5. **Observation**: Monitor initial runs of new scripts

### Emergency Procedures

```python
# Software halt
controller.halt()

# Or via Hypha
await robot.halt()
```

## Logging

Service logs are written to:
- `robotic_arm_service.log` - Hypha service operations
- `dorna.log` - Low-level robot communication

Logs use rotating file handlers with automatic rotation at 100KB (3 backups).

## Troubleshooting

### Connection Issues

```
Failed to connect: [Errno 113] No route to host
```
- Verify robot controller is powered on
- Check network connectivity: `ping 192.168.2.20`
- Verify WebSocket port is open: `telnet 192.168.2.20 443`

### Script Errors

```
Error playing script
```
- Check motion limits are not exceeded
- Verify all joints are within safe ranges
- Check for obstacles in the path

### Service Health

The service includes automatic health checking that:
- Verifies service registration every 30 seconds
- Attempts automatic reconnection on failure
- Logs all errors for debugging

## Development

### Adding New Motion Paths

1. Record movements using Dorna Lab or manually create script files
2. Add script file to `paths/` directory
3. Add corresponding method in `RoboticArmService`
4. Register method in `start_hypha_service()` service dict
5. Add `@schema_function(skip_self=True)` decorator for API documentation

### Script Recording Tips

- Use conservative velocities (`vel: 20-60`) for precision moves
- Use higher velocities (`vel: 90`) for transit moves
- Always include intermediate safe positions to avoid collisions
- Test scripts incrementally before full execution

## Robot Settings and Parameters

The Dorna API provides access to various robot settings and real-time state. Below are the current settings retrieved from the REEF Imaging robotic arm:

### Accessing Robot Settings

```python
from dorna2 import Dorna

robot = Dorna()
robot.connect("192.168.2.20")

# Get complete system state
sys_data = robot.sys()

# Get specific parameters
version = robot.version()          # Firmware version
uid = robot.uid()                  # Controller UID
motor_status = robot.get_motor()   # Motor enabled (1) or disabled (0)
alarm_status = robot.get_alarm()   # Alarm state
toollength = robot.get_toollength() # Tool length in mm
joints = robot.get_all_joint()     # Joint positions [j0-j7]
pose = robot.get_all_pose()        # TCP pose [x, y, z, a, b, c, d, e]

robot.close()
```

### Current Robot Configuration

**Controller Information:**
| Parameter | Value |
|-----------|-------|
| Firmware Version | 203 |
| Controller UID | 22001F000A51323137373734 |
| Motor Status | Enabled (1) |
| Alarm Status | Inactive (0) |
| Tool Length | 0 mm |

**Joint Positions (degrees/mm):**
| Joint | Value | Description |
|-------|-------|-------------|
| j0 | ~0° | Base rotation |
| j1 | ~106° | Shoulder |
| j2 | ~-117° | Elbow |
| j3 | ~-79° | Wrist 1 |
| j4 | ~0.7° | Wrist 2 |
| j5 | ~0° | Gripper (0=open, -90=closed) |
| j6 | ~1192 mm | Long slide rail position |
| j7 | ~461 mm | Hamilton slide rail position |

**TCP Pose (Cartesian coordinates):**
| Coordinate | Value | Description |
|------------|-------|-------------|
| x | ~188 mm | Forward/backward |
| y | ~0 mm | Left/right |
| z | ~338 mm | Up/down |
| a | ~-90° | Rotation around X |
| b | ~0.7° | Rotation around Y |
| c | ~0° | Rotation around Z |
| d | ~1192 mm | Auxiliary axis 6 value |
| e | ~461 mm | Auxiliary axis 7 value |

**Auxiliary Axis Ratios:**
| Axis | Ratio Config | Description |
|------|--------------|-------------|
| Axis 5 | [1, 1, 800, 5, 4000, 5] | Gripper gear ratio |
| Axis 6 | [1, 1, 1600, -20, 4000, -20] | Long slide rail (3.6m) |
| Axis 7 | [1, 1, 400, 10, 4000, 10] | Hamilton transport rail |

**I/O States:**
| I/O | State | Notes |
|-----|-------|-------|
| out7 | 1 (ON) | Work light is currently ON |
| in0-in15 | 0 | All inputs inactive |
| out0-out6 | 0 | Outputs off |

### Settings Commands Reference

| Method | Description | Return Type |
|--------|-------------|-------------|
| `sys()` | Complete system state dictionary | dict |
| `version()` | Firmware version number | int/float |
| `uid()` | Controller unique identifier | string |
| `get_motor()` | Motor enable status (0/1) | int |
| `set_motor(enable)` | Enable/disable motors | stat |
| `get_alarm()` | Alarm status (0/1) | int |
| `set_alarm(state)` | Set alarm state | stat |
| `get_toollength()` | Tool length in mm | float |
| `set_toollength(length)` | Set tool length | stat |
| `get_all_joint()` | All 8 joint positions | list |
| `get_joint(index)` | Specific joint position | float |
| `set_joint(index, val)` | Set joint value | stat |
| `get_all_pose()` | TCP pose [x,y,z,a,b,c,d,e] | list |
| `get_pose(index)` | Specific pose coordinate | float |
| `get_gravity()` | Gravity compensation params | list |
| `set_gravity(en, m, x, y, z)` | Set gravity compensation | stat |
| `get_axis(index)` | Auxiliary axis ratio | list |
| `set_axis(index, ratio)` | Set axis ratio | stat |
| `get_pid(index)` | PID parameters for joint | list |
| `set_pid(index, p, i, d, thr, dur)` | Set PID params | stat |
| `get_all_input()` | All 16 input states | list |
| `get_input(index)` | Specific input state | int |
| `get_all_output()` | All 16 output states | list |
| `get_output(index)` | Specific output state | int |
| `set_output(index, val)` | Set output state | stat |

### System State Dictionary (`sys()`)

The `sys()` method returns a comprehensive dictionary containing:

```python
{
  "id": <command_id>,        # Last command ID
  "stat": <status>,          # Command status (2 = completed)
  "cmd": <command>,          # Last command type
  "alarm": <0_or_1>,         # Alarm state
  "j0"-"j7": <values>,       # Joint positions
  "x", "y", "z": <values>,   # Cartesian position
  "a", "b", "c": <values>,   # Orientation angles
  "d", "e": <values>,        # Auxiliary axes positions
  "vel": <velocity>,         # Current velocity
  "accel": <acceleration>,   # Current acceleration
  "motor": <0_or_1>,         # Motor state
  "toollength": <mm>,        # Tool length
  "in0"-"in15": <0_or_1>,    # Input states
  "out0"-"out15": <0_or_1>,  # Output states
  "pwm0"-"pwm4": <values>,   # PWM states
  "duty0"-"duty4": <values>, # PWM duty cycles
  "freq0"-"freq4": <values>, # PWM frequencies
  "adc0"-"adc4": <values>,   # ADC readings
  "version": <firmware>,     # Firmware version
  "uid": <string>            # Controller UID
}
```

## See Also

- [Cytomat Control](../cytomat-control/README.md) - Incubator integration
- [Squid Control](../squid-control/README.md) - Microscope integration
- [Main REEF Documentation](../../AGENTS.md) - Platform overview
