# Cytomat Incubator Control

Hypha service for controlling the Cytomat incubator — 80-slot sample storage with temperature and CO2 management.

## Files

| File | Purpose |
|------|---------|
| `start_hypha_service_incubator.py` | Hypha service registration and API |
| `samples.json` | Sample metadata and slot tracking database |
| `hypha/` | Hypha utilities |

## Hypha Service

**Service ID**: `incubator-control`
**Workspace**: `reef-imaging`

## Key APIs

| Method | Description |
|--------|-------------|
| `put_sample_from_transfer_station_to_slot(slot_id, sample_info)` | Move sample from transfer station into a slot |
| `get_sample_from_slot_to_transfer_station(slot_id)` | Move sample from slot to transfer station |
| `get_incubator_samples(slot_id)` | Canonical sample metadata listing owned by incubator |
| `add_sample(slot_id, sample_info)` | Register sample metadata for a slot |
| `remove_sample(slot_id)` | Unregister sample from a slot |
| `get_status()` | System health and current operation |
| `get_temperature()` | Current temperature reading |
| `get_co2_level()` | Current CO2 level |
| `ping()` | Health check |

`get_slot_information()` remains available as the lower-level slot dump, but
`get_incubator_samples()` is the preferred API for clients that need sample
inventory metadata.

## Error Codes

| Code | Meaning |
|------|---------|
| 0 | No error |
| 1 | Motor communication disrupted |
| 2 | Plate not mounted on shovel |
| 3 | Plate not dropped from shovel |
| 4 | Shovel not extended |
| 5 | Procedure timeout |
| 6 | Transfer door not opened |
| 7 | Transfer door not closed |
| 8 | Shovel not retracted |
| 10 | Step motor temperature too high |
| 13 | Heating or CO2 communication disrupted |
| 255 | Critical |

## Prerequisites

- **Conda Environment**: The incubator control requires the `cytomat-env` conda environment which has the `cytomat` library installed.
  ```bash
  conda activate cytomat-env
  ```

## Running

**Standard**:
```bash
conda activate cytomat-env
cd reef_imaging/control/cytomat-control
python start_hypha_service_incubator.py
```

**Local server**:
```bash
python start_hypha_service_incubator.py --local
```

**Simulation** (no hardware):
```bash
python start_hypha_service_incubator.py --simulation
```

## Environment Variables

Set in `.env` at the project root:

```
REEF_WORKSPACE_TOKEN=...   # cloud operation
REEF_LOCAL_TOKEN=...       # local operation
REEF_LOCAL_WORKSPACE=reef-imaging-local
```

## Logging

Log file: `incubator_service.log` (rotating, 100 KB max, 3 backups)
