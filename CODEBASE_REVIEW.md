# REEF Imaging Codebase Review

**Date:** 2026-04-17  
**Reviewer:** Claude  
**Scope:** All Python code in `reef_imaging/` excluding `squid-control` submodule  
**Total Lines:** ~11,700 lines of Python

---

## Executive Summary

The REEF Imaging codebase is a well-architected, safety-conscious automated microscopy platform with strong documentation (`AGENTS.md`, `CLAUDE.md`) and consistent Hypha RPC service patterns. The admission controller design is particularly solid. However, the codebase has accumulated significant technical debt: the orchestrator is monolithic at 3,147 lines, error handling is overly broad (~40+ bare `except Exception` in `orchestrator.py` alone), test coverage is minimal (~5%), and there are security and reliability issues that should be addressed.

**Overall Grade: B** — Functional and well-designed at the architectural level, but needs refactoring, better error handling, and more tests before it can be considered production-hardened.

---

## 1. Critical Issues (Fix Immediately)

### 1.1 Bogus Dependency in `pyproject.toml` (`logging==0.5.1`)

**File:** `pyproject.toml:14`

```toml
dependencies = [
    ...
    "logging==0.5.1",  # <-- This is NOT Python's stdlib logging
    ...
]
```

The `logging` package on PyPI is an unrelated third-party library from 2009. Python's built-in `logging` module is part of the standard library and should never be in `dependencies`. This could cause package resolution conflicts.

**Fix:** Remove the line entirely.

---

### 1.2 `.env-template` Contains Example Secrets That Look Production-Real

**File:** `.env-template`

```
S3_SECRET_KEY=393poripir23poi
JWT_SECRET=sdf093u40230482-348923
REDIS_PASSWORD=2340823rpoij3p0523-5
```

These are structured like real secrets. If anyone copies this template without replacing them, they run with weak, guessable credentials. The `JWT_SECRET` in particular is critical — if this value is actually used anywhere in production, authentication is compromised.

**Fix:** Replace all secret values with clearly fake placeholders:
```
S3_SECRET_KEY=REPLACE_WITH_REAL_SECRET
JWT_SECRET=REPLACE_WITH_REAL_SECRET
REDIS_PASSWORD=REPLACE_WITH_REAL_SECRET
```

Also add a comment block at the top of `.env-template` warning never to commit `.env`.

---

### 1.3 `run_hamilton_protocol` Accepts Arbitrary Script Content Without Validation

**File:** `orchestrator.py` (around line 2700+)

The Hamilton protocol API accepts raw script content and forwards it to the executor. While the executor may be an isolated Windows machine, there's no length limit, content-type check, or sanitization on the input.

**Risk:** Potential command injection if the Hamilton executor ever parses the script in an unsafe way, or denial of service via extremely large script payloads.

**Fix:** Add validation — maximum script length (e.g., 100KB), require a specific file extension or header marker, and log all script content hashes for auditability.

---

### 1.4 Watchdog Cannot Restart Service Due to Missing Permissions

**File:** `lab_live_stream/lab_cameras_watchdog.py` (prior to recent fix)

The watchdog was calling `systemctl restart` without sudo, causing `Interactive authentication required` failures. The service was down for over a month because the watchdog couldn't actually recover it.

**Status:** Recently fixed with a fallback to `SIGKILL` via `psutil`.  
**Remaining risk:** The `SIGKILL` fallback is destructive. Consider adding a `sudoers` rule for the watchdog so it can use `systemctl` properly.

---

## 2. High-Priority Issues

### 2.1 `orchestrator.py` is 3,147 Lines — Severely Overgrown

**File:** `reef_imaging/orchestrator.py`

A single file containing:
- Service connection management (6 services)
- Health check orchestration
- Task scheduling and time-lapse logic
- Config file I/O with locking
- Plate transport coordination (load/scan/unload)
- Hamilton protocol execution
- Offline processing coordination
- 20+ API endpoint handlers decorated with `@schema_function`

This violates the Single Responsibility Principle and makes the file impossible to review, test, or navigate.

**Recommended split:**
```
reef_imaging/orchestrator/
    __init__.py          # OrchestrationSystem facade
    core.py              # Main class, initialization, event loop
    connections.py       # Service discovery & connection management
    health.py            # Health check logic
    tasks.py             # Task loading, scheduling, time-lapse
    transport.py         # Load/scan/unload coordination
    api.py               # @schema_function API endpoints
    config_io.py         # Atomic config read/write
```

**Effort estimate:** 1-2 days of careful refactoring with existing tests as safety net.

---

### 2.2 ~40+ Bare `except Exception` Catches in `orchestrator.py`

**Pattern:** `except Exception as e:` is used 40+ times in `orchestrator.py` alone.

**Problem:** This catches `KeyboardInterrupt`, `SystemExit`, `asyncio.CancelledError`, `MemoryError`, and every other unexpected exception. It makes debugging nearly impossible because stack traces are swallowed or reduced to a single log line. It also prevents asyncio tasks from being cancelled properly.

**Examples:**
- `orchestrator.py:113` — `_mark_critical_services` catches `Exception` around `set.add()` which can never fail
- `orchestrator.py:490` — Health check reconnection catches all exceptions indiscriminately
- `orchestrator.py:778` — `_register_self_as_hypha_service` catches all exceptions

**Fix:**
1. Catch specific exceptions (e.g., `ConnectionError`, `TimeoutError`, `json.JSONDecodeError`)
2. Never catch `Exception` without re-raising `KeyboardInterrupt`, `SystemExit`, and `asyncio.CancelledError`
3. Use a helper pattern:
```python
except Exception as e:
    if isinstance(e, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
        raise
    logger.error(...)
```

---

### 2.3 Six Nearly Identical `setup_logging()` Functions

**Files:**
- `orchestrator.py:34`
- `control/cytomat-control/start_hypha_service_incubator.py:86`
- `control/dorna-control/start_hypha_service_robotic_arm.py:21`
- `control/mirror-services/mirror_robotic_arm.py:18`
- `control/mirror-services/mirror_incubator.py:19`
- `control/mirror-services/mirror_hamilton.py:17`

All follow the exact same pattern: `RotatingFileHandler` + `StreamHandler`, same formatter string. This is ~30 lines duplicated 6 times = 180 lines of duplication.

**Fix:** Create `reef_imaging/utils/logging_config.py`:
```python
import logging.handlers

def setup_logging(log_file: str, max_bytes: int = 10*1024*1024, backup_count: int = 5,
                  level: int = logging.INFO) -> logging.Logger:
    ...
```

---

### 2.4 Minimal Test Coverage (~5%)

**Current tests:**
- `tests/test_admission.py` — 58 lines, admission controller only
- `tests/test_orchestrator_refactor.py` — 290 lines, basic API behavior
- `tests/test_incubator_service.py` — 87 lines
- `tests/test_hardware_smoke_test.py` — 74 lines

**Total:** ~509 lines of tests for ~11,700 lines of code.

**What's missing:**
- No integration test for a full load-scan-unload cycle
- No error injection tests (what happens when the incubator disconnects mid-transport?)
- No tests for config file atomic writes
- No tests for health check reconnection logic
- No tests for the time-lapse scheduling algorithm
- No tests for camera streaming

**Fix priority:**
1. Add unit tests for `orchestrator.py` helpers (`_load_and_update_tasks`, `_build_request`, etc.)
2. Add integration test that mocks all services and runs a full cycle
3. Add error-injection tests: service disconnects during critical operations

---

### 2.5 Inconsistent Error Handling in `lab_cameras.py`

**File:** `lab_live_stream/lab_cameras.py`

The camera capture thread uses a simple failure counter (`consecutive_failures`) but the health endpoint (`/health`) returns `connected: true/false` based only on the last capture attempt. If the camera fails 9 times in a row then succeeds once, `connected` is `true` even though reliability is poor.

**Also:** The time-lapse recording thread writes to `VideoWriter` with hardcoded `(640, 480)` resolution. If the camera returns a different resolution, the video will be corrupted.

**Fix:**
1. Add a rolling window health metric (e.g., "healthy if 8/10 last frames succeeded")
2. Read actual camera resolution with `cam.get(cv2.CAP_PROP_FRAME_WIDTH/HEIGHT)` instead of hardcoding

---

## 3. Medium-Priority Issues

### 3.1 `hamilton-control/` Directory is Empty

**File:** `reef_imaging/control/hamilton-control/` (empty)

An empty directory suggests either an abandoned feature or incomplete cleanup. It may confuse new developers.

**Fix:** Remove the directory or add a `README.md` explaining it's a placeholder.

---

### 3.2 `dotenv.load_dotenv()` Called Multiple Times at Module Import Time

**Pattern seen in:** `orchestrator.py`, `lab_cameras.py`, `hardware_smoke_test.py`, all mirror services, all control services

Calling `dotenv.load_dotenv()` at module import time is an anti-pattern. It:
1. Causes side effects on import
2. Makes testing difficult (can't mock env vars before import)
3. Performs redundant work (called ~10+ times across the codebase)

**Fix:** Load environment variables once in a central location, ideally in `reef_imaging/__init__.py` or a dedicated `config.py`, and use `os.environ.get()` everywhere else.

---

### 3.3 Health Check Reconnection Logic is Complex and Untested

**File:** `orchestrator.py:480-720`

The health check task has nested try/except blocks, reconnection loops, and special handling for "critical operations" where failures cause **program exit**. This logic is:
- Difficult to reason about
- Untested
- Contains the broad `except Exception` anti-pattern
- May exit the entire orchestrator because one microscope had a transient network blip during a scan

**Fix:** Extract health check logic into a dedicated `health.py` module with smaller, testable functions. Add unit tests with mocked service objects.

---

### 3.4 No Timeout on `asyncio.wait_for` for Many Service Calls

Many async service calls (e.g., `microscope_service.scan_start()`, `robotic_arm.transport_plate()`) are awaited directly without timeouts. If a service hangs, the orchestrator hangs forever.

**Fix:** Wrap all external service calls in `asyncio.wait_for(..., timeout=...)`. Define a `DEFAULT_SERVICE_TIMEOUT = 300` (5 minutes) constant and use it consistently.

---

### 3.5 Config File Has No Schema Validation

**File:** `reef_imaging/config.json`

The config is read as raw JSON with no validation. Invalid fields, missing required keys, or wrong types are only discovered at runtime, often deep in the execution flow.

**Fix:** Use Pydantic models for config validation:
```python
from pydantic import BaseModel

class SampleConfig(BaseModel):
    name: str
    settings: SampleSettings
    operational_state: OperationalState
```

This would catch config errors at load time with clear error messages.

---

### 3.6 `LabCamera.record_time_lapse()` Hardcodes Resolution and Frame Rate

**File:** `lab_live_stream/lab_cameras.py:150-175`

```python
out = cv2.VideoWriter(
    os.path.join(self.video_dir, filename), fourcc, 24, (640, 480)
)
```

The resolution `(640, 480)` and frame rate `24` are hardcoded. If the camera produces a different resolution, OpenCV will silently produce corrupted video.

**Fix:** Query the camera's actual properties:
```python
width = int(cam.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(cam.get(cv2.CAP_PROP_FRAME_HEIGHT))
```

---

## 4. Low-Priority / Polish Issues

### 4.1 Type Hints are Inconsistent

Some methods are fully typed (`admission.py`), others have no type hints at all (`orchestrator.py` methods). This makes IDE autocomplete and static analysis less useful.

**Fix:** Add type hints to all public methods, especially API endpoints.

---

### 4.2 Magic Numbers Scattered Throughout

Examples:
- `CONFIG_READ_INTERVAL = 10` — `orchestrator.py:64`
- `ORCHESTRATOR_LOOP_SLEEP = 5` — `orchestrator.py:65`
- Health check interval `30` seconds — repeated in multiple files
- `max_failures = 10` — `lab_cameras.py:94`
- `duration = 30 * 60` (30 min) — `lab_cameras.py:161`
- Video cleanup `72` hours — `lab_cameras.py:182`

**Fix:** Centralize these in a `constants.py` or `config.py` module.

---

### 4.3 Commented-Out Code in `orchestrator.py`

**File:** `orchestrator.py:3118-3121`

```python
# parser = argparse.ArgumentParser(description='Run the Orchestration System.')
# parser.add_argument('--local', action='store_true', ...)
# args = parser.parse_args()
```

Dead code should be removed, not commented out. Git history preserves it if needed.

---

### 4.4 `async with self._config_lock:` Only Wraps File Read, Not Write

**File:** `orchestrator.py:282-293`

The config read is protected by `asyncio.Lock`, but the `_write_tasks_to_config` method likely also needs protection. Need to verify this is consistent.

---

## 5. Architecture Strengths (Keep Doing These)

1. **Admission Controller** (`admission.py`) — Excellent design. Clean separation, async-safe, good error types.
2. **Atomic Config Writes** — Using `.tmp` + `os.replace()` prevents corruption. Good pattern.
3. **Consistent Hypha Registration** — All services follow the same pattern. Easy to understand.
4. **Critical Operation Tracking** — Safety-first design prevents shutdown during robotic arm movement.
5. **Documentation** — `AGENTS.md` and `CLAUDE.md` are well-written and genuinely useful.
6. **Service Visibility Separation** — Hardware controls are `protected`, cameras are `public`. Correct.
7. **Modular Hardware Control** — Clear separation between orchestrator, control modules, and mirror services.

---

## 6. Priority-Ranked Action Plan

| Priority | Task | Effort | Files |
|----------|------|--------|-------|
| **P0** | Remove `logging==0.5.1` from `pyproject.toml` | 5 min | `pyproject.toml` |
| **P0** | Sanitize `.env-template` secrets | 5 min | `.env-template` |
| **P1** | Refactor `orchestrator.py` into subpackage | 1-2 days | `reef_imaging/orchestrator/` |
| **P1** | Fix bare `except Exception` in `orchestrator.py` | 2-4 hrs | `orchestrator.py` |
| **P1** | Add timeout wrappers to all service calls | 2-3 hrs | `orchestrator.py` |
| **P2** | Create shared `setup_logging()` utility | 30 min | New `utils/logging_config.py` |
| **P2** | Add config validation with Pydantic | 2-3 hrs | `config.py` + `config.json` readers |
| **P2** | Fix `lab_cameras.py` resolution hardcoding | 30 min | `lab_cameras.py` |
| **P2** | Centralize `dotenv.load_dotenv()` | 1 hr | `__init__.py` + all services |
| **P3** | Add integration tests for full cycle | 1-2 days | `tests/` |
| **P3** | Add type hints to all public methods | 2-3 hrs | All modules |
| **P3** | Centralize magic numbers | 1 hr | New `constants.py` |
| **P3** | Remove empty `hamilton-control/` directory | 5 min | — |

---

## 7. Security Checklist

- [x] Tokens stored in `.env` (gitignored) — **Good**
- [x] Service visibility appropriate (protected for hardware, public for cameras) — **Good**
- [ ] `.env-template` contains realistic-looking example secrets — **Fix needed**
- [ ] `run_hamilton_protocol` accepts arbitrary script content — **Add validation**
- [ ] No rate limiting on API endpoints — **Consider adding**
- [ ] No audit logging of who called which API — **Consider adding for compliance**
