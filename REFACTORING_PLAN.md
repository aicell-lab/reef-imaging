# REEF-Imaging Architecture Refactoring Plan

**Date:** 2026-03-20  
**Author:** Kimi Claw (AI Architecture Review)  
**Status:** Ready for Implementation  
**Estimated Duration:** 3-4 weeks  

---

## Executive Summary

This document outlines a comprehensive refactoring plan for the reef-imaging system to address architectural technical debt, improve maintainability, and establish production-ready code quality.

### Current State Assessment
- **Lines of Code:** ~5,000 across orchestrator and services
- **Maintainability Score:** 6/10 (functional but high technical debt)
- **Test Coverage:** ~0%
- **Key Issues:** God classes, code duplication, tight coupling, inconsistent APIs

### Target State
- **Maintainability Score:** 9/10
- **Test Coverage:** >80%
- **Key Improvements:** Clean architecture, dependency injection, state machines, shared libraries

---

## Phase 0: Immediate Fixes (COMPLETED ✓)

### Completed Changes
1. **Extracted Config Management** — `reef_imaging/orchestrator/config/` module
2. **Shared Health Check Library** — `reef_imaging/utils/health.py` with circuit breaker pattern
3. **Fixed Hard-coded Paths** — Environment variables and relative paths
4. **Standardized API Responses** — `reef_imaging/utils/responses.py` with generic `ApiResponse[T]`

**Branch:** `refactor/extract-config-store`  
**Commit:** `e6df1bc`

---

## Phase 1: Infrastructure Layer (1 week)

### 1.1 Hardware Abstraction Layer

**Goal:** Decouple services from specific hardware vendors (Cytomat, Dorna).

**New Files:**
```
infrastructure/
├── hardware/
│   ├── __init__.py
│   ├── base.py              # Abstract interfaces
│   ├── cytomat/
│   │   ├── __init__.py
│   │   └── driver.py        # CytomatDriver implementation
│   └── dorna/
│       ├── __init__.py
│       └── driver.py        # DornaDriver implementation
```

**Key Interfaces:**
```python
class IncubatorDriver(ABC):
    @abstractmethod
    async def load_sample(self, slot: int) -> None: ...
    
    @abstractmethod
    async def unload_sample(self, slot: int) -> None: ...
    
    @abstractmethod
    async def get_status(self) -> IncubatorStatus: ...

class RoboticArmDriver(ABC):
    @abstractmethod
    async def move_to_microscope(self, microscope_id: int) -> None: ...
    
    @abstractmethod
    async def move_to_incubator(self) -> None: ...
```

**Benefit:** Swap Cytomat → Liconic incubator by implementing the same interface. Zero changes to service code.

### 1.2 Shared Service Base Class

**Goal:** Eliminate duplicated Hypha registration, health check, and lifecycle code.

**New File:** `infrastructure/hypha/service.py`

```python
class HyphaService(ABC):
    """Base class for all Hypha-registered services."""
    
    def __init__(self, service_id: str, config: ServiceConfig):
        self.service_id = service_id
        self.config = config
        self.health_monitor = None
        self.server = None
    
    async def start(self):
        """Register with Hypha and start health monitoring."""
        self.server = await connect_to_server({...})
        await self._register_service()
        self.health_monitor = HealthMonitor(...)
        await self.health_monitor.start()
    
    @abstractmethod
    def get_service_methods(self) -> Dict[str, Callable]:
        """Return RPC method dictionary."""
        pass
    
    async def stop(self):
        """Graceful shutdown."""
        await self.health_monitor.stop()
        await self.server.disconnect()
```

**Refactoring Impact:**
- Incubator service: 700 lines → ~300 lines
- Robotic arm service: 700 lines → ~300 lines

### 1.3 Centralized Persistence

**Goal:** Unify sample tracking (currently split between incubator service and orchestrator).

**New File:** `infrastructure/persistence/samples.py`

```python
class SampleRepository:
    """Centralized sample location tracking."""
    
    async def get_location(self, sample_id: str) -> Optional[Location]: ...
    async def update_location(self, sample_id: str, location: Location) -> None: ...
    async def list_samples(self, status: Optional[SampleStatus] = None) -> List[Sample]: ...
    async def register_sample(self, sample: Sample) -> None: ...
```

---

## Phase 2: Core Domain Layer (1 week)

### 2.1 Domain Entities

**Goal:** Pure business logic with no infrastructure dependencies.

**New Files:**
```
core/
├── __init__.py
├── domain.py              # Sample, Task, Experiment entities
└── value_objects.py       # Location, Status, TimePoint
```

**Example:**
```python
@dataclass
class Task:
    id: TaskId
    sample: Sample
    schedule: List[TimePoint]
    status: TaskStatus
    allocated_microscope: MicroscopeId
    
    def is_due(self, now: datetime) -> bool: ...
    def next_timepoint(self) -> Optional[TimePoint]: ...
```

### 2.2 State Machine for Task Lifecycle

**Goal:** Prevent invalid state transitions (e.g., scanning → loading).

**New File:** `core/state_machine.py`

```python
class TaskStateMachine(StateMachine):
    """Explicit task state transitions."""
    
    pending = State(initial=True)
    loading = State()
    scanning = State()
    unloading = State()
    uploading = State()
    completed = State(final=True)
    error = State(final=True)
    paused = State()
    
    # Valid transitions
    start_loading = pending.to(loading)
    finish_loading = loading.to(scanning)
    start_scanning = scanning.to.itself()  # Time point iteration
    finish_scanning = scanning.to(unloading)
    start_uploading = unloading.to(uploading)
    finish_uploading = uploading.to(completed)
    
    # Error handling
    fail = State.to(error)
    pause = pending.to(paused) | scanning.to(paused) | uploading.to(paused)
    resume = paused.to(pending)
```

**Benefit:** Impossible to have invalid state transitions. Self-documenting workflow.

### 2.3 Pure Scheduler Logic

**Goal:** Deterministic scheduling without side effects (testable).

**New File:** `core/scheduler.py`

```python
class Scheduler:
    """Pure scheduling logic - no I/O, no Hypha, no hardware."""
    
    def __init__(self, tasks: List[Task]):
        self.tasks = tasks
    
    def what_should_run_now(self, now: datetime) -> List[Task]:
        """Return tasks that should start now."""
        return [t for t in self.tasks if t.is_due(now)]
    
    def allocate_microscopes(self, available: List[MicroscopeId]) -> Dict[TaskId, MicroscopeId]:
        """Assign microscopes to pending tasks."""
        # Pure algorithm - easy to test
```

### 2.4 Transport Operation Abstraction

**Goal:** Model load/unload as a domain concept, not just RPC calls.

**New File:** `core/transport.py`

```python
@dataclass
class TransportOperation:
    operation_id: UUID
    sample_id: SampleId
    source: Location
    destination: Location
    status: TransportStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

class TransportPlanner:
    """Plan transport operations without executing."""
    
    def plan_load(self, sample: Sample, microscope: MicroscopeId) -> TransportOperation: ...
    def plan_unload(self, sample: Sample) -> TransportOperation: ...
```

---

## Phase 3: Orchestrator Refactoring (1.5 weeks)

### 3.1 Current Problems
- `OrchestrationSystem`: ~1,600 lines, 40+ methods
- Mixes concerns: API, scheduling, transport, health checks, config I/O
- Tight coupling to Hypha RPC throughout

### 3.2 Target Architecture

```
services/orchestrator/
├── __init__.py
├── main.py                   # Entry point only
├── coordinator.py            # High-level coordination (~200 lines)
├── service_manager.py        # Service lifecycle (~200 lines)
├── transport_executor.py     # Execute transport ops (~200 lines)
└── api/
    ├── __init__.py
    └── hypha_routes.py       # Thin API layer (~150 lines)
```

### 3.3 Dependency Injection

**Before (tight coupling):**
```python
class OrchestrationSystem:
    def __init__(self):
        self.incubator = await connect_to_server(...)  # Direct Hypha
```

**After (dependency injection):**
```python
class Coordinator:
    def __init__(
        self,
        incubator: IncubatorPort,
        arm: RoboticArmPort,
        microscopes: Dict[MicroscopeId, MicroscopePort],
        scheduler: Scheduler,
        task_repo: TaskRepository
    ):
        self._incubator = incubator  # Abstract interface
        self._arm = arm
        self._microscopes = microscopes
        self._scheduler = scheduler
        self._tasks = task_repo
```

**Benefit:** Can test Coordinator with mocks. No Hypha server needed for unit tests.

### 3.4 API Layer Separation

**New File:** `services/orchestrator/api/hypha_routes.py`

```python
class OrchestratorApi:
    """Thin layer - just converts RPC calls to Coordinator methods."""
    
    def __init__(self, coordinator: Coordinator):
        self._coord = coordinator
    
    @schema_function
    async def load_plate(self, slot: int, microscope_id: str) -> ApiResponse:
        try:
            await self._coord.load_sample(slot, microscope_id)
            return ApiResponse.success()
        except Exception as e:
            return ApiResponse.error(str(e))
```

---

## Phase 4: API Standardization (3 days)

### 4.1 Pydantic Schemas for All Inputs/Outputs

**New File:** `api/schemas.py`

```python
class LoadPlateRequest(BaseModel):
    incubator_slot: int = Field(..., ge=1, le=44)
    microscope_id: str

class ScanRequest(BaseModel):
    microscope_id: str
    wells_to_scan: List[str]
    illumination_settings: List[IlluminationConfig]
    
    @validator('wells_to_scan')
    def validate_wells(cls, v):
        for well in v:
            if not re.match(r'^[A-H][1-12]$', well):
                raise ValueError(f'Invalid well format: {well}')
        return v
```

### 4.2 Consistent Error Handling

All services return:
```python
{
    "status": "success" | "error",
    "data": T | null,
    "message": string | null,
    "error_code": string | null  # Machine-readable error code
}
```

**Error Code Standard:**
- `CONN_FAILED` - Connection to service failed
- `HARDWARE_ERROR` - Hardware-specific error
- `INVALID_STATE` - Invalid task state transition
- `NOT_FOUND` - Sample/task not found
- `TIMEOUT` - Operation timeout

---

## Testing Strategy

### Unit Tests
```
tests/
├── unit/
│   ├── core/
│   │   ├── test_scheduler.py      # Pure logic - easy to test
│   │   ├── test_state_machine.py  # State transitions
│   │   └── test_transport.py      # Transport planning
│   ├── infrastructure/
│   │   ├── test_health_monitor.py
│   │   └── test_sample_repo.py
│   └── services/
│       └── test_coordinator.py    # With mocked ports
```

### Integration Tests
```
tests/
├── integration/
│   ├── test_incubator_service.py
│   ├── test_robotic_arm_service.py
│   └── test_end_to_end.py         # Full workflow
```

### Test Fixtures
```python
@pytest.fixture
def mock_incubator():
    """Mock incubator for testing Coordinator."""
    return AsyncMock(spec=IncubatorPort)

@pytest.fixture
def coordinator(mock_incubator, mock_arm, ...):
    """Coordinator with all mocks."""
    return Coordinator(
        incubator=mock_incubator,
        arm=mock_arm,
        ...
    )
```

---

## Migration Guide

### For Developers

1. **Start with Phase 0 branch:**
   ```bash
   git checkout refactor/extract-config-store
   ```

2. **Implement phases incrementally:**
   - Each phase should maintain backward compatibility
   - Run existing tests after each phase
   - Add new tests before refactoring (characterization tests)

3. **Hardware testing required for:**
   - Phase 1 (hardware drivers)
   - Phase 3 (orchestrator transport execution)

### Backward Compatibility

- **Config format:** Unchanged
- **API responses:** Migration period with deprecation warnings
- **Service IDs:** Unchanged
- **Environment variables:** Add new ones, keep old ones as fallback

---

## Benefits Summary

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Largest class | 1,600 lines | 200 lines | **8x smaller** |
| Duplicated code | 180+ lines | 0 lines | **Eliminated** |
| Test coverage | ~0% | >80% | **From 0 to 80%** |
| Hardware coupling | Tight | Loose | **Swappable** |
| State management | Ad-hoc | State machine | **Validated** |
| API consistency | Mixed | Standardized | **Consistent** |

---

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Hardware regression | Medium | High | Keep existing tests, add characterization tests |
| Performance degradation | Low | Medium | Benchmark before/after |
| Breaking API changes | Low | High | Deprecation period, versioned APIs |
| Developer productivity dip | Medium | Low | Pair programming, clear documentation |

---

## Next Steps

1. **Review this plan** with the team
2. **Set up CI/CD** if not already done
3. **Create characterization tests** for current behavior
4. **Implement Phase 1** (infrastructure layer)
5. **Hardware testing** after each phase
6. **Celebrate** when done 🎉

---

## Resources

- **Architecture Review:** `/memory/2026-03-20-comprehensive-architecture-review.md`
- **Similar Projects Research:** `/memory/2026-03-19-similar-projects.md`
- **Refactoring Patterns:** See `python-statemachine`, `dependency-injector` libraries

---

*End of Plan*
