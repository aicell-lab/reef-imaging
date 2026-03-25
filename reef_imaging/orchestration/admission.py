"""Admission control for orchestrator operations."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class AdmissionRequest:
    """Describes an operation that needs exclusive access to resources."""

    operation_id: str
    operation_type: str
    resources: tuple[str, ...]
    microscope_id: str | None = None
    incubator_slot: int | None = None
    task_name: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True)
class BlockedResource:
    """A resource that is already held by another active operation."""

    resource: str
    operation_id: str
    operation_type: str
    microscope_id: str | None = None
    incubator_slot: int | None = None
    task_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource": self.resource,
            "operation_id": self.operation_id,
            "operation_type": self.operation_type,
            "microscope_id": self.microscope_id,
            "incubator_slot": self.incubator_slot,
            "task_name": self.task_name,
        }


@dataclass(frozen=True)
class OperationLease:
    """An active reservation returned by the admission controller."""

    request: AdmissionRequest
    started_at: datetime

    @property
    def operation_id(self) -> str:
        return self.request.operation_id

    @property
    def operation_type(self) -> str:
        return self.request.operation_type

    @property
    def resources(self) -> tuple[str, ...]:
        return self.request.resources

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation_id": self.request.operation_id,
            "operation_type": self.request.operation_type,
            "task_name": self.request.task_name,
            "microscope_id": self.request.microscope_id,
            "incubator_slot": self.request.incubator_slot,
            "resources": list(self.request.resources),
            "started_at": self.started_at.strftime("%Y-%m-%dT%H:%M:%S"),
            "metadata": dict(self.request.metadata or {}),
        }


class ResourceBusyError(RuntimeError):
    """Raised when an operation cannot start because required resources are busy."""

    def __init__(self, request: AdmissionRequest, blockers: tuple[BlockedResource, ...]):
        self.request = request
        self.blockers = blockers
        blocker_summary = ", ".join(
            f"{blocker.resource} ({blocker.operation_type}:{blocker.operation_id})"
            for blocker in blockers
        ) or "unknown"
        super().__init__(
            f"Operation '{request.operation_type}' cannot start because resources are busy: "
            f"{blocker_summary}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": str(self),
            "operation_id": self.request.operation_id,
            "operation_type": self.request.operation_type,
            "blocked_by": [blocker.to_dict() for blocker in self.blockers],
        }


class OperationAdmissionController:
    """Tracks active operations and the resources they hold."""

    def __init__(self):
        self._condition = asyncio.Condition()
        self._resource_to_operation: dict[str, str] = {}
        self._leases: dict[str, OperationLease] = {}

    def _collect_blockers(self, resources: tuple[str, ...]) -> tuple[BlockedResource, ...]:
        blockers: list[BlockedResource] = []
        seen_operations: set[str] = set()
        for resource in resources:
            operation_id = self._resource_to_operation.get(resource)
            if not operation_id or operation_id in seen_operations:
                continue
            lease = self._leases[operation_id]
            blockers.append(
                BlockedResource(
                    resource=resource,
                    operation_id=lease.operation_id,
                    operation_type=lease.operation_type,
                    microscope_id=lease.request.microscope_id,
                    incubator_slot=lease.request.incubator_slot,
                    task_name=lease.request.task_name,
                )
            )
            seen_operations.add(operation_id)
        return tuple(blockers)

    def _grant(self, request: AdmissionRequest) -> OperationLease:
        lease = OperationLease(request=request, started_at=datetime.now())
        self._leases[request.operation_id] = lease
        for resource in request.resources:
            self._resource_to_operation[resource] = request.operation_id
        return lease

    async def try_acquire(self, request: AdmissionRequest) -> OperationLease:
        """Acquire resources immediately or raise if another operation holds them."""

        async with self._condition:
            blockers = self._collect_blockers(request.resources)
            if blockers:
                raise ResourceBusyError(request, blockers)
            return self._grant(request)

    async def acquire(self, request: AdmissionRequest, timeout: float | None = None) -> OperationLease:
        """Wait until resources become available, or raise on timeout."""

        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout

        async with self._condition:
            while True:
                blockers = self._collect_blockers(request.resources)
                if not blockers:
                    return self._grant(request)

                if deadline is not None:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise ResourceBusyError(request, blockers)
                    try:
                        await asyncio.wait_for(self._condition.wait(), timeout=remaining)
                    except asyncio.TimeoutError as exc:
                        raise ResourceBusyError(request, blockers) from exc
                else:
                    await self._condition.wait()

    async def release(self, operation_id: str) -> None:
        """Release all resources associated with an operation id."""

        async with self._condition:
            lease = self._leases.pop(operation_id, None)
            if lease is None:
                return

            for resource in lease.resources:
                if self._resource_to_operation.get(resource) == operation_id:
                    del self._resource_to_operation[resource]
            self._condition.notify_all()

    async def snapshot(self) -> dict[str, Any]:
        """Return a serializable view of the active reservations."""

        async with self._condition:
            leases = sorted(self._leases.values(), key=lambda current: current.started_at)
            return {
                "active_operations": [lease.to_dict() for lease in leases],
                "held_resources": dict(sorted(self._resource_to_operation.items())),
            }

    @asynccontextmanager
    async def hold(
        self,
        request: AdmissionRequest,
        *,
        wait: bool = False,
        timeout: float | None = None,
    ):
        """Acquire resources for the lifetime of the context manager."""

        if wait:
            lease = await self.acquire(request, timeout=timeout)
        else:
            lease = await self.try_acquire(request)

        try:
            yield lease
        finally:
            await self.release(lease.operation_id)
