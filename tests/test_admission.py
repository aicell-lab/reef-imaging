import asyncio
import unittest

from reef_imaging.orchestration import (
    AdmissionRequest,
    OperationAdmissionController,
    ResourceBusyError,
)


class OperationAdmissionControllerTests(unittest.IsolatedAsyncioTestCase):
    async def test_try_acquire_rejects_busy_resources(self):
        controller = OperationAdmissionController()
        first_request = AdmissionRequest(
            operation_id="op-1",
            operation_type="manual-load",
            resources=("microscope:microscope-squid-1", "transport-lane"),
        )
        conflicting_request = AdmissionRequest(
            operation_id="op-2",
            operation_type="manual-scan",
            resources=("microscope:microscope-squid-1",),
        )

        lease = await controller.try_acquire(first_request)

        with self.assertRaises(ResourceBusyError) as context:
            await controller.try_acquire(conflicting_request)

        self.assertEqual(context.exception.blockers[0].resource, "microscope:microscope-squid-1")
        await controller.release(lease.operation_id)

    async def test_acquire_waits_until_resources_are_released(self):
        controller = OperationAdmissionController()
        initial_request = AdmissionRequest(
            operation_id="op-1",
            operation_type="scheduled-cycle",
            resources=("microscope:microscope-squid-2",),
        )
        waiting_request = AdmissionRequest(
            operation_id="op-2",
            operation_type="manual-scan",
            resources=("microscope:microscope-squid-2",),
        )

        initial_lease = await controller.try_acquire(initial_request)

        async def release_later():
            await asyncio.sleep(0.05)
            await controller.release(initial_lease.operation_id)

        release_task = asyncio.create_task(release_later())
        waiting_lease = await controller.acquire(waiting_request, timeout=1.0)

        self.assertEqual(waiting_lease.operation_id, "op-2")

        await controller.release(waiting_lease.operation_id)
        await release_task
