import json
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import reef_imaging.orchestrator as orchestrator_module


def _build_sample(name: str, slot: int, microscope_id: str, well: str, note: str) -> dict:
    return {
        "name": name,
        "settings": {
            "scan_mode": "full_automation",
            "saved_data_type": "raw_images_well_plate",
            "incubator_slot": slot,
            "allocated_microscope": microscope_id,
            "pending_time_points": ["2026-03-25T12:00:00"],
            "imaged_time_points": [],
            "wells_to_scan": [well],
            "Nx": 1,
            "Ny": 1,
            "dx": 0.8,
            "dy": 0.8,
            "illumination_settings": [{"channel": "BF"}],
            "do_contrast_autofocus": True,
            "do_reflection_af": False,
            "custom_note": note,
        },
        "operational_state": {"status": "pending"},
    }


class FakeHamiltonExecutor:
    def __init__(self, *, status=None, start_result=None, poll_results=None):
        self.status = status or {
            "busy": False,
            "last_action_id": None,
            "success": True,
        }
        self.start_result = start_result or {
            "accepted": True,
            "busy": False,
            "action_id": "action-1",
            "status": "running",
        }
        self.poll_results = list(poll_results or [])
        self.start_calls = []
        self.poll_calls = 0

    async def ping(self):
        return "pong"

    async def get_status(self):
        return dict(self.status)

    async def start_execution(self, script_content, timeout):
        self.start_calls.append({"script_content": script_content, "timeout": timeout})
        return dict(self.start_result)

    async def poll_status(self, n_lines=100):
        self.poll_calls += 1
        if self.poll_results:
            result = dict(self.poll_results.pop(0))
            self.status = dict(result)
            return result
        return dict(self.status)


class FakeIncubator:
    def __init__(self, location="incubator_slot"):
        self.location = location
        self.transfer_requests = []
        self.location_updates = []

    async def get_sample_location(self, slot):
        return self.location

    async def get_sample_from_slot_to_transfer_station(self, slot):
        self.transfer_requests.append(slot)

    async def update_sample_location(self, slot, location):
        self.location = location
        self.location_updates.append((slot, location))


class FakeRoboticArm:
    def __init__(self):
        self.calls = []

    async def transport_plate(self, **kwargs):
        self.calls.append(kwargs)


class OrchestratorRefactorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logger = logging.getLogger("reef-imaging-tests")
        logger.handlers.clear()
        logger.addHandler(logging.NullHandler())
        orchestrator_module.logger = logger

    async def test_load_update_preserves_per_task_raw_settings(self):
        config_data = {
            "samples": [
                _build_sample("task-alpha", 1, "microscope-squid-1", "A1", "alpha"),
                _build_sample("task-beta", 2, "microscope-squid-2", "B2", "beta"),
            ],
            "microscopes": [
                {"id": "microscope-squid-1"},
                {"id": "microscope-squid-2"},
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            tmp_path = Path(temp_dir) / "config.json.tmp"
            config_path.write_text(json.dumps(config_data, indent=4), encoding="utf-8")

            orchestrator = orchestrator_module.OrchestrationSystem()

            with patch.object(orchestrator_module, "CONFIG_FILE_PATH", str(config_path)), patch.object(
                orchestrator_module,
                "CONFIG_FILE_PATH_TMP",
                str(tmp_path),
            ):
                await orchestrator._load_and_update_tasks()
                await orchestrator._write_tasks_to_config()

            rewritten_data = json.loads(config_path.read_text(encoding="utf-8"))
            rewritten_samples = {sample["name"]: sample["settings"] for sample in rewritten_data["samples"]}

            self.assertEqual(rewritten_samples["task-alpha"]["custom_note"], "alpha")
            self.assertEqual(rewritten_samples["task-beta"]["custom_note"], "beta")

    async def test_load_api_rejects_busy_transport_resources(self):
        orchestrator = orchestrator_module.OrchestrationSystem()
        orchestrator.configured_microscopes_info = {"microscope-squid-1": {"id": "microscope-squid-1"}}
        orchestrator.incubator = object()
        orchestrator.robotic_arm = object()
        orchestrator.microscope_services = {"microscope-squid-1": object()}

        request = orchestrator._build_request(
            "existing-operation",
            microscope_id="microscope-squid-1",
            incubator_slot=1,
            extra_resources=orchestrator._transport_resources(),
        )
        lease = await orchestrator.admission_controller.try_acquire(request)

        response = await orchestrator.transport_plate_api("incubator", "microscope-squid-1", slot=1)

        self.assertFalse(response["success"])
        self.assertEqual(response["state"], "busy")

        await orchestrator.admission_controller.release(lease.operation_id)

    async def test_scan_api_rejects_busy_microscope(self):
        orchestrator = orchestrator_module.OrchestrationSystem()
        orchestrator.configured_microscopes_info = {"microscope-squid-1": {"id": "microscope-squid-1"}}
        orchestrator.microscope_services = {"microscope-squid-1": object()}

        request = orchestrator._build_request(
            "existing-scan",
            microscope_id="microscope-squid-1",
        )
        lease = await orchestrator.admission_controller.try_acquire(request)

        response = await orchestrator.scan_microscope_only_api(
            "microscope-squid-1",
            {
                "saved_data_type": "raw_images_well_plate",
                "illumination_settings": [{"channel": "BF"}],
                "do_contrast_autofocus": True,
                "do_reflection_af": False,
                "wells_to_scan": ["A1"],
                "Nx": 1,
                "Ny": 1,
                "dx": 0.8,
                "dy": 0.8,
            },
        )

        self.assertFalse(response["success"])
        self.assertEqual(response["state"], "busy")

        await orchestrator.admission_controller.release(lease.operation_id)

    async def test_get_hamilton_status_reports_disconnected_when_executor_missing(self):
        orchestrator = orchestrator_module.OrchestrationSystem()

        with patch.object(orchestrator, "setup_connections", new=AsyncMock(return_value=False)):
            response = await orchestrator.get_hamilton_status()

        self.assertTrue(response["success"])
        self.assertFalse(response["connected"])
        self.assertEqual(response["service_id"], "hamilton-script-executor")
        self.assertIsNone(response["executor_status"])

    async def test_run_hamilton_protocol_rejects_empty_script(self):
        orchestrator = orchestrator_module.OrchestrationSystem()

        response = await orchestrator.run_hamilton_protocol("   ")

        self.assertFalse(response["success"])
        self.assertIn("script_content", response["message"])

    async def test_run_hamilton_protocol_respects_hamilton_admission_lock(self):
        orchestrator = orchestrator_module.OrchestrationSystem()
        executor = FakeHamiltonExecutor()
        orchestrator.hamilton_executor = executor

        request = orchestrator._build_request(
            "existing-hamilton-work",
            extra_resources=(orchestrator._hamilton_resource(),),
        )
        lease = await orchestrator.admission_controller.try_acquire(request)

        response = await orchestrator.run_hamilton_protocol("print('hello from hamilton')")

        self.assertFalse(response["success"])
        self.assertEqual(response["state"], "busy")
        self.assertEqual(executor.start_calls, [])

        await orchestrator.admission_controller.release(lease.operation_id)

    async def test_run_hamilton_protocol_forwards_script_and_timeout_to_executor(self):
        orchestrator = orchestrator_module.OrchestrationSystem()
        executor = FakeHamiltonExecutor(
            start_result={
                "accepted": True,
                "busy": False,
                "action_id": "action-123",
                "status": "running",
            },
            poll_results=[
                {"busy": True, "last_action_id": None, "success": False},
                {
                    "busy": False,
                    "last_action_id": "action-123",
                    "success": True,
                    "output": "protocol complete",
                },
            ],
        )
        orchestrator.hamilton_executor = executor

        with patch.object(
            orchestrator,
            "get_runtime_status",
            new=AsyncMock(return_value={"success": True, "connected_services": {"hamilton": True}}),
        ):
            response = await orchestrator.run_hamilton_protocol(
                "print('run protocol')",
                timeout=3600,
            )

        self.assertTrue(response["success"])
        self.assertEqual(executor.start_calls[0]["script_content"], "print('run protocol')")
        self.assertEqual(executor.start_calls[0]["timeout"], 3600)
        self.assertEqual(response["execution_result"]["last_action_id"], "action-123")
        self.assertGreaterEqual(executor.poll_calls, 2)

    async def test_hamilton_execution_lock_does_not_block_unrelated_microscope_resource(self):
        orchestrator = orchestrator_module.OrchestrationSystem()

        hamilton_request = orchestrator._build_request(
            "hamilton-execution",
            extra_resources=(orchestrator._hamilton_resource(),),
        )
        hamilton_lease = await orchestrator.admission_controller.try_acquire(hamilton_request)

        microscope_request = orchestrator._build_request(
            "scan-microscope-only",
            microscope_id="microscope-squid-1",
        )
        microscope_lease = await orchestrator.admission_controller.try_acquire(microscope_request)

        self.assertTrue(microscope_lease.operation_id.startswith("scan-microscope-only-"))

        await orchestrator.admission_controller.release(microscope_lease.operation_id)
        await orchestrator.admission_controller.release(hamilton_lease.operation_id)

    async def test_transport_to_hamilton_rejects_when_executor_is_busy(self):
        orchestrator = orchestrator_module.OrchestrationSystem()
        orchestrator.incubator = FakeIncubator(location="incubator_slot")
        orchestrator.robotic_arm = FakeRoboticArm()
        orchestrator.hamilton_executor = FakeHamiltonExecutor(
            status={
                "busy": True,
                "current_action_id": "action-busy",
                "success": False,
            }
        )

        response = await orchestrator.transport_plate_api("incubator", "hamilton", slot=3)

        self.assertFalse(response["success"])
        self.assertEqual(response["state"], "busy")
        self.assertIn("Hamilton is busy", response["message"])
        self.assertEqual(orchestrator.robotic_arm.calls, [])
