import importlib
from datetime import datetime
import json
import logging
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


def _install_hypha_rpc_stub() -> None:
    if "hypha_rpc" in sys.modules:
        return

    hypha_rpc_module = types.ModuleType("hypha_rpc")

    async def _connect_to_server(*args, **kwargs):
        raise RuntimeError("connect_to_server should be mocked in unit tests")

    hypha_rpc_module.connect_to_server = _connect_to_server

    hypha_rpc_utils_module = types.ModuleType("hypha_rpc.utils")
    hypha_rpc_schema_module = types.ModuleType("hypha_rpc.utils.schema")

    def schema_function(*args, **kwargs):
        def decorator(func):
            return func

        return decorator

    hypha_rpc_schema_module.schema_function = schema_function

    sys.modules["hypha_rpc"] = hypha_rpc_module
    sys.modules["hypha_rpc.utils"] = hypha_rpc_utils_module
    sys.modules["hypha_rpc.utils.schema"] = hypha_rpc_schema_module


def _install_dotenv_stub() -> None:
    if "dotenv" in sys.modules:
        return

    dotenv_module = types.ModuleType("dotenv")
    dotenv_module.load_dotenv = lambda *args, **kwargs: None
    dotenv_module.find_dotenv = lambda *args, **kwargs: ""
    sys.modules["dotenv"] = dotenv_module


_install_hypha_rpc_stub()
_install_dotenv_stub()
orchestrator_module = importlib.import_module("reef_imaging.orchestrator")


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
    def __init__(self, *, status=None, run_state=None, analysis_result=None):
        self.status = status or {
            "busy": False,
            "active_runs": [],
            "total_runs": 0,
        }
        self.run_state = run_state or {
            "status": "succeeded",
            "run_id": "run-1",
        }
        self.analysis_result = analysis_result or {
            "analysis": {"status": "accepted", "rejections": [], "warnings": []},
            "commands": [],
            "command_count": 0,
        }
        self.analyze_calls: list = []
        self.submit_calls: list = []
        self.play_calls: list = []
        self.get_run_calls: list = []

    async def ping(self):
        return "pong"

    async def get_status(self):
        return dict(self.status)

    async def analyze_protocol(self, protocol_source, manifest=None):
        self.analyze_calls.append({"protocol_source": protocol_source, "manifest": manifest})
        return dict(self.analysis_result)

    async def submit_protocol(self, protocol_source, manifest=None):
        self.submit_calls.append({"protocol_source": protocol_source, "manifest": manifest})
        return {"run_id": "run-1", "status": "idle"}

    async def play_run(self, run_id):
        self.play_calls.append(run_id)
        return {"run_id": run_id, "status": "running"}

    async def get_run(self, run_id):
        self.get_run_calls.append(run_id)
        return dict(self.run_state)

class FakeIncubator:
    def __init__(self, location="incubator_slot"):
        self.location = location
        self.transfer_requests = []
        self.location_updates = []
        self.slot_puts = []

    async def get_sample_location(self, slot):
        return self.location

    async def get_sample_from_slot_to_transfer_station(self, slot):
        self.transfer_requests.append(slot)

    async def update_sample_location(self, slot, location):
        self.location = location
        self.location_updates.append((slot, location))

    async def put_sample_from_transfer_station_to_slot(self, slot):
        self.slot_puts.append(slot)


class FakeRoboticArm:
    def __init__(self):
        self.calls = []
        self.actions = []

    async def transport_plate(self, **kwargs):
        self.calls.append(kwargs)

    async def execute_action(self, action_id):
        self.actions.append(action_id)
        return True


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

    async def test_task_api_uses_patched_config_paths(self):
        config_data = {
            "samples": [],
            "microscopes": [{"id": "microscope-squid-1"}],
        }
        new_task = _build_sample("task-gamma", 7, "microscope-squid-1", "C3", "gamma")

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
                add_result = await orchestrator.add_imaging_task(new_task)
                all_tasks = await orchestrator.get_all_imaging_tasks()
                delete_result = await orchestrator.delete_imaging_task("task-gamma")

            rewritten_data = json.loads(config_path.read_text(encoding="utf-8"))

            self.assertTrue(add_result["success"])
            self.assertEqual([task["name"] for task in all_tasks], ["task-gamma"])
            self.assertTrue(delete_result["success"])
            self.assertEqual(rewritten_data["samples"], [])

    async def test_start_due_task_copies_task_config_without_name_error(self):
        orchestrator = orchestrator_module.OrchestrationSystem()
        due_time = datetime.now()
        orchestrator.tasks = {
            "task-alpha": {
                "config": {
                    "name": "task-alpha",
                    "scan_mode": "microscope_only",
                    "saved_data_type": "raw_images_well_plate",
                    "allocated_microscope": "microscope-squid-1",
                    "pending_datetimes": [due_time],
                    "illumination_settings": [{"channel": "BF"}],
                    "do_contrast_autofocus": True,
                    "do_reflection_af": False,
                    "wells_to_scan": ["A1"],
                    "Nx": 1,
                    "Ny": 1,
                    "dx": 0.8,
                    "dy": 0.8,
                },
                "status": "pending",
            }
        }
        orchestrator.configured_microscopes_info = {"microscope-squid-1": {"id": "microscope-squid-1"}}
        orchestrator.microscope_services = {"microscope-squid-1": object()}
        orchestrator._update_task_state_and_write_config = AsyncMock()
        orchestrator._run_scheduled_cycle = AsyncMock()

        started = await orchestrator._start_due_task("task-alpha", due_time)

        self.assertTrue(started)
        await orchestrator._scheduled_cycle_tasks["task-alpha"]
        orchestrator._run_scheduled_cycle.assert_awaited_once()

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
        self.assertEqual(response["service_id"], "hamilton-control-service")
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
        self.assertEqual(executor.submit_calls, [])

        await orchestrator.admission_controller.release(lease.operation_id)

    async def test_run_hamilton_protocol_uses_analyze_submit_play_poll(self):
        orchestrator = orchestrator_module.OrchestrationSystem()
        executor = FakeHamiltonExecutor(
            run_state={
                "status": "succeeded",
                "run_id": "run-1",
            },
            status={
                "busy": False,
                "active_runs": [],
                "total_runs": 1,
            },
        )
        orchestrator.hamilton_executor = executor
        orchestrator.robotic_arm = FakeRoboticArm()

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
        self.assertEqual(response["message"], "Hamilton protocol succeeded.")
        self.assertEqual(response["run_id"], "run-1")
        self.assertEqual(orchestrator.robotic_arm.actions, ["move_plate_rail_to_hamilton"])
        self.assertEqual(executor.analyze_calls[0]["protocol_source"], "print('run protocol')")
        self.assertEqual(executor.submit_calls[0]["protocol_source"], "print('run protocol')")
        self.assertEqual(executor.play_calls, ["run-1"])
        self.assertEqual(executor.get_run_calls, ["run-1"])

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
                "active_runs": ["run-busy"],
                "total_runs": 1,
            }
        )

        response = await orchestrator.transport_plate_api("incubator", "hamilton", slot=3)

        self.assertFalse(response["success"])
        self.assertEqual(response["state"], "busy")
        self.assertIn("Hamilton is busy", response["message"])
        self.assertEqual(orchestrator.robotic_arm.calls, [])

    async def test_transport_to_hamilton_fails_when_sample_not_in_incubator(self):
        orchestrator = orchestrator_module.OrchestrationSystem()
        orchestrator.incubator = FakeIncubator(location="hamilton")
        orchestrator.robotic_arm = FakeRoboticArm()
        orchestrator.hamilton_executor = FakeHamiltonExecutor()

        response = await orchestrator.transport_plate_api("incubator", "hamilton", slot=3)

        self.assertFalse(response["success"])
        self.assertIn("expected sample at incubator_slot, found hamilton", response["message"])
        self.assertEqual(orchestrator.robotic_arm.calls, [])

    async def test_transport_from_hamilton_to_incubator_fails_when_sample_not_at_hamilton(self):
        orchestrator = orchestrator_module.OrchestrationSystem()
        orchestrator.incubator = FakeIncubator(location="incubator_slot")
        orchestrator.robotic_arm = FakeRoboticArm()
        orchestrator.hamilton_executor = FakeHamiltonExecutor()

        response = await orchestrator.transport_plate_api("hamilton", "incubator", slot=3)

        self.assertFalse(response["success"])
        self.assertIn("expected sample at hamilton, found incubator_slot", response["message"])
        self.assertEqual(orchestrator.robotic_arm.calls, [])

    async def test_transport_from_microscope_to_hamilton_fails_when_sample_not_on_source(self):
        orchestrator = orchestrator_module.OrchestrationSystem()
        orchestrator.incubator = FakeIncubator(location="incubator_slot")
        orchestrator.robotic_arm = FakeRoboticArm()
        orchestrator.hamilton_executor = FakeHamiltonExecutor()
        orchestrator.configured_microscopes_info = {"microscope-squid-1": {"id": "microscope-squid-1"}}
        orchestrator.microscope_services = {"microscope-squid-1": object()}

        response = await orchestrator.transport_plate_api("microscope-squid-1", "hamilton", slot=3)

        self.assertFalse(response["success"])
        self.assertIn("expected sample at microscope-squid-1, found incubator_slot", response["message"])
        self.assertEqual(orchestrator.robotic_arm.calls, [])

    async def test_transport_from_hamilton_to_microscope_fails_when_sample_not_at_hamilton(self):
        orchestrator = orchestrator_module.OrchestrationSystem()
        orchestrator.incubator = FakeIncubator(location="incubator_slot")
        orchestrator.robotic_arm = FakeRoboticArm()
        orchestrator.hamilton_executor = FakeHamiltonExecutor()
        orchestrator.configured_microscopes_info = {"microscope-squid-1": {"id": "microscope-squid-1"}}
        orchestrator.microscope_services = {"microscope-squid-1": object()}

        response = await orchestrator.transport_plate_api("hamilton", "microscope-squid-1", slot=3)

        self.assertFalse(response["success"])
        self.assertIn("expected sample at hamilton, found incubator_slot", response["message"])
        self.assertEqual(orchestrator.robotic_arm.calls, [])
