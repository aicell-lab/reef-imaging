import json
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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

        response = await orchestrator.load_plate_from_incubator_to_microscope_api(1, "microscope-squid-1")

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
