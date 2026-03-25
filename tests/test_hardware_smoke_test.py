import tempfile
import unittest
from pathlib import Path

from reef_imaging.hardware_smoke_test import (
    HardwareSmokeTestRunner,
    build_cycle_plan,
    parse_slot_selection,
)


class FakeOrchestratorService:
    def __init__(self, *, runtime_status=None, samples=None, fail_step=None):
        self.runtime_status = runtime_status or {
            "success": True,
            "active_operations": [],
            "in_critical_operation": False,
            "connected_services": {
                "incubator": True,
                "robotic_arm": True,
                "microscopes": {
                    "microscope-squid-1": True,
                },
            },
            "configured_microscopes": ["microscope-squid-1"],
        }
        self.samples = samples or [
            {
                "incubator_slot": 1,
                "name": "sample-a",
                "status": "IN",
                "location": "incubator_slot",
                "well_plate_type": "96",
                "available": True,
            }
        ]
        self.fail_step = fail_step
        self.calls = []
        self.cancel_calls = 0
        self.halt_calls = 0

    async def get_runtime_status(self):
        self.calls.append(("get_runtime_status",))
        return self.runtime_status

    async def get_incubator_samples(self, slot=None, only_available=False):
        self.calls.append(("get_incubator_samples", slot, only_available))
        samples = list(self.samples)
        if slot is not None:
            samples = [sample for sample in samples if sample["incubator_slot"] == slot]
        if only_available:
            samples = [sample for sample in samples if sample.get("available")]
        return {"success": True, "samples": samples}

    async def load_plate_from_incubator_to_microscope(self, *, incubator_slot, microscope_id):
        self.calls.append(("load", incubator_slot, microscope_id))
        if self.fail_step == "load":
            return {"success": False, "message": "load failed"}
        sample = next(sample for sample in self.samples if sample["incubator_slot"] == incubator_slot)
        sample["location"] = f"on_{microscope_id}"
        return {"success": True}

    async def scan_microscope_only(self, *, microscope_id, scan_config, action_id):
        self.calls.append(("scan", microscope_id, action_id, scan_config["well_plate_type"]))
        if self.fail_step == "scan":
            return {"success": False, "message": "scan failed"}
        return {"success": True, "action_id": action_id}

    async def unload_plate_from_microscope(self, *, incubator_slot, microscope_id):
        self.calls.append(("unload", incubator_slot, microscope_id))
        if self.fail_step == "unload":
            return {"success": False, "message": "unload failed"}
        sample = next(sample for sample in self.samples if sample["incubator_slot"] == incubator_slot)
        sample["location"] = "incubator_slot"
        return {"success": True}

    async def cancel_microscope_scan(self, *, microscope_id):
        self.calls.append(("cancel_microscope_scan", microscope_id))
        self.cancel_calls += 1
        return {"success": True}

    async def halt_robotic_arm(self):
        self.calls.append(("halt_robotic_arm",))
        self.halt_calls += 1
        return {"success": True}


class HardwareSmokeTestHelpers(unittest.TestCase):
    def test_parse_slot_selection_accepts_unique_slots_up_to_limit(self):
        self.assertEqual(parse_slot_selection("1, 3,5", [1, 2, 3, 4, 5]), [1, 3, 5])

    def test_parse_slot_selection_rejects_invalid_requests(self):
        with self.assertRaises(ValueError):
            parse_slot_selection("", [1, 2])
        with self.assertRaises(ValueError):
            parse_slot_selection("1,1", [1, 2])
        with self.assertRaises(ValueError):
            parse_slot_selection("1,2,3,4,5,6", [1, 2, 3, 4, 5, 6])
        with self.assertRaises(ValueError):
            parse_slot_selection("9", [1, 2, 3])

    def test_build_cycle_plan_expands_samples_across_all_microscopes(self):
        cycles = build_cycle_plan(
            [
                {"incubator_slot": 1, "name": "alpha", "well_plate_type": "96"},
                {"incubator_slot": 2, "name": "beta", "well_plate_type": "384"},
            ],
            ["microscope-squid-1", "microscope-squid-2"],
        )

        self.assertEqual(
            [(cycle.incubator_slot, cycle.microscope_id) for cycle in cycles],
            [
                (1, "microscope-squid-1"),
                (1, "microscope-squid-2"),
                (2, "microscope-squid-1"),
                (2, "microscope-squid-2"),
            ],
        )


class HardwareSmokeTestRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_runner_refuses_to_start_when_orchestrator_is_busy(self):
        service = FakeOrchestratorService(
            runtime_status={
                "success": True,
                "active_operations": [{"operation_id": "op-1"}],
                "in_critical_operation": False,
                "connected_services": {
                    "incubator": True,
                    "robotic_arm": True,
                    "microscopes": {"microscope-squid-1": True},
                },
                "configured_microscopes": ["microscope-squid-1"],
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = HardwareSmokeTestRunner(
                service,
                output_fn=lambda message: None,
                report_root=Path(temp_dir),
            )
            summary = await runner.run()

        self.assertEqual(summary["status"], "failed")
        self.assertIn("busy", summary["message"])

    async def test_runner_rejects_missing_microscope_preflight(self):
        service = FakeOrchestratorService(
            runtime_status={
                "success": True,
                "active_operations": [],
                "in_critical_operation": False,
                "connected_services": {
                    "incubator": True,
                    "robotic_arm": True,
                    "microscopes": {
                        "microscope-squid-1": True,
                        "microscope-squid-2": False,
                    },
                },
                "configured_microscopes": ["microscope-squid-1", "microscope-squid-2"],
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = HardwareSmokeTestRunner(
                service,
                output_fn=lambda message: None,
                report_root=Path(temp_dir),
            )
            summary = await runner.run()

        self.assertEqual(summary["status"], "failed")
        self.assertIn("not connected", summary["message"])

    async def test_runner_uses_load_scan_unload_sequence_for_each_cycle(self):
        service = FakeOrchestratorService()
        answers = iter(["1", "y", "y"])

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = HardwareSmokeTestRunner(
                service,
                input_fn=lambda prompt: next(answers),
                output_fn=lambda message: None,
                report_root=Path(temp_dir),
            )
            summary = await runner.run()
            self.assertTrue(Path(summary["report_dir"]).joinpath("summary.json").exists())
            self.assertTrue(Path(summary["report_dir"]).joinpath("run.log").exists())

        self.assertEqual(summary["status"], "completed")
        self.assertEqual(
            [call[0] for call in service.calls if call[0] in {"load", "scan", "unload"}],
            ["load", "scan", "unload"],
        )

    async def test_runner_stops_after_first_failure_and_offers_emergency_actions(self):
        service = FakeOrchestratorService(
            samples=[
                {
                    "incubator_slot": 1,
                    "name": "sample-a",
                    "status": "IN",
                    "location": "incubator_slot",
                    "well_plate_type": "96",
                    "available": True,
                },
                {
                    "incubator_slot": 2,
                    "name": "sample-b",
                    "status": "IN",
                    "location": "incubator_slot",
                    "well_plate_type": "384",
                    "available": True,
                },
            ],
            fail_step="scan",
        )
        answers = iter(["1,2", "y", "y", "cancel-scan", "halt-robot", ""])

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = HardwareSmokeTestRunner(
                service,
                input_fn=lambda prompt: next(answers),
                output_fn=lambda message: None,
                report_root=Path(temp_dir),
            )
            summary = await runner.run()

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(service.cancel_calls, 1)
        self.assertEqual(service.halt_calls, 1)
        load_calls = [call for call in service.calls if call[0] == "load"]
        self.assertEqual(load_calls, [("load", 1, "microscope-squid-1")])
        self.assertEqual(summary["failure"]["cycle"]["incubator_slot"], 1)
