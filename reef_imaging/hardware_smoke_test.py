import asyncio
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, List, Sequence

import dotenv
from hypha_rpc import connect_to_server

# Load environment variables from .env file
dotenv.load_dotenv()

DEFAULT_LOCAL_SERVER_URL = os.environ.get("REEF_LOCAL_SERVER_URL", "http://reef.dyn.scilifelab.se:9527")
DEFAULT_REPORT_ROOT = Path(__file__).resolve().parent.parent / "hardware_test_reports"
DEFAULT_ILLUMINATION_SETTINGS = [
    {"channel": "BF LED matrix full", "intensity": 50, "exposure_time": 100}
]


class OperatorAbortError(RuntimeError):
    """Raised when the operator intentionally stops the smoke test."""


@dataclass(frozen=True)
class SmokeCycle:
    incubator_slot: int
    sample_name: str
    well_plate_type: str
    microscope_id: str


def parse_slot_selection(raw_selection: str, available_slots: Iterable[int]) -> List[int]:
    available_slot_set = set(available_slots)
    tokens = [token.strip() for token in raw_selection.split(",") if token.strip()]
    if not tokens:
        raise ValueError("Please enter at least one incubator slot.")
    if len(tokens) > 5:
        raise ValueError("Please select at most 5 samples.")

    selected_slots = []
    for token in tokens:
        try:
            slot = int(token)
        except ValueError as exc:
            raise ValueError(f"Invalid slot value '{token}'. Use comma-separated integers.") from exc
        selected_slots.append(slot)

    if len(selected_slots) != len(set(selected_slots)):
        raise ValueError("Duplicate slots are not allowed.")

    invalid_slots = [slot for slot in selected_slots if slot not in available_slot_set]
    if invalid_slots:
        raise ValueError(f"Selected slot(s) are not currently available: {invalid_slots}")

    return selected_slots


def build_cycle_plan(selected_samples: Sequence[dict], microscopes: Sequence[str]) -> List[SmokeCycle]:
    cycles = []
    for sample in selected_samples:
        for microscope_id in microscopes:
            cycles.append(
                SmokeCycle(
                    incubator_slot=sample["incubator_slot"],
                    sample_name=sample["name"],
                    well_plate_type=sample.get("well_plate_type", "96"),
                    microscope_id=microscope_id,
                )
            )
    return cycles


def format_samples_table(samples: Sequence[dict]) -> str:
    headers = ["Slot", "Sample", "Location", "Status", "Plate"]
    rows = [
        [
            str(sample["incubator_slot"]),
            sample["name"],
            sample["location"],
            sample["status"] or "-",
            sample.get("well_plate_type", "96"),
        ]
        for sample in samples
    ]

    widths = [len(header) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    def format_row(row: Sequence[str]) -> str:
        return " | ".join(value.ljust(widths[index]) for index, value in enumerate(row))

    lines = [format_row(headers), "-+-".join("-" * width for width in widths)]
    lines.extend(format_row(row) for row in rows)
    return "\n".join(lines)


def build_smoke_scan_config(well_plate_type: str) -> dict:
    return {
        "saved_data_type": "raw_images_well_plate",
        "well_plate_type": well_plate_type,
        "wells_to_scan": ["A1"],
        "Nx": 2,
        "Ny": 2,
        "dx": 0.8,
        "dy": 0.8,
        "illumination_settings": DEFAULT_ILLUMINATION_SETTINGS,
        "do_contrast_autofocus": False,
        "do_reflection_af": False,
    }


class HardwareSmokeTestRunner:
    def __init__(
        self,
        orchestrator_service,
        *,
        input_fn: Callable[[str], str] = input,
        output_fn: Callable[[str], None] = print,
        report_root: Path = DEFAULT_REPORT_ROOT,
        now_fn: Callable[[], datetime] = datetime.now,
    ):
        self.orchestrator = orchestrator_service
        self.input_fn = input_fn
        self.output_fn = output_fn
        self.report_root = Path(report_root)
        self.now_fn = now_fn
        self.run_id = self.now_fn().strftime("%Y%m%d_%H%M%S")
        self.report_dir = self.report_root / self.run_id
        self.log_path = self.report_dir / "run.log"
        self.summary_path = self.report_dir / "summary.json"

    def _record(self, message: str) -> None:
        timestamped = f"[{self.now_fn().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        self.output_fn(timestamped)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(timestamped + "\n")

    def _confirm(self, prompt: str) -> bool:
        response = self.input_fn(f"{prompt} [y/N]: ").strip().lower()
        return response in {"y", "yes"}

    def _prompt_for_slots(self, available_slots: Sequence[int]) -> List[int]:
        while True:
            response = self.input_fn(
                "Select 1-5 incubator slots to test (comma-separated, for example 3,8,11): "
            )
            try:
                return parse_slot_selection(response, available_slots)
            except ValueError as exc:
                self._record(str(exc))

    async def _get_runtime_status(self) -> dict:
        status = await self.orchestrator.get_runtime_status()
        if not status.get("success", True):
            raise RuntimeError(status.get("message", "Failed to retrieve runtime status."))
        return status

    async def _preflight(self) -> tuple[dict, List[dict], List[str]]:
        runtime_status = await self._get_runtime_status()
        if runtime_status.get("active_operations"):
            raise RuntimeError("The orchestrator is busy. Wait for all active operations to finish.")
        if runtime_status.get("active_tasks"):
            raise RuntimeError(f"The orchestrator still has active tasks: {runtime_status['active_tasks']}")
        if runtime_status.get("in_critical_operation"):
            raise RuntimeError("The orchestrator is in a critical operation. Try again when it is idle.")

        connected_services = runtime_status.get("connected_services", {})
        if not connected_services.get("incubator"):
            raise RuntimeError("Incubator service is not connected.")
        if not connected_services.get("robotic_arm"):
            raise RuntimeError("Robotic arm service is not connected.")

        microscopes = runtime_status.get("configured_microscopes", [])
        if not microscopes:
            raise RuntimeError("No microscopes are configured in the orchestrator.")

        missing_microscopes = [
            microscope_id
            for microscope_id in microscopes
            if not connected_services.get("microscopes", {}).get(microscope_id)
        ]
        if missing_microscopes:
            raise RuntimeError(f"Configured microscopes are not connected: {missing_microscopes}")

        samples_response = await self.orchestrator.get_incubator_samples(only_available=True)
        if not samples_response.get("success", True):
            raise RuntimeError(samples_response.get("message", "Failed to query incubator samples."))

        available_samples = samples_response.get("samples", [])
        if not available_samples:
            raise RuntimeError("No incubator samples are currently available for smoke testing.")

        return runtime_status, available_samples, microscopes

    async def _safe_runtime_status(self) -> dict:
        try:
            return await self._get_runtime_status()
        except Exception as exc:
            return {"success": False, "message": f"Failed to retrieve runtime status after error: {exc}"}

    async def _verify_sample_returned(self, cycle: SmokeCycle) -> dict:
        response = await self.orchestrator.get_incubator_samples(slot=cycle.incubator_slot)
        if not response.get("success", True):
            raise RuntimeError(response.get("message", "Failed to verify incubator sample location."))
        samples = response.get("samples", [])
        if len(samples) != 1:
            raise RuntimeError(f"Expected one sample record for slot {cycle.incubator_slot}, got {len(samples)}.")
        sample = samples[0]
        if sample.get("location") != "incubator_slot":
            raise RuntimeError(
                f"Sample in slot {cycle.incubator_slot} returned to '{sample.get('location')}', not 'incubator_slot'."
            )
        return sample

    async def _offer_emergency_actions(self, cycle: SmokeCycle) -> List[dict]:
        actions_taken = []
        while True:
            choice = self.input_fn(
                "Emergency action? [enter=skip, cancel-scan, halt-robot]: "
            ).strip().lower()
            if choice in {"", "skip"}:
                return actions_taken
            if choice == "cancel-scan":
                response = await self.orchestrator.cancel_microscope_scan(microscope_id=cycle.microscope_id)
            elif choice == "halt-robot":
                response = await self.orchestrator.halt_robotic_arm()
            else:
                self._record("Unknown emergency action. Use 'cancel-scan', 'halt-robot', or press enter to skip.")
                continue

            actions_taken.append({"action": choice, "response": response})
            self._record(f"Emergency action '{choice}' response: {response}")

    async def _run_cycle(self, cycle: SmokeCycle) -> dict:
        action_id = f"hardware_smoke_{self.run_id}_slot{cycle.incubator_slot}_{cycle.microscope_id}"
        cycle_result = {
            "cycle": asdict(cycle),
            "action_id": action_id,
            "status": "running",
            "last_completed_step": None,
        }

        try:
            load_response = await self.orchestrator.load_plate_from_incubator_to_microscope(
                incubator_slot=cycle.incubator_slot,
                microscope_id=cycle.microscope_id,
            )
            cycle_result["load_response"] = load_response
            if not load_response.get("success"):
                raise RuntimeError(load_response.get("message", "Load failed."))
            cycle_result["last_completed_step"] = "load"

            scan_response = await self.orchestrator.scan_microscope_only(
                microscope_id=cycle.microscope_id,
                scan_config=build_smoke_scan_config(cycle.well_plate_type),
                action_id=action_id,
            )
            cycle_result["scan_response"] = scan_response
            if not scan_response.get("success"):
                raise RuntimeError(scan_response.get("message", "Scan failed."))
            cycle_result["last_completed_step"] = "scan"

            unload_response = await self.orchestrator.unload_plate_from_microscope(
                incubator_slot=cycle.incubator_slot,
                microscope_id=cycle.microscope_id,
            )
            cycle_result["unload_response"] = unload_response
            if not unload_response.get("success"):
                raise RuntimeError(unload_response.get("message", "Unload failed."))
            cycle_result["last_completed_step"] = "unload"

            verification = await self._verify_sample_returned(cycle)
            cycle_result["verification"] = verification
            cycle_result["last_completed_step"] = "verified"
            cycle_result["status"] = "completed"
            return cycle_result
        except Exception as exc:
            cycle_result["status"] = "failed"
            cycle_result["error"] = str(exc)
            cycle_result["runtime_status"] = await self._safe_runtime_status()
            return cycle_result

    def _write_summary(self, summary: dict) -> None:
        self.summary_path.parent.mkdir(parents=True, exist_ok=True)
        with self.summary_path.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2)

    async def run(self) -> dict:
        self.report_dir.mkdir(parents=True, exist_ok=True)
        summary = {
            "run_id": self.run_id,
            "started_at": self.now_fn().isoformat(timespec="seconds"),
            "status": "running",
            "cycles": [],
        }
        self._write_summary(summary)

        try:
            runtime_status, available_samples, microscopes = await self._preflight()
            self._record("Preflight passed. Available incubator samples:")
            samples_table = format_samples_table(available_samples)
            self.output_fn(samples_table)
            with self.log_path.open("a", encoding="utf-8") as handle:
                handle.write(samples_table + "\n")

            selected_slots = self._prompt_for_slots([sample["incubator_slot"] for sample in available_samples])
            sample_by_slot = {sample["incubator_slot"]: sample for sample in available_samples}
            selected_samples = [sample_by_slot[slot] for slot in selected_slots]
            cycles = build_cycle_plan(selected_samples, microscopes)

            self._record(f"Selected incubator slots: {selected_slots}")
            self._record(f"Target microscopes: {microscopes}")
            self._record(
                f"Planned cycles: {len(cycles)} ({len(selected_samples)} sample(s) x {len(microscopes)} microscope(s))"
            )

            if not self._confirm("Confirm you are in the lab and the robot/microscope motion path is clear"):
                raise OperatorAbortError("Operator did not acknowledge lab safety preconditions.")

            summary["selected_slots"] = selected_slots
            summary["configured_microscopes"] = microscopes
            summary["preflight_runtime_status"] = runtime_status

            for index, cycle in enumerate(cycles, start=1):
                if not self._confirm(
                    f"Start cycle {index}/{len(cycles)} for slot {cycle.incubator_slot} "
                    f"({cycle.sample_name}) on {cycle.microscope_id}"
                ):
                    raise OperatorAbortError(
                        f"Operator stopped before cycle {index} for slot {cycle.incubator_slot} on {cycle.microscope_id}."
                    )

                self._record(
                    f"Starting cycle {index}/{len(cycles)}: slot {cycle.incubator_slot}, "
                    f"sample '{cycle.sample_name}', microscope {cycle.microscope_id}"
                )
                cycle_result = await self._run_cycle(cycle)
                summary["cycles"].append(cycle_result)
                self._write_summary(summary)

                if cycle_result["status"] != "completed":
                    self._record(
                        "Cycle failed. "
                        f"slot={cycle.incubator_slot}, microscope={cycle.microscope_id}, "
                        f"action_id={cycle_result['action_id']}, "
                        f"last_completed_step={cycle_result.get('last_completed_step')}, "
                        f"error={cycle_result.get('error')}"
                    )
                    cycle_result["emergency_actions"] = await self._offer_emergency_actions(cycle)
                    summary["status"] = "failed"
                    summary["failure"] = cycle_result
                    self._write_summary(summary)
                    return summary

                self._record(
                    f"Cycle completed successfully: slot {cycle.incubator_slot}, microscope {cycle.microscope_id}, "
                    f"action_id={cycle_result['action_id']}"
                )

            summary["status"] = "completed"
            return summary
        except OperatorAbortError as exc:
            summary["status"] = "aborted"
            summary["message"] = str(exc)
            self._record(str(exc))
            return summary
        except Exception as exc:
            summary["status"] = "failed"
            summary["message"] = str(exc)
            self._record(f"Smoke test failed during preflight or setup: {exc}")
            return summary
        finally:
            summary["finished_at"] = self.now_fn().isoformat(timespec="seconds")
            summary["report_dir"] = str(self.report_dir)
            self._write_summary(summary)


async def connect_to_orchestrator(
    *,
    server_url: str = DEFAULT_LOCAL_SERVER_URL,
    workspace: str = None,
    token: str = None,
):
    workspace = workspace or os.environ.get("REEF_LOCAL_WORKSPACE")
    token = token or os.environ.get("REEF_LOCAL_TOKEN")
    if not workspace:
        raise RuntimeError("REEF_LOCAL_WORKSPACE is required to connect to the local orchestrator.")
    if not token:
        raise RuntimeError("REEF_LOCAL_TOKEN is required to connect to the local orchestrator.")

    server = await connect_to_server({
        "server_url": server_url,
        "workspace": workspace,
        "token": token,
        "ping_interval": 30,
    })
    orchestrator = await server.get_service("orchestrator-manager")
    return server, orchestrator


async def async_main() -> int:
    server, orchestrator = await connect_to_orchestrator()
    runner = HardwareSmokeTestRunner(orchestrator)
    try:
        summary = await runner.run()
        return 0 if summary["status"] == "completed" else 1
    finally:
        await server.disconnect()


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()
