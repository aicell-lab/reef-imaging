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


@dataclass(frozen=True)
class HamiltonCycle:
    incubator_slot: int
    sample_name: str
    cycle_type: str  # 'incubator_to_hamilton', 'hamilton_to_incubator', 'microscope_to_hamilton', 'hamilton_to_microscope'
    microscope_id: str = None  # Only used for microscope-related cycles


@dataclass(frozen=True)
class TransportCycle:
    """A transport-only cycle without scanning."""
    incubator_slot: int
    sample_name: str
    from_device: str  # Source device ID
    to_device: str    # Target device ID


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


def build_hamilton_cycle_plan(selected_samples: Sequence[dict], test_type: str, microscopes: Sequence[str] = None) -> List[HamiltonCycle]:
    """Build Hamilton test cycles based on test type.
    
    test_type: 'incubator_only' - test incubator <-> Hamilton
               'microscope_only' - test microscope <-> Hamilton (uses first microscope)
               'full' - test incubator -> Hamilton -> microscope -> Hamilton -> incubator
    """
    cycles = []
    for sample in selected_samples:
        if test_type == "incubator_only":
            # Test incubator <-> Hamilton round trip
            cycles.append(HamiltonCycle(
                incubator_slot=sample["incubator_slot"],
                sample_name=sample["name"],
                cycle_type="incubator_to_hamilton"
            ))
            cycles.append(HamiltonCycle(
                incubator_slot=sample["incubator_slot"],
                sample_name=sample["name"],
                cycle_type="hamilton_to_incubator"
            ))
        elif test_type == "microscope_only" and microscopes:
            # Test microscope <-> Hamilton (use first microscope)
            microscope_id = microscopes[0]
            cycles.append(HamiltonCycle(
                incubator_slot=sample["incubator_slot"],
                sample_name=sample["name"],
                cycle_type="hamilton_to_microscope",
                microscope_id=microscope_id
            ))
            cycles.append(HamiltonCycle(
                incubator_slot=sample["incubator_slot"],
                sample_name=sample["name"],
                cycle_type="microscope_to_hamilton",
                microscope_id=microscope_id
            ))
        elif test_type == "full" and microscopes:
            # Full cycle: incubator -> Hamilton -> microscope -> Hamilton -> incubator
            microscope_id = microscopes[0]
            cycles.append(HamiltonCycle(
                incubator_slot=sample["incubator_slot"],
                sample_name=sample["name"],
                cycle_type="incubator_to_hamilton"
            ))
            cycles.append(HamiltonCycle(
                incubator_slot=sample["incubator_slot"],
                sample_name=sample["name"],
                cycle_type="hamilton_to_microscope",
                microscope_id=microscope_id
            ))
            cycles.append(HamiltonCycle(
                incubator_slot=sample["incubator_slot"],
                sample_name=sample["name"],
                cycle_type="microscope_to_hamilton",
                microscope_id=microscope_id
            ))
            cycles.append(HamiltonCycle(
                incubator_slot=sample["incubator_slot"],
                sample_name=sample["name"],
                cycle_type="hamilton_to_incubator"
            ))
    return cycles


def build_transport_cycle_plan(selected_samples: Sequence[dict], microscopes: Sequence[str]) -> List[TransportCycle]:
    """Build transport-only test cycles as a single chain through all devices.
    
    Tests transport by moving a plate through all devices in sequence:
    incubator -> microscope-squid-1 -> microscope-squid-2 -> microscope-squid-plus-3 -> hamilton -> incubator
    
    This ensures the plate is always at the expected location for each transport operation.
    """
    cycles = []
    for sample in selected_samples:
        slot = sample["incubator_slot"]
        name = sample["name"]
        
        # Build a single chain through all devices
        # Start from incubator
        current_location = "incubator"
        
        # Go to each microscope in order
        for microscope_id in microscopes:
            cycles.append(TransportCycle(
                incubator_slot=slot,
                sample_name=name,
                from_device=current_location,
                to_device=microscope_id
            ))
            current_location = microscope_id
        
        # Finally go to hamilton and back to incubator
        cycles.append(TransportCycle(
            incubator_slot=slot,
            sample_name=name,
            from_device=current_location,
            to_device="hamilton"
        ))
        cycles.append(TransportCycle(
            incubator_slot=slot,
            sample_name=name,
            from_device="hamilton",
            to_device="incubator"
        ))
    
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

    def _prompt_for_test_mode(self) -> str:
        """Ask user to select test mode."""
        self._record("Select test mode:")
        self._record("  1. Microscope only (default) - test incubator <-> microscope transport and scanning")
        self._record("  2. Hamilton only (incubator) - test incubator <-> Hamilton transport")
        self._record("  3. Hamilton only (microscope) - test microscope <-> Hamilton transport")
        self._record("  4. Hamilton full cycle - test incubator -> Hamilton -> microscope -> Hamilton -> incubator")
        self._record("  5. Combined - test microscope first, then Hamilton")
        self._record("  6. Transportation only - test transport chain through all devices without scanning")
        while True:
            response = self.input_fn("Enter choice [1-6, default=1]: ").strip()
            if response in {"", "1"}:
                return "microscope_only"
            elif response == "2":
                return "hamilton_incubator"
            elif response == "3":
                return "hamilton_microscope"
            elif response == "4":
                return "hamilton_full"
            elif response == "5":
                return "combined"
            elif response == "6":
                return "transportation_only"
            else:
                self._record("Invalid choice. Please enter 1-6.")

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

    async def _offer_emergency_actions(self, cycle: SmokeCycle = None, hamilton_cycle: HamiltonCycle = None) -> List[dict]:
        actions_taken = []
        while True:
            choice = self.input_fn(
                "Emergency action? [enter=skip, cancel-scan, halt-robot]: "
            ).strip().lower()
            if choice in {"", "skip"}:
                return actions_taken
            if choice == "cancel-scan":
                microscope_id = cycle.microscope_id if cycle else (hamilton_cycle.microscope_id if hamilton_cycle else None)
                if microscope_id:
                    response = await self.orchestrator.cancel_microscope_scan(microscope_id=microscope_id)
                else:
                    self._record("Cannot cancel scan: no microscope specified.")
                    continue
            elif choice == "halt-robot":
                response = await self.orchestrator.halt_robotic_arm()
            else:
                self._record("Unknown emergency action. Use 'cancel-scan', 'halt-robot', or press enter to skip.")
                continue

            actions_taken.append({"action": choice, "response": response})
            self._record(f"Emergency action '{choice}' response: {response}")

    async def _run_cycle(self, cycle: SmokeCycle) -> dict:
        """Run a full smoke cycle with scanning (legacy mode)."""
        action_id = f"hardware_smoke_{self.run_id}_slot{cycle.incubator_slot}_{cycle.microscope_id}"
        cycle_result = {
            "cycle": asdict(cycle),
            "action_id": action_id,
            "status": "running",
            "last_completed_step": None,
        }

        try:
            # Use unified transport_plate API for load
            load_response = await self.orchestrator.transport_plate(
                from_device="incubator",
                to_device=cycle.microscope_id,
                slot=cycle.incubator_slot,
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

            # Use unified transport_plate API for unload
            unload_response = await self.orchestrator.transport_plate(
                from_device=cycle.microscope_id,
                to_device="incubator",
                slot=cycle.incubator_slot,
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

    async def _verify_sample_at_hamilton(self, cycle: HamiltonCycle) -> dict:
        response = await self.orchestrator.get_incubator_samples(slot=cycle.incubator_slot)
        if not response.get("success", True):
            raise RuntimeError(response.get("message", "Failed to verify sample location."))
        samples = response.get("samples", [])
        if len(samples) != 1:
            raise RuntimeError(f"Expected one sample record for slot {cycle.incubator_slot}, got {len(samples)}.")
        sample = samples[0]
        if sample.get("location") != "hamilton":
            raise RuntimeError(
                f"Sample in slot {cycle.incubator_slot} is at '{sample.get('location')}', not 'hamilton'."
            )
        return sample

    async def _run_hamilton_cycle(self, cycle: HamiltonCycle) -> dict:
        """Run a Hamilton transport cycle (legacy mode using unified API)."""
        action_id = f"hardware_smoke_{self.run_id}_slot{cycle.incubator_slot}_{cycle.cycle_type}"
        cycle_result = {
            "cycle": asdict(cycle),
            "action_id": action_id,
            "status": "running",
            "last_completed_step": None,
        }

        try:
            # Map legacy cycle types to unified transport_plate API
            route_map = {
                "incubator_to_hamilton": ("incubator", "hamilton"),
                "hamilton_to_incubator": ("hamilton", "incubator"),
                "microscope_to_hamilton": (cycle.microscope_id, "hamilton"),
                "hamilton_to_microscope": ("hamilton", cycle.microscope_id),
            }
            
            from_device, to_device = route_map[cycle.cycle_type]
            response = await self.orchestrator.transport_plate(
                from_device=from_device,
                to_device=to_device,
                slot=cycle.incubator_slot,
            )
            cycle_result["transport_response"] = response
            
            if not response.get("success"):
                raise RuntimeError(response.get("message", f"Transport {cycle.cycle_type} failed."))
            
            # Verify based on target
            if cycle.cycle_type in ("incubator_to_hamilton", "microscope_to_hamilton"):
                verification = await self._verify_sample_at_hamilton(cycle)
                cycle_result["verification"] = verification
            elif cycle.cycle_type == "hamilton_to_incubator":
                verification = await self._verify_sample_returned(cycle)
                cycle_result["verification"] = verification
            elif cycle.cycle_type == "hamilton_to_microscope":
                runtime_status = await self._get_runtime_status()
                sample_flags = runtime_status.get("sample_on_microscope_flags", {})
                if not sample_flags.get(cycle.microscope_id, False):
                    raise RuntimeError(f"Sample not detected on microscope {cycle.microscope_id} after transport.")
            
            cycle_result["last_completed_step"] = cycle.cycle_type
            cycle_result["status"] = "completed"
            return cycle_result
        except Exception as exc:
            cycle_result["status"] = "failed"
            cycle_result["error"] = str(exc)
            cycle_result["runtime_status"] = await self._safe_runtime_status()
            return cycle_result

    async def _run_hamilton_cycles(self, cycles: List[HamiltonCycle], summary: dict) -> dict:
        """Run Hamilton test cycles."""
        for index, cycle in enumerate(cycles, start=1):
            microscope_info = f" -> {cycle.microscope_id}" if cycle.microscope_id else ""
            if not self._confirm(
                f"Start Hamilton cycle {index}/{len(cycles)}: {cycle.cycle_type} "
                f"for slot {cycle.incubator_slot} ({cycle.sample_name}){microscope_info}"
            ):
                raise OperatorAbortError(
                    f"Operator stopped before Hamilton cycle {index} for slot {cycle.incubator_slot}."
                )

            self._record(
                f"Starting Hamilton cycle {index}/{len(cycles)}: {cycle.cycle_type}, "
                f"slot {cycle.incubator_slot}, sample '{cycle.sample_name}'{microscope_info}"
            )
            cycle_result = await self._run_hamilton_cycle(cycle)
            summary["cycles"].append(cycle_result)
            self._write_summary(summary)

            if cycle_result["status"] != "completed":
                self._record(
                    "Hamilton cycle failed. "
                    f"slot={cycle.incubator_slot}, type={cycle.cycle_type}, "
                    f"action_id={cycle_result['action_id']}, "
                    f"last_completed_step={cycle_result.get('last_completed_step')}, "
                    f"error={cycle_result.get('error')}"
                )
                cycle_result["emergency_actions"] = await self._offer_emergency_actions(hamilton_cycle=cycle)
                summary["status"] = "failed"
                summary["failure"] = cycle_result
                self._write_summary(summary)
                return summary

            self._record(
                f"Hamilton cycle completed successfully: {cycle.cycle_type}, slot {cycle.incubator_slot}, "
                f"action_id={cycle_result['action_id']}"
            )
        return summary

    async def _run_transport_cycle(self, cycle: TransportCycle) -> dict:
        """Run a transport-only cycle without scanning."""
        action_id = f"transport_test_{self.run_id}_slot{cycle.incubator_slot}_{cycle.from_device}_to_{cycle.to_device}"
        cycle_result = {
            "cycle": asdict(cycle),
            "action_id": action_id,
            "status": "running",
            "last_completed_step": None,
        }

        try:
            response = await self.orchestrator.transport_plate(
                from_device=cycle.from_device,
                to_device=cycle.to_device,
                slot=cycle.incubator_slot,
            )
            cycle_result["transport_response"] = response
            
            if not response.get("success"):
                raise RuntimeError(response.get("message", f"Transport {cycle.from_device} -> {cycle.to_device} failed."))
            
            # Verify based on target device
            if cycle.to_device == "incubator":
                verification = await self._verify_sample_returned(cycle)
                cycle_result["verification"] = verification
            elif cycle.to_device == "hamilton":
                # Create a temporary HamiltonCycle for verification
                hamilton_cycle = HamiltonCycle(
                    incubator_slot=cycle.incubator_slot,
                    sample_name=cycle.sample_name,
                    cycle_type="microscope_to_hamilton" if cycle.from_device.startswith("microscope") else "incubator_to_hamilton"
                )
                verification = await self._verify_sample_at_hamilton(hamilton_cycle)
                cycle_result["verification"] = verification
            elif cycle.to_device.startswith("microscope"):
                runtime_status = await self._get_runtime_status()
                sample_flags = runtime_status.get("sample_on_microscope_flags", {})
                if not sample_flags.get(cycle.to_device, False):
                    raise RuntimeError(f"Sample not detected on microscope {cycle.to_device} after transport.")
            
            cycle_result["last_completed_step"] = f"{cycle.from_device}_to_{cycle.to_device}"
            cycle_result["status"] = "completed"
            return cycle_result
        except Exception as exc:
            cycle_result["status"] = "failed"
            cycle_result["error"] = str(exc)
            cycle_result["runtime_status"] = await self._safe_runtime_status()
            return cycle_result

    async def _run_transport_cycles(self, cycles: List[TransportCycle], summary: dict) -> dict:
        """Run transport-only test cycles."""
        for index, cycle in enumerate(cycles, start=1):
            if not self._confirm(
                f"Start transport cycle {index}/{len(cycles)}: {cycle.from_device} -> {cycle.to_device} "
                f"for slot {cycle.incubator_slot} ({cycle.sample_name})"
            ):
                raise OperatorAbortError(
                    f"Operator stopped before transport cycle {index} for slot {cycle.incubator_slot}."
                )

            self._record(
                f"Starting transport cycle {index}/{len(cycles)}: {cycle.from_device} -> {cycle.to_device}, "
                f"slot {cycle.incubator_slot}, sample '{cycle.sample_name}'"
            )
            cycle_result = await self._run_transport_cycle(cycle)
            summary["cycles"].append(cycle_result)
            self._write_summary(summary)

            if cycle_result["status"] != "completed":
                self._record(
                    "Transport cycle failed. "
                    f"slot={cycle.incubator_slot}, route={cycle.from_device} -> {cycle.to_device}, "
                    f"action_id={cycle_result['action_id']}, "
                    f"last_completed_step={cycle_result.get('last_completed_step')}, "
                    f"error={cycle_result.get('error')}"
                )
                summary["status"] = "failed"
                summary["failure"] = cycle_result
                self._write_summary(summary)
                return summary

            self._record(
                f"Transport cycle completed successfully: {cycle.from_device} -> {cycle.to_device}, "
                f"slot {cycle.incubator_slot}, action_id={cycle_result['action_id']}"
            )
        return summary

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

            test_mode = self._prompt_for_test_mode()
            summary["test_mode"] = test_mode

            selected_slots = self._prompt_for_slots([sample["incubator_slot"] for sample in available_samples])
            sample_by_slot = {sample["incubator_slot"]: sample for sample in available_samples}
            selected_samples = [sample_by_slot[slot] for slot in selected_slots]

            self._record(f"Selected incubator slots: {selected_slots}")
            self._record(f"Test mode: {test_mode}")

            if not self._confirm("Confirm you are in the lab and the robot/microscope motion path is clear"):
                raise OperatorAbortError("Operator did not acknowledge lab safety preconditions.")

            summary["selected_slots"] = selected_slots
            summary["configured_microscopes"] = microscopes
            summary["preflight_runtime_status"] = runtime_status

            if test_mode == "microscope_only":
                cycles = build_cycle_plan(selected_samples, microscopes)
                self._record(f"Target microscopes: {microscopes}")
                self._record(
                    f"Planned cycles: {len(cycles)} ({len(selected_samples)} sample(s) x {len(microscopes)} microscope(s))"
                )

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
                        cycle_result["emergency_actions"] = await self._offer_emergency_actions(cycle=cycle)
                        summary["status"] = "failed"
                        summary["failure"] = cycle_result
                        self._write_summary(summary)
                        return summary

                    self._record(
                        f"Cycle completed successfully: slot {cycle.incubator_slot}, microscope {cycle.microscope_id}, "
                        f"action_id={cycle_result['action_id']}"
                    )

            elif test_mode in ("hamilton_incubator", "hamilton_microscope", "hamilton_full"):
                hamilton_test_type = {
                    "hamilton_incubator": "incubator_only",
                    "hamilton_microscope": "microscope_only",
                    "hamilton_full": "full"
                }[test_mode]
                hamilton_cycles = build_hamilton_cycle_plan(selected_samples, hamilton_test_type, microscopes)
                self._record(f"Planned Hamilton cycles: {len(hamilton_cycles)}")

                summary = await self._run_hamilton_cycles(hamilton_cycles, summary)
                if summary.get("status") == "failed":
                    return summary

            elif test_mode == "combined":
                # First run microscope tests
                cycles = build_cycle_plan(selected_samples, microscopes)
                self._record(f"Target microscopes: {microscopes}")
                self._record(
                    f"Planned microscope cycles: {len(cycles)} ({len(selected_samples)} sample(s) x {len(microscopes)} microscope(s))"
                )

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
                        cycle_result["emergency_actions"] = await self._offer_emergency_actions(cycle=cycle)
                        summary["status"] = "failed"
                        summary["failure"] = cycle_result
                        self._write_summary(summary)
                        return summary

                    self._record(
                        f"Cycle completed successfully: slot {cycle.incubator_slot}, microscope {cycle.microscope_id}, "
                        f"action_id={cycle_result['action_id']}"
                    )

                # Then run Hamilton full cycle
                if self._confirm("Microscope tests completed. Continue with Hamilton full cycle test?"):
                    hamilton_cycles = build_hamilton_cycle_plan(selected_samples, "full", microscopes)
                    self._record(f"Planned Hamilton cycles: {len(hamilton_cycles)}")
                    summary = await self._run_hamilton_cycles(hamilton_cycles, summary)
                    if summary.get("status") == "failed":
                        return summary

            elif test_mode == "transportation_only":
                # Run transport chain through all devices without scanning
                transport_cycles = build_transport_cycle_plan(selected_samples, microscopes)
                self._record(f"Target devices: incubator, hamilton, {', '.join(microscopes)}")
                self._record(
                    f"Planned transport cycles: {len(transport_cycles)} (single chain through all devices)"
                )
                self._record("Transport chain:")
                for i, cycle in enumerate(transport_cycles, 1):
                    self._record(f"  {i}. {cycle.from_device} -> {cycle.to_device} (slot {cycle.incubator_slot})")
                
                summary = await self._run_transport_cycles(transport_cycles, summary)
                if summary.get("status") == "failed":
                    return summary

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
