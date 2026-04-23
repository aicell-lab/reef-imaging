"""Orchestrator for the reef-imaging project.

Task:
1. Load a plate from incubator to microscope
2. Scan the plate
3. Unload the plate from microscope to incubator
"""
import asyncio
import copy
from datetime import datetime
import json
import logging
import logging.handlers
import os
from uuid import uuid4

import dotenv
from hypha_rpc import connect_to_server

from reef_imaging.orchestration import (
    AdmissionRequest,
    OperationAdmissionController,
    ResourceBusyError,
)

logger = logging.getLogger(__name__)


class HamiltonBusyError(RuntimeError):
    """Raised when a Hamilton-related action is rejected because the executor is busy."""


class TransportPreconditionError(RuntimeError):
    """Raised when a transport request does not match the sample's verified source state."""


# Set up logging
def setup_logging(log_file="orchestrator.log", max_bytes=10*1024*1024, backup_count=5):
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if logger.handlers:
        return logger

    file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

dotenv.load_dotenv()
ENV_FILE = dotenv.find_dotenv()
if ENV_FILE:
    dotenv.load_dotenv(ENV_FILE)

MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE_PATH = os.path.join(MODULE_DIR, "config.json")
CONFIG_FILE_PATH_TMP = os.path.join(MODULE_DIR, "config.json.tmp")
CONFIG_READ_INTERVAL = 10
ORCHESTRATOR_LOOP_SLEEP = 5


class OrchestrationSystemBase:
    def __init__(self):
        self.server_url = "http://reef.dyn.scilifelab.se:9527"
        self.local_workspace = os.environ.get("REEF_LOCAL_WORKSPACE")
        self.local_token = os.environ.get("REEF_LOCAL_TOKEN")
        
        # Orchestrator's own Hypha service registration details
        self.orchestrator_hypha_server_url = "https://hypha.aicell.io"
        self.workspace = "reef-imaging" # Default workspace for aicell.io
        self.token_for_orchestrator_registration = os.environ.get("REEF_WORKSPACE_TOKEN")
        
        self.orchestrator_hypha_service_id = "orchestrator-manager"
        self.orchestrator_hypha_server_connection = None
        
        # Stable server connection to local Hypha server (kept alive throughout)
        self.local_server_connection = None
        
        self.incubator = None
        self.microscope_services = {} # microscope_id -> service object
        self.configured_microscopes_info = {} # microscope_id -> config dict from config.json
        self.robotic_arm = None
        self.hamilton_executor = None
        self.sample_on_microscope_flags = {} # microscope_id -> bool, True if sample on that microscope

        self.incubator_id = "incubator-control"
        self.robotic_arm_id = "robotic-arm-control"
        self.hamilton_executor_id = "hamilton-script-executor"

        self.tasks = {} # Stores task configurations and states
        self.health_check_tasks = {} # Stores asyncio tasks for health checks, keyed by (service_type, service_id)
        self.active_task_name = None # Compatibility field; mirrors the first running task if any
        self._active_task_names = set()
        self._scheduled_cycle_tasks = {} # task_name -> asyncio.Task
        self._config_lock = asyncio.Lock()
        self.admission_controller = OperationAdmissionController()

        # Critical operation tracking - True when robotic arm is moving or microscope is scanning
        self.in_critical_operation = False
        # Track exactly which services are in a critical section to avoid unrelated shutdowns
        self.critical_services = set()  # set of tuples: (service_type, service_identifier)

    def _mark_critical_services(self, service_types: list):
        """Mark services as being in a critical operation."""
        for service_type, service_id in service_types:
            self.critical_services.add((service_type, service_id))

    def _unmark_critical_services(self, service_types: list):
        """Unmark services from critical operation."""
        for service_type, service_id in service_types:
            self.critical_services.discard((service_type, service_id))

    def _refresh_legacy_active_task_name(self):
        """Keep the legacy single-task field aligned with the running task set."""
        self.active_task_name = sorted(self._active_task_names)[0] if self._active_task_names else None

    def _mark_task_running(self, task_name: str):
        self._active_task_names.add(task_name)
        self._refresh_legacy_active_task_name()

    def _mark_task_not_running(self, task_name: str):
        self._active_task_names.discard(task_name)
        self._refresh_legacy_active_task_name()

    def _new_operation_id(self, operation_type: str) -> str:
        return f"{operation_type}-{uuid4().hex[:12]}"

    def _task_resource(self, task_name: str) -> str:
        return f"task:{task_name}"

    def _microscope_resource(self, microscope_id: str) -> str:
        return f"microscope:{microscope_id}"

    def _slot_resource(self, incubator_slot: int) -> str:
        return f"incubator-slot:{incubator_slot}"

    def _hamilton_resource(self) -> str:
        return "hamilton"

    def _transport_resources(self) -> tuple[str, ...]:
        return ("transport-lane", "robotic-arm", "incubator")

    def _hamilton_transport_resources(self) -> tuple[str, ...]:
        return self._transport_resources() + (self._hamilton_resource(),)

    def _build_request(
        self,
        operation_type: str,
        *,
        task_name: str = None,
        microscope_id: str = None,
        incubator_slot: int = None,
        extra_resources: tuple[str, ...] = (),
        metadata: dict = None,
    ) -> AdmissionRequest:
        resources = []
        if task_name:
            resources.append(self._task_resource(task_name))
        if microscope_id:
            resources.append(self._microscope_resource(microscope_id))
        if incubator_slot is not None:
            resources.append(self._slot_resource(incubator_slot))
        resources.extend(extra_resources)

        unique_resources = tuple(dict.fromkeys(resources))
        return AdmissionRequest(
            operation_id=self._new_operation_id(operation_type),
            operation_type=operation_type,
            resources=unique_resources,
            microscope_id=microscope_id,
            incubator_slot=incubator_slot,
            task_name=task_name,
            metadata=metadata or {},
        )

    def _busy_response(self, message: str, busy_error: ResourceBusyError) -> dict:
        response = {
            "success": False,
            "message": message,
            "state": "busy",
            "blocked_by": [blocker.to_dict() for blocker in busy_error.blockers],
        }
        return response

    def _hamilton_busy_response(self, message: str, *, executor_status: dict = None) -> dict:
        response = {
            "success": False,
            "message": message,
            "state": "busy",
        }
        if executor_status is not None:
            response["executor_status"] = executor_status
        return response

    def _build_service_api(self) -> dict:
        return {
            "name": "Orchestrator Manager",
            "id": self.orchestrator_hypha_service_id,
            "config": {
                "visibility": "protected",
            },
            "ping": self.ping,
            # Task management
            "add_imaging_task": self.add_imaging_task,
            "delete_imaging_task": self.delete_imaging_task,
            "pause_imaging_task": self.pause_imaging_task,
            "resume_imaging_task": self.resume_imaging_task,
            "get_all_imaging_tasks": self.get_all_imaging_tasks,
            # Unified transport API
            "transport_plate": self.transport_plate_api,
            # Status and monitoring
            "get_runtime_status": self.get_runtime_status,
            "get_hamilton_status": self.get_hamilton_status,
            "get_lab_video_stream_urls": self.get_lab_video_stream_urls,
            # Emergency controls
            "cancel_microscope_scan": self.cancel_microscope_scan,
            "halt_robotic_arm": self.halt_robotic_arm,
            # Processing
            "process_timelapse_offline": self.process_timelapse_offline_api,
            "scan_microscope_only": self.scan_microscope_only_api,
            "run_hamilton_protocol": self.run_hamilton_protocol,
        }

    def _get_config_file_path(self) -> str:
        import reef_imaging.orchestrator as orchestrator_module

        return orchestrator_module.CONFIG_FILE_PATH

    def _get_config_file_path_tmp(self) -> str:
        import reef_imaging.orchestrator as orchestrator_module

        return orchestrator_module.CONFIG_FILE_PATH_TMP

    async def _ensure_local_server_connection(self):
        if not self.local_token:
            logger.error("REEF_LOCAL_TOKEN not set. Cannot setup local connection.")
            return None
        if not self.local_workspace:
            logger.error("REEF_LOCAL_WORKSPACE not set. Cannot setup local connection.")
            return None

        if not self.local_server_connection:
            logger.info(f"Creating stable connection to local Hypha server: {self.server_url}")
            self.local_server_connection = await connect_to_server({
                "server_url": self.server_url,
                "token": self.local_token,
                "workspace": self.local_workspace,
                "ping_interval": 30,
            })
            logger.info("Stable server connection established.")
        return self.local_server_connection

    async def _start_health_check(self, service_type, service_instance, service_identifier=None): # MODIFIED signature
        key = (service_type, service_identifier) if service_identifier else service_type
        if key in self.health_check_tasks and not self.health_check_tasks[key].done():
            logger.info(f"Health check for {service_type} ({service_identifier if service_identifier else ''}) already running.")
            return
        logger.info(f"Starting health check for {service_type} ({service_identifier if service_identifier else ''})...")
        logger.debug(f"Creating health check task with key: {key}")
        task = asyncio.create_task(self.check_service_health(service_instance, service_type, service_identifier)) # Pass identifier
        self.health_check_tasks[key] = task
        logger.debug(f"Health check task created and stored for {service_type} ({service_identifier if service_identifier else ''})")

    async def _stop_health_check(self, service_type, service_identifier=None): # MODIFIED signature
        key = (service_type, service_identifier) if service_identifier else service_type
        if key in self.health_check_tasks:
            task = self.health_check_tasks.pop(key)
            if task and not task.done():
                logger.info(f"Stopping health check for {service_type} ({service_identifier if service_identifier else ''})...")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.info(f"Health check for {service_type} ({service_identifier if service_identifier else ''}) cancelled.")

    async def _load_and_update_tasks(self):
        new_task_configs = {}
        raw_settings_by_task = {}
        raw_config_data = None
        config_file_path = self._get_config_file_path()

        async with self._config_lock:
            try:
                with open(config_file_path, 'r') as f:
                    raw_config_data = json.load(f)
            except FileNotFoundError:
                logger.error(f"Configuration file {config_file_path} not found.")
                raw_config_data = {"samples": []}
            except (json.JSONDecodeError, OSError) as e:
                logger.error(f"Error reading {config_file_path}: {e}. Will not update tasks from file this cycle.")
                # Add a small delay to prevent rapid retries on file system errors
                await asyncio.sleep(1)
                return

        for sample_config_from_file in raw_config_data.get("samples", []):
            task_name = sample_config_from_file.get("name")
            settings = sample_config_from_file.get("settings")

            if not task_name or not settings:
                logger.warning(f"Found a sample configuration without a name or settings in {config_file_path}. Skipping: {sample_config_from_file}")
                continue

            try:
                pending_datetimes = []
                for tp_str in settings.get("pending_time_points", []):
                    dt_obj = datetime.fromisoformat(tp_str) # Expects naive ISO string
                    pending_datetimes.append(dt_obj)
                pending_datetimes.sort() 

                imaged_datetimes = []
                for tp_str in settings.get("imaged_time_points", []):
                    dt_obj = datetime.fromisoformat(tp_str) # Expects naive ISO string
                    imaged_datetimes.append(dt_obj)
                imaged_datetimes.sort()
                
                # Determine flags based on actual datetime lists
                has_pending = bool(pending_datetimes)
                has_imaged = bool(imaged_datetimes)

                # These flags in settings are for what's WRITTEN to config, orchestrator uses internal datetime lists primarily.
                normalized_settings = copy.deepcopy(settings)
                normalized_settings["imaging_completed"] = not has_pending
                normalized_settings["imaging_started"] = has_imaged or (not has_pending and has_imaged) # Started if imaged, or completed & imaged
                raw_settings_by_task[task_name] = normalized_settings

                scan_mode = settings.get("scan_mode", "full_automation")
                saved_data_type = settings.get("saved_data_type", "raw_images_well_plate")
                
                parsed_settings_config = {
                    "name": task_name,
                    "scan_mode": scan_mode,
                    "saved_data_type": saved_data_type,
                    "allocated_microscope": settings.get("allocated_microscope", "microscope-squid-1"),
                    "scan_timeout_minutes": settings.get("scan_timeout_minutes", 120),
                    "illumination_settings": copy.deepcopy(settings["illumination_settings"]),
                    "do_contrast_autofocus": settings["do_contrast_autofocus"],
                    "do_reflection_af": settings["do_reflection_af"],
                    "pending_datetimes": pending_datetimes,
                    "imaged_datetimes": imaged_datetimes,
                    "imaging_started_flag": normalized_settings["imaging_started"],
                    "imaging_completed_flag": normalized_settings["imaging_completed"]
                }

                if scan_mode == "full_automation":
                    parsed_settings_config["incubator_slot"] = settings["incubator_slot"]

                if saved_data_type == "raw_images_well_plate":
                    parsed_settings_config.update({
                        "wells_to_scan": copy.deepcopy(settings["wells_to_scan"]),
                        "Nx": settings["Nx"],
                        "Ny": settings["Ny"],
                        "dx": settings.get("dx", 0.8),
                        "dy": settings.get("dy", 0.8),
                        "well_plate_type": settings.get("well_plate_type", "96"),
                    })
                    # Optional focus_map_points for raw_images_well_plate
                    if "focus_map_points" in settings:
                        parsed_settings_config["focus_map_points"] = copy.deepcopy(settings["focus_map_points"])
                else:
                    parsed_settings_config["positions"] = copy.deepcopy(settings.get("positions", []))
                    # Optional focus_map_points for raw_image_flexible
                    if "focus_map_points" in settings:
                        parsed_settings_config["focus_map_points"] = copy.deepcopy(settings["focus_map_points"])
                    # Optional move_for_autofocus for raw_image_flexible
                    if "move_for_autofocus" in settings:
                        parsed_settings_config["move_for_autofocus"] = settings["move_for_autofocus"]

                new_task_configs[task_name] = parsed_settings_config
            except KeyError as e:
                logger.error(f"Missing key {e} in configuration settings for sample {task_name}. Skipping.")
                continue
            except ValueError as e: # Catch errors from datetime.fromisoformat if string is not naive or malformed
                logger.error(f"Error parsing time strings (ensure they are naive local time) for sample {task_name}: {e}. Skipping.")
                continue

        tasks_to_remove = [name for name in self.tasks if name not in new_task_configs]
        for task_name in tasks_to_remove:
            logger.info(f"Task {task_name} removed from configuration. Deactivating.")
            if task_name in self._active_task_names:
                logger.warning(f"Running task {task_name} was removed from config while still active.")
                self._mark_task_not_running(task_name)
            del self.tasks[task_name]

        a_task_state_changed_for_write = False
        for task_name, current_settings_config in new_task_configs.items():
            raw_settings_for_task = raw_settings_by_task.get(task_name, {})
            operational_state_from_file = {}
            for sample_in_file in raw_config_data.get("samples", []):
                if sample_in_file.get("name") == task_name:
                    operational_state_from_file = sample_in_file.get("operational_state", {})
                    break
            
            persisted_status = operational_state_from_file.get("status", "pending")

            # Determine actual status based on current pending_datetimes
            current_actual_status = persisted_status
            if not current_settings_config["pending_datetimes"]:
                # Only set to completed if not currently uploading or paused
                if persisted_status not in ["uploading", "paused"]:
                    current_actual_status = "completed"
                else:
                    current_actual_status = persisted_status  # Keep uploading or paused status
            elif persisted_status == "completed" and current_settings_config["pending_datetimes"]:
                # If file said completed, but now there are pending points (e.g. user added them)
                current_actual_status = "pending" # Reset to pending
                logger.info(f"Task '{task_name}' was completed but now has pending points. Resetting status to pending.")
                a_task_state_changed_for_write = True

            if task_name not in self.tasks:
                logger.info(f"New task added: {task_name}")
                self.tasks[task_name] = {
                    "config": current_settings_config,
                    "status": current_actual_status,
                    "_raw_settings_from_input": copy.deepcopy(raw_settings_for_task)
                }
                a_task_state_changed_for_write = True # Status might have been determined above

            else: # Task already exists, update it
                existing_task_data = self.tasks[task_name]
                previous_config = existing_task_data["config"]
                previous_raw_settings = existing_task_data.get("_raw_settings_from_input", {})
                # Check for config changes that might warrant a state reset
                # Note: well_plate_type is not checked here as it's now read from incubator service
                config_changed_significantly = (
                    previous_config["pending_datetimes"] != current_settings_config["pending_datetimes"] or
                    previous_config["imaged_datetimes"] != current_settings_config["imaged_datetimes"] or
                    any(previous_config.get(k) != current_settings_config.get(k)
                        for k in ["incubator_slot", "allocated_microscope", "wells_to_scan", "Nx", "Ny"])
                )

                config_changed = previous_config != current_settings_config
                raw_settings_changed = previous_raw_settings != raw_settings_for_task
                if config_changed:
                    existing_task_data["config"] = current_settings_config
                if raw_settings_changed:
                    existing_task_data["_raw_settings_from_input"] = copy.deepcopy(raw_settings_for_task)
                if config_changed or raw_settings_changed:
                    a_task_state_changed_for_write = True

                if existing_task_data["status"] != current_actual_status:
                    logger.info(f"Task '{task_name}' status changing from '{existing_task_data['status']}' to '{current_actual_status}' due to config load/re-evaluation.")
                    existing_task_data["status"] = current_actual_status
                    a_task_state_changed_for_write = True

                if config_changed_significantly and existing_task_data["status"] not in ["pending", "completed", "paused"]:
                    if current_settings_config["pending_datetimes"]:
                        if existing_task_data["status"] != "pending":
                            logger.info(f"Task '{task_name}' had significant config changes while in status '{existing_task_data['status']}'. Resetting to pending as new points exist.")
                            existing_task_data["status"] = "pending"
                            a_task_state_changed_for_write = True
                    elif not existing_task_data["config"]["imaged_datetimes"]:
                        # No pending, no imaged, but config changed? Should be completed. Or if no pending but imaged.
                        if existing_task_data["status"] != "completed":
                            logger.info(f"Task '{task_name}' had significant config changes. No pending points. Marking completed.")
                            existing_task_data["status"] = "completed"
                            a_task_state_changed_for_write = True

            # Final status check: if a task somehow ends up with status != completed but no pending_datetimes, fix it.
            # But respect 'uploading' and 'paused' status - don't force it to completed
            task_state_dict = self.tasks[task_name]
            if not task_state_dict["config"]["pending_datetimes"] and task_state_dict["status"] not in ["completed", "uploading", "paused"]:
                logger.warning(f"Task '{task_name}' has status '{task_state_dict['status']}' but no pending time points. Forcing to 'completed'.")
                task_state_dict["status"] = "completed"
                a_task_state_changed_for_write = True

        # Load microscope configurations
        newly_configured_microscopes_info = {}
        for mic_config in raw_config_data.get("microscopes", []):
            mic_id = mic_config.get("id")
            if mic_id:
                newly_configured_microscopes_info[mic_id] = mic_config
                if mic_id not in self.sample_on_microscope_flags: # Initialize flag for new microscopes
                    self.sample_on_microscope_flags[mic_id] = False
            else:
                logger.warning(f"Found a microscope configuration without an ID in {config_file_path}. Skipping: {mic_config}")
        
        # Handle microscopes removed from config
        removed_microscope_ids = [mid for mid in self.configured_microscopes_info if mid not in newly_configured_microscopes_info]
        for mid in removed_microscope_ids:
            logger.info(f"Microscope {mid} removed from configuration. Will disconnect if connected.")
            # Actual disconnection will be handled by setup_connections or a dedicated cleanup if needed
            if mid in self.sample_on_microscope_flags:
                del self.sample_on_microscope_flags[mid]
            # Stop health check if running for this microscope
            await self._stop_health_check('microscope', mid)
            if mid in self.microscope_services:
                try:
                    await self.microscope_services[mid].disconnect()
                    logger.info(f"Disconnected removed microscope {mid}.")
                except (ConnectionError, OSError, AttributeError) as e:
                    logger.error(f"Error disconnecting removed microscope {mid}: {e}")
                del self.microscope_services[mid]

        self.configured_microscopes_info = newly_configured_microscopes_info
        
        if a_task_state_changed_for_write or tasks_to_remove:
            await self._write_tasks_to_config()

    async def _write_tasks_to_config(self):
        """Writes the current state of all tasks back to the configuration file."""

        output_config_data = {"samples": []}
        config_file_path = self._get_config_file_path()
        config_file_path_tmp = self._get_config_file_path_tmp()

        async with self._config_lock: 
            try:
                with open(config_file_path, 'r') as f_read:
                    existing_data = json.load(f_read)
                    for key, value in existing_data.items():
                        if key != "samples":
                            output_config_data[key] = value
            except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
                 logger.warning(f"Could not re-read {config_file_path} before writing: {e}. Will create/overwrite with current task data only.")

            for task_name, task_data_internal in self.tasks.items():
                settings_to_write = copy.deepcopy(task_data_internal.get("_raw_settings_from_input", {}))
                current_internal_config = task_data_internal["config"]

                # Ensure all critical fields from internal config are preserved
                # Always write scan_mode and saved_data_type (with defaults if missing)
                settings_to_write["scan_mode"] = current_internal_config.get("scan_mode", "full_automation")
                settings_to_write["saved_data_type"] = current_internal_config.get("saved_data_type", "raw_images_well_plate")
                
                critical_fields = [
                    "incubator_slot", "allocated_microscope", 
                    "wells_to_scan", "Nx", "Ny", "dx", "dy", "well_plate_type", "positions",
                    "illumination_settings", "do_contrast_autofocus", "do_reflection_af",
                    "focus_map_points", "move_for_autofocus"
                ]
                for field in critical_fields:
                    if field in current_internal_config:
                        settings_to_write[field] = copy.deepcopy(current_internal_config[field])

                settings_to_write["pending_time_points"] = sorted([
                    dt.strftime('%Y-%m-%dT%H:%M:%S') for dt in current_internal_config.get("pending_datetimes", [])
                ])
                settings_to_write["imaged_time_points"] = sorted([
                    dt.strftime('%Y-%m-%dT%H:%M:%S') for dt in current_internal_config.get("imaged_datetimes", [])
                ])

                has_pending = bool(current_internal_config.get("pending_datetimes"))
                has_imaged = bool(current_internal_config.get("imaged_datetimes"))
                
                # Update these flags based on the current truth (pending/imaged datetimes)
                settings_to_write["imaging_completed"] = not has_pending
                settings_to_write["imaging_started"] = has_imaged or (not has_pending and has_imaged)
                
                sample_entry = {
                    "name": task_name,
                    "settings": settings_to_write, 
                    "operational_state": {
                        "status": task_data_internal["status"],
                        "last_updated_by_orchestrator": datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                    }
                }
                output_config_data["samples"].append(sample_entry)
            
            try:
                with open(config_file_path_tmp, 'w') as f_write:
                    json.dump(output_config_data, f_write, indent=4)
                os.replace(config_file_path_tmp, config_file_path)
            except (IOError, OSError) as e:
                logger.error(f"Error writing tasks state to {config_file_path}: {e}")
                
    async def _update_task_state_and_write_config(self, task_name, status=None, current_tp_to_move_to_imaged: datetime = None):
        """Helper to update task state (including time points) and write to config."""
        if task_name not in self.tasks:
            logger.warning(f"_update_task_state_and_write_config: Task {task_name} not found.")
            return

        changed = False
        task_state = self.tasks[task_name]
        task_config_internal = task_state["config"]

        if status and task_state["status"] != status:
            logger.info(f"Task '{task_name}' status changing from '{task_state['status']}' to '{status}'")
            task_state["status"] = status
            changed = True
        
        if current_tp_to_move_to_imaged:
            if current_tp_to_move_to_imaged in task_config_internal["pending_datetimes"]:
                task_config_internal["pending_datetimes"].remove(current_tp_to_move_to_imaged)
                task_config_internal["imaged_datetimes"].append(current_tp_to_move_to_imaged)
                task_config_internal["imaged_datetimes"].sort() 
                logger.info(f"Moved time point {current_tp_to_move_to_imaged.isoformat()} to imaged for task '{task_name}'.")
                changed = True
            else:
                logger.warning(f"Time point {current_tp_to_move_to_imaged.isoformat()} not found in pending_datetimes for task '{task_name}'. Cannot move.")

        # Update status based on pending points (but respect explicit status like "uploading")
        if not task_config_internal["pending_datetimes"]: 
            if task_state["status"] not in ["completed", "uploading"]:
                logger.info(f"Task '{task_name}' has no more pending time points. Marking as completed.")
                task_state["status"] = "completed"
                changed = True
            elif task_state["status"] == "uploading":
                logger.info(f"Task '{task_name}' is uploading and has no pending time points. Keeping uploading status.")
        elif status == "completed" and task_config_internal["pending_datetimes"]:
            logger.warning(f"Task '{task_name}' set to completed, but still has pending points. Reverting to pending.")
            task_state["status"] = "pending"
            changed = True

        if changed:
            await self._write_tasks_to_config()
