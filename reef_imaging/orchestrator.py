"""
This code is the orchestrator for the reef-imaging project.
Task: 
1. Load a plate from incubator to microscope
2. Scan the plate
3. Unload the plate from microscope to incubator
"""
import asyncio
from hypha_rpc import connect_to_server
from hypha_rpc.utils.schema import schema_function
import os
import dotenv
import logging
import sys
import logging.handlers
from datetime import datetime
import json
import copy
from uuid import uuid4

from reef_imaging.orchestration import (
    AdmissionRequest,
    OperationAdmissionController,
    ResourceBusyError,
)

logger = logging.getLogger(__name__)


class HamiltonBusyError(RuntimeError):
    """Raised when a Hamilton-related action is rejected because the executor is busy."""

# Set up logging
def setup_logging(log_file="orchestrator.log", max_bytes=10*1024*1024, backup_count=5):
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if logger.handlers:
        return logger

    # Rotating file handler - this will automatically rotate between orchestrator.log, orchestrator.log.1, orchestrator.log.2, etc.
    file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

dotenv.load_dotenv()
ENV_FILE = dotenv.find_dotenv()
if ENV_FILE:
    dotenv.load_dotenv(ENV_FILE)

# Get the directory where this module is located
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE_PATH = os.path.join(MODULE_DIR, "config.json")
CONFIG_FILE_PATH_TMP = os.path.join(MODULE_DIR, "config.json.tmp")
CONFIG_READ_INTERVAL = 10 # Seconds to wait before re-reading config.json
ORCHESTRATOR_LOOP_SLEEP = 5 # Seconds to sleep in main loop when no immediate task is due

class OrchestrationSystem:
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
            try:
                self.critical_services.add((service_type, service_id))
            except Exception:
                pass

    def _unmark_critical_services(self, service_types: list):
        """Unmark services from critical operation."""
        for service_type, service_id in service_types:
            try:
                self.critical_services.discard((service_type, service_id))
            except Exception:
                pass

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

        async with self._config_lock:
            try:
                with open(CONFIG_FILE_PATH, 'r') as f:
                    raw_config_data = json.load(f)
            except FileNotFoundError:
                logger.error(f"Configuration file {CONFIG_FILE_PATH} not found.")
                raw_config_data = {"samples": []}
            except (json.JSONDecodeError, OSError) as e:
                logger.error(f"Error reading {CONFIG_FILE_PATH}: {e}. Will not update tasks from file this cycle.")
                # Add a small delay to prevent rapid retries on file system errors
                await asyncio.sleep(1)
                return

        for sample_config_from_file in raw_config_data.get("samples", []):
            task_name = sample_config_from_file.get("name")
            settings = sample_config_from_file.get("settings")

            if not task_name or not settings:
                logger.warning(f"Found a sample configuration without a name or settings in {CONFIG_FILE_PATH}. Skipping: {sample_config_from_file}")
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
                logger.warning(f"Found a microscope configuration without an ID in {CONFIG_FILE_PATH}. Skipping: {mic_config}")
        
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
                except Exception as e:
                    logger.error(f"Error disconnecting removed microscope {mid}: {e}")
                del self.microscope_services[mid]

        self.configured_microscopes_info = newly_configured_microscopes_info
        
        if a_task_state_changed_for_write or tasks_to_remove:
            await self._write_tasks_to_config()

    async def _write_tasks_to_config(self):
        """Writes the current state of all tasks back to the configuration file."""
        
        output_config_data = {"samples": []}
        
        async with self._config_lock: 
            try:
                with open(CONFIG_FILE_PATH, 'r') as f_read:
                    existing_data = json.load(f_read)
                    for key, value in existing_data.items():
                        if key != "samples":
                            output_config_data[key] = value
            except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
                 logger.warning(f"Could not re-read {CONFIG_FILE_PATH} before writing: {e}. Will create/overwrite with current task data only.")

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
                with open(CONFIG_FILE_PATH_TMP, 'w') as f_write:
                    json.dump(output_config_data, f_write, indent=4)
                os.replace(CONFIG_FILE_PATH_TMP, CONFIG_FILE_PATH)
            except (IOError, OSError) as e:
                logger.error(f"Error writing tasks state to {CONFIG_FILE_PATH}: {e}")
                
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

    async def check_service_health(self, service, service_type, service_identifier=None):
        """Check if the service is healthy with smart failure handling:
        - During critical operations (robotic arm moving, scanning): Retry 10 times then EXIT program
        - When idle and server connection died: Attempt full reconnection (no exit)
        - When idle and microscope offline: Drop microscope from active services and stop health check
        - When idle and incubator/arm unreachable: Log and keep retrying (no exit)
        Note: We keep the server connection stable and only refresh the service reference when idle."""
        log_service_name_part = service_identifier if service_identifier else (service.id if hasattr(service, "id") else service_type)
        service_name = f"{service_type} ({log_service_name_part})"

        logger.info(f"Health check loop started for {service_name}")
        consecutive_failures = 0
        max_failures = 10

        while True:
            try:
                # Set a timeout for the ping operation
                ping_result = await asyncio.wait_for(service.ping(), timeout=5)

                if ping_result != "pong":
                    logger.error(f"{service_name} service ping check failed: {ping_result}")
                    raise Exception("Service not healthy")

                # Service is healthy - reset failure counter
                if consecutive_failures > 0:
                    logger.info(f"{service_name} service recovered after {consecutive_failures} failures.")
                consecutive_failures = 0
                logger.debug(f"{service_name} service health check passed.")

            except (asyncio.TimeoutError, Exception) as e:
                consecutive_failures += 1

                if isinstance(e, asyncio.TimeoutError):
                    logger.warning(f"{service_name} service ping timed out (failure {consecutive_failures}/{max_failures}).")
                else:
                    logger.warning(f"{service_name} service health check failed (failure {consecutive_failures}/{max_failures}): {e}")

                # Check if this specific service is in a critical operation
                is_critical_for_service = False
                if service_identifier is not None:
                    is_critical_for_service = (service_type, service_identifier) in self.critical_services
                else:
                    is_critical_for_service = (service_type, None) in self.critical_services

                if is_critical_for_service:
                    logger.warning(f"{service_name} health check failed during CRITICAL OPERATION (robotic arm moving or scanning).")

                    if consecutive_failures >= max_failures:
                        error_msg = f"{service_name} failed {max_failures} times during critical operation. Exiting program for safety."
                        logger.critical(error_msg)
                        logger.critical("Orchestrator will exit. Monitoring system will alert and restart.")
                        sys.exit(1)

                    retry_delay = 10 * consecutive_failures  # 10s, 20s, 30s...
                    logger.warning(f"Will retry {service_name} health check in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    continue

                else:
                    # Not in critical operation - safe to attempt refresh
                    logger.info(f"{service_name} health check failed while IDLE. Attempting service proxy refresh.")

                    try:
                        await self._refresh_service_proxy(service_type, service_identifier)
                        logger.info(f"{service_name} service proxy refreshed successfully.")

                        # Update the local service variable to the refreshed proxy
                        if service_type == 'incubator':
                            service = self.incubator
                        elif service_type == 'microscope' and service_identifier:
                            service = self.microscope_services.get(service_identifier)
                        elif service_type == 'robotic_arm':
                            service = self.robotic_arm
                        elif service_type == 'hamilton':
                            service = self.hamilton_executor

                        if service is None:
                            logger.error(f"Failed to get refreshed {service_name} service reference. Will retry.")
                            await asyncio.sleep(30)
                            continue

                        consecutive_failures = 0
                        await asyncio.sleep(30)
                        continue

                    except ConnectionError as conn_err:
                        # The local server connection itself is dead — attempt full reconnect
                        logger.warning(f"{service_name}: server connection dead ({conn_err}). Attempting full reconnect...")
                        try:
                            await self._reset_and_reconnect_local_server()
                            logger.info("Full reconnect succeeded. Resetting failure counter.")
                            # Refresh our local service reference after reconnect
                            if service_type == 'incubator':
                                service = self.incubator
                            elif service_type == 'microscope' and service_identifier:
                                service = self.microscope_services.get(service_identifier)
                            elif service_type == 'robotic_arm':
                                service = self.robotic_arm
                            elif service_type == 'hamilton':
                                service = self.hamilton_executor
                            consecutive_failures = 0
                        except Exception as reconnect_err:
                            logger.error(f"Full reconnect failed: {reconnect_err}. Will retry in 60 seconds.")
                            await asyncio.sleep(60)
                        continue

                    except Exception as refresh_error:
                        logger.error(f"Failed to refresh {service_name} service proxy: {refresh_error}")

                        if consecutive_failures >= max_failures:
                            if service_type == 'microscope':
                                # An offline microscope should not crash the orchestrator.
                                # Drop it from active services — it will be reconnected when a task needs it.
                                logger.warning(
                                    f"{service_name} unreachable after {max_failures} attempts. "
                                    f"Removing from active services. Will reconnect when a task requires it."
                                )
                                if service_identifier and service_identifier in self.microscope_services:
                                    del self.microscope_services[service_identifier]
                                if service_identifier and service_identifier in self.sample_on_microscope_flags:
                                    self.sample_on_microscope_flags[service_identifier] = False
                                key = (service_type, service_identifier) if service_identifier else service_type
                                self.health_check_tasks.pop(key, None)
                                return  # Exit health check loop for this microscope
                            else:
                                # For incubator/robotic arm: log critical but keep retrying, don't exit.
                                logger.critical(
                                    f"{service_name} unreachable after {max_failures} attempts while idle. "
                                    f"This is unexpected — check hardware. Resetting counter and continuing to retry."
                                )
                                consecutive_failures = 0

                        logger.info(f"Will retry {service_name} refresh in 60 seconds...")
                        await asyncio.sleep(60)
                        continue

            await asyncio.sleep(30)  # Check every 30 seconds when healthy

    _DEAD_CONNECTION_KEYWORDS = ("1011", "ping timeout", "no close frame", "keepalive", "connection closed", "websocket")

    async def _refresh_service_proxy(self, service_type, service_id):
        """Refresh a service proxy from the existing stable server connection.
        This does NOT create a new server connection, just gets a fresh service reference.
        Raises ConnectionError if the underlying server connection appears dead."""
        if not self.local_server_connection:
            raise ConnectionError("Local server connection not available")

        try:
            if service_type == 'incubator':
                logger.info(f"Refreshing incubator service proxy ({self.incubator_id})...")
                self.incubator = await self.local_server_connection.get_service(self.incubator_id)
                logger.info(f"Incubator service proxy refreshed.")

            elif service_type == 'microscope':
                if not service_id:
                    raise Exception("Microscope service_id required for refresh")
                logger.info(f"Refreshing microscope service proxy ({service_id})...")
                microscope_service = await self.local_server_connection.get_service(service_id)
                self.microscope_services[service_id] = microscope_service
                logger.info(f"Microscope service proxy {service_id} refreshed.")

            elif service_type == 'robotic_arm':
                logger.info(f"Refreshing robotic arm service proxy ({self.robotic_arm_id})...")
                self.robotic_arm = await self.local_server_connection.get_service(self.robotic_arm_id)
                logger.info(f"Robotic arm service proxy refreshed.")
            elif service_type == 'hamilton':
                logger.info(f"Refreshing Hamilton executor service proxy ({self.hamilton_executor_id})...")
                self.hamilton_executor = await self.local_server_connection.get_service(self.hamilton_executor_id)
                logger.info("Hamilton executor service proxy refreshed.")
            else:
                raise Exception(f"Unknown service type: {service_type}")
        except ConnectionError:
            raise
        except Exception as e:
            err_lower = str(e).lower()
            if any(kw in err_lower for kw in self._DEAD_CONNECTION_KEYWORDS):
                raise ConnectionError(f"Server connection dead: {e}") from e
            raise

    async def _reset_and_reconnect_local_server(self):
        """Null out the dead server connection and all service proxies, then reconnect."""
        logger.warning("Resetting dead local server connection and all service proxies for full reconnect...")
        self.local_server_connection = None
        self.incubator = None
        self.robotic_arm = None
        self.hamilton_executor = None
        self.microscope_services.clear()
        return await self.setup_connections()

    async def disconnect_single_service(self, service_type, service_id_to_disconnect=None):
        """Clear a specific service reference and stop its health check."""
        actual_service_id = service_id_to_disconnect
        if service_type == 'incubator':
            actual_service_id = self.incubator_id
        elif service_type == 'robotic_arm':
            actual_service_id = self.robotic_arm_id
        elif service_type == 'hamilton':
            actual_service_id = self.hamilton_executor_id
        
        if actual_service_id:
             await self._stop_health_check(service_type, actual_service_id)

        try:
            if service_type == 'incubator' and self.incubator:
                logger.info(f"Clearing incubator service reference ({self.incubator_id})...")
                self.incubator = None
            elif service_type == 'microscope':
                if service_id_to_disconnect and service_id_to_disconnect in self.microscope_services:
                    logger.info(f"Clearing microscope service reference ({service_id_to_disconnect})...")
                    self.microscope_services.pop(service_id_to_disconnect)
                    if service_id_to_disconnect in self.sample_on_microscope_flags:
                        self.sample_on_microscope_flags[service_id_to_disconnect] = False 
            elif service_type == 'robotic_arm' and self.robotic_arm:
                logger.info(f"Clearing robotic arm service reference ({self.robotic_arm_id})...")
                self.robotic_arm = None
            elif service_type == 'hamilton' and self.hamilton_executor:
                logger.info(f"Clearing Hamilton executor service reference ({self.hamilton_executor_id})...")
                self.hamilton_executor = None
                
        except Exception as e:
            logger.error(f"Error clearing {service_type} service reference ({service_id_to_disconnect if service_id_to_disconnect else ''}): {e}")


    async def setup_connections(self): 
        """Set up ONE stable connection to local Hypha server and get all service proxies.
        The server connection is kept alive throughout, only service proxies are refreshed on failures."""
        try:
            connection = await self._ensure_local_server_connection()
            if not connection:
                return False

            logger.info("Reusing existing stable server connection.")

            # Get service proxies from the stable connection
            if not self.incubator:
                self.incubator = await connection.get_service(self.incubator_id)
                logger.info(f"Incubator ({self.incubator_id}) service proxy obtained.")
                await self._start_health_check('incubator', self.incubator, self.incubator_id)
                
            if not self.robotic_arm:
                self.robotic_arm = await connection.get_service(self.robotic_arm_id)
                logger.info(f"Robotic arm ({self.robotic_arm_id}) service proxy obtained.")
                await self._start_health_check('robotic_arm', self.robotic_arm, self.robotic_arm_id)

            if not self.hamilton_executor:
                try:
                    self.hamilton_executor = await connection.get_service(self.hamilton_executor_id)
                    logger.info(f"Hamilton executor ({self.hamilton_executor_id}) service proxy obtained.")
                    await self._start_health_check('hamilton', self.hamilton_executor, self.hamilton_executor_id)
                except Exception as hamilton_error:
                    logger.warning(
                        f"Hamilton executor ({self.hamilton_executor_id}) is not currently available: {hamilton_error}"
                    )

        except Exception as e:
            logger.error(f"Failed to setup local services (incubator/robotic arm): {e}")
            return False 

        # Get microscope service proxies from the stable connection
        connected_microscope_count = 0
        if not self.configured_microscopes_info:
            logger.warning("No microscopes defined in the configuration.")
        else:
            logger.info(f"Found {len(self.configured_microscopes_info)} configured microscopes: {list(self.configured_microscopes_info.keys())}")
        
        for mic_id in self.configured_microscopes_info.keys():
            if mic_id not in self.microscope_services: 
                logger.info(f"Getting service proxy for microscope: {mic_id}...")
                try:
                    microscope_service_instance = await connection.get_service(mic_id)
                    self.microscope_services[mic_id] = microscope_service_instance
                    if mic_id not in self.sample_on_microscope_flags:
                        self.sample_on_microscope_flags[mic_id] = False
                    logger.info(f"Microscope {mic_id} service proxy obtained.")
                    
                    await self._start_health_check('microscope', microscope_service_instance, mic_id)
                    connected_microscope_count += 1
                except Exception as e:
                    logger.error(f"Failed to get service proxy for microscope {mic_id}: {e}")
                    if mic_id in self.microscope_services: 
                        del self.microscope_services[mic_id]
            else:
                logger.info(f"Microscope {mic_id} already has service proxy.")
                connected_microscope_count += 1
        
        # Clean up microscopes no longer in config
        connected_ids = list(self.microscope_services.keys())
        for mid in connected_ids:
            if mid not in self.configured_microscopes_info:
                logger.info(f"Microscope {mid} no longer in configuration. Removing service proxy.")
                await self.disconnect_single_service('microscope', mid)

        logger.info(f'Connection setup completed. {connected_microscope_count}/{len(self.configured_microscopes_info)} microscopes ready.')
        
        return bool(self.incubator and self.robotic_arm)

    async def disconnect_services(self):
        """Stop all health checks, clear service references, and disconnect from the stable server connection."""
        logger.info("Disconnecting all services...")
        
        # Clear service references and stop health checks
        if self.incubator:
            await self.disconnect_single_service('incubator', self.incubator_id) 
        
        microscope_ids_to_disconnect = list(self.microscope_services.keys())
        for mic_id in microscope_ids_to_disconnect:
            await self.disconnect_single_service('microscope', mic_id)
        
        if self.robotic_arm:
            await self.disconnect_single_service('robotic_arm', self.robotic_arm_id)

        if self.hamilton_executor:
            await self.disconnect_single_service('hamilton', self.hamilton_executor_id)
        
        # Disconnect the stable server connection
        if self.local_server_connection:
            try:
                logger.info("Disconnecting stable server connection...")
                await self.local_server_connection.disconnect()
                self.local_server_connection = None
                logger.info("Stable server connection disconnected.")
            except Exception as e:
                logger.error(f"Error disconnecting stable server connection: {e}")
                
        logger.info("Disconnect process completed.")

    async def _run_manual_transport_operation(self, action: str, incubator_slot: int, microscope_id: str):
        """Execute a transport request directly if all required resources are idle."""
        if not self.incubator or not self.robotic_arm or microscope_id not in self.microscope_services:
            setup_ok = await self.setup_connections()
            if not setup_ok or microscope_id not in self.microscope_services:
                raise Exception(f"Transport services are not ready for microscope {microscope_id}.")

        request = self._build_request(
            f"manual-{action}",
            microscope_id=microscope_id,
            incubator_slot=incubator_slot,
            extra_resources=self._transport_resources(),
            metadata={"trigger": "api", "action": action},
        )

        async with self.admission_controller.hold(request):
            if action == "load":
                await self._execute_load_operation(
                    incubator_slot,
                    microscope_id,
                    manage_transport_resources=False,
                )
            elif action == "unload":
                await self._execute_unload_operation(
                    incubator_slot,
                    microscope_id,
                    manage_transport_resources=False,
                )
            else:
                raise ValueError(f"Unknown manual transport action '{action}'.")

    async def transport_plate_api(self, from_device: str, to_device: str, slot: int = None):
        """
        Unified API endpoint to transport a plate between devices.
        
        Args:
            from_device: Source device service ID - 'incubator', 'hamilton', or microscope ID
            to_device: Target device service ID - 'incubator', 'hamilton', or microscope ID  
            slot: Incubator slot number (1-42). Required when incubator is involved
                  to identify and track which plate is being moved.
        
        Supported device IDs:
        - 'incubator' - The Cytomat incubator
        - 'hamilton' - The Hamilton liquid handler
        - 'microscope-squid-1' - Microscope 1
        - 'microscope-squid-2' - Microscope 2  
        - 'microscope-squid-plus-3' - Microscope 3
        
        Examples:
        - incubator -> microscope: `transport_plate("incubator", "microscope-squid-1", slot=5)`
        - microscope -> incubator: `transport_plate("microscope-squid-1", "incubator", slot=5)`  
        - incubator -> hamilton: `transport_plate("incubator", "hamilton", slot=5)`
        - hamilton -> microscope: `transport_plate("hamilton", "microscope-squid-2", slot=5)`
        - microscope -> hamilton: `transport_plate("microscope-squid-1", "hamilton", slot=5)`
        """
        logger.info(f"API call: transport_plate from '{from_device}' to '{to_device}' (slot={slot})")
        
        from_device = from_device.lower().strip()
        to_device = to_device.lower().strip()
        
        valid_devices = {"incubator", "hamilton", "microscope-squid-1", "microscope-squid-2", "microscope-squid-plus-3"}
        if from_device not in valid_devices:
            return {"success": False, "message": f"Invalid from_device '{from_device}'. Must be one of: {valid_devices}"}
        if to_device not in valid_devices:
            return {"success": False, "message": f"Invalid to_device '{to_device}'. Must be one of: {valid_devices}"}
        if from_device == to_device:
            return {"success": False, "message": f"Source and target devices cannot be the same: '{from_device}'"}
        
        incubator_involved = from_device == "incubator" or to_device == "incubator"
        if incubator_involved:
            if slot is None:
                return {"success": False, "message": "slot is required when incubator is involved"}
            if not isinstance(slot, int) or slot < 1 or slot > 42:
                return {"success": False, "message": f"slot must be an integer between 1-42, got {slot}"}
        
        try:
            if from_device == "incubator" and to_device.startswith("microscope-"):
                return await self._load_plate_api_wrapper(slot, to_device)
            elif from_device.startswith("microscope-") and to_device == "incubator":
                return await self._unload_plate_api_wrapper(slot, from_device)
            elif from_device == "incubator" and to_device == "hamilton":
                return await self._load_to_hamilton_api_wrapper(slot)
            elif from_device == "hamilton" and to_device == "incubator":
                return await self._unload_from_hamilton_api_wrapper(slot)
            elif from_device.startswith("microscope-") and to_device == "hamilton":
                return await self._microscope_to_hamilton_api_wrapper(slot, from_device)
            elif from_device == "hamilton" and to_device.startswith("microscope-"):
                return await self._hamilton_to_microscope_api_wrapper(slot, to_device)
            elif from_device.startswith("microscope-") and to_device.startswith("microscope-"):
                return await self._microscope_to_microscope_api_wrapper(slot, from_device, to_device)
            else:
                return {"success": False, "message": f"Unsupported route: '{from_device}' -> '{to_device}'"}
        except Exception as e:
            logger.error(f"Transport operation failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _get_hamilton_executor_proxy(self, *, refresh_if_missing: bool = True):
        """Return the Hamilton executor proxy, optionally refreshing local services."""

        if self.hamilton_executor is None and refresh_if_missing:
            await self.setup_connections()
        return self.hamilton_executor

    async def _get_hamilton_executor_status(self, *, refresh_if_missing: bool = True):
        """Fetch the current Hamilton executor status, or ``None`` if unavailable."""

        service = await self._get_hamilton_executor_proxy(refresh_if_missing=refresh_if_missing)
        if service is None:
            return None
        return await service.get_status()

    async def _assert_hamilton_idle_for_transport(self):
        """Reject Hamilton transport while the executor is busy with a protocol run."""

        status = await self._get_hamilton_executor_status()
        if status is None:
            raise RuntimeError(
                f"Hamilton executor service '{self.hamilton_executor_id}' is not available."
            )
        if status.get("busy"):
            current_action = status.get("current_action_id") or status.get("action_id")
            raise HamiltonBusyError(
                f"Hamilton is busy executing a protocol"
                f"{f' ({current_action})' if current_action else ''}."
            )
        return status

    @schema_function(skip_self=True)
    async def get_hamilton_status(self):
        """Return Hamilton executor connectivity and the current executor status."""

        try:
            executor_status = await self._get_hamilton_executor_status()
            admission_snapshot = await self.admission_controller.snapshot()
            hamilton_operations = [
                operation
                for operation in admission_snapshot["active_operations"]
                if self._hamilton_resource() in operation.get("resources", [])
            ]
            return {
                "success": True,
                "connected": executor_status is not None,
                "service_id": self.hamilton_executor_id,
                "executor_status": executor_status,
                "active_operations": hamilton_operations,
            }
        except Exception as e:
            logger.error(f"Failed to get Hamilton status: {e}", exc_info=True)
            return {
                "success": False,
                "connected": False,
                "service_id": self.hamilton_executor_id,
                "executor_status": None,
                "active_operations": [],
                "message": str(e),
            }

    @schema_function(skip_self=True)
    async def run_hamilton_protocol(
        self,
        script_content: str,
        timeout: int = 3600,
    ):
        """Start a Hamilton script on the existing executor service without transport."""

        if not isinstance(script_content, str) or not script_content.strip():
            return {"success": False, "message": "script_content must be a non-empty string."}
        if timeout <= 0:
            return {"success": False, "message": "timeout must be greater than zero."}

        try:
            service = await self._get_hamilton_executor_proxy()
            if service is None:
                raise RuntimeError(
                    f"Hamilton executor service '{self.hamilton_executor_id}' is not available."
                )

            request = self._build_request(
                "hamilton-execution",
                extra_resources=(self._hamilton_resource(),),
                metadata={"timeout_seconds": timeout},
            )

            async with self.admission_controller.hold(request):
                start_result = await service.start_execution(
                    script_content=script_content,
                    timeout=timeout,
                )
                if not start_result.get("accepted"):
                    live_status = await self._get_hamilton_executor_status(refresh_if_missing=False)
                    message = start_result.get("error") or "Hamilton executor rejected the run request."
                    response = {
                        "success": False,
                        "message": message,
                        "start_result": start_result,
                        "hamilton_status": live_status,
                    }
                    if start_result.get("busy"):
                        response["state"] = "busy"
                    return response

            return {
                "success": True,
                "message": "Hamilton protocol accepted and started.",
                "start_result": start_result,
                "action_id": start_result.get("action_id"),
                "state": start_result.get("status", "running"),
                "hamilton_status": await self.get_hamilton_status(),
                "runtime_status": await self.get_runtime_status(),
            }
        except ResourceBusyError as busy_error:
            return self._busy_response(
                "Hamilton protocol request rejected - Hamilton-related resources are busy.",
                busy_error,
            )
        except Exception as e:
            logger.error(f"Hamilton protocol execution failed: {e}", exc_info=True)
            return {
                "success": False,
                "message": str(e),
                "hamilton_status": await self.get_hamilton_status(),
                "runtime_status": await self.get_runtime_status(),
            }

    async def _execute_load_operation(
        self,
        incubator_slot: int,
        microscope_id_str: str,
        *,
        manage_transport_resources: bool = True,
    ):
        """Execute the load operation: move sample from incubator to microscope."""
        if manage_transport_resources:
            request = self._build_request(
                "transport-load",
                extra_resources=self._transport_resources(),
                metadata={
                    "phase": "load",
                    "microscope_id": microscope_id_str,
                    "incubator_slot": incubator_slot,
                },
            )
            async with self.admission_controller.hold(request, wait=True):
                await self._execute_load_operation(
                    incubator_slot,
                    microscope_id_str,
                    manage_transport_resources=False,
                )
                return

        target_microscope_service = self.microscope_services.get(microscope_id_str)
        if not target_microscope_service:
            raise Exception(f"Microscope service {microscope_id_str} is not connected.")

        # Verify actual sample location from incubator service (source of truth)
        # This protects against stale in-memory flags after a crash/restart
        try:
            actual_location = await self.incubator.get_sample_location(incubator_slot)
            logger.info(f"Incubator reports sample location for slot {incubator_slot}: {actual_location}")
            if actual_location == microscope_id_str:
                self.sample_on_microscope_flags[microscope_id_str] = True
                logger.info(f"Sample already on microscope {microscope_id_str} per incubator. Skipping load.")
                return
            elif actual_location == "incubator_slot":
                self.sample_on_microscope_flags[microscope_id_str] = False
            # For other locations (e.g. "robotic_arm"), fall through to the in-memory flag
        except Exception as e:
            logger.warning(f"Could not verify sample location from incubator for slot {incubator_slot}: {e}. Falling back to in-memory flag.")

        if self.sample_on_microscope_flags.get(microscope_id_str, False):
            logger.info(f"Sample plate already on microscope {microscope_id_str} (in-memory flag). Skipping load.")
            return

        logger.info(f"Loading sample from incubator slot {incubator_slot} to microscope {microscope_id_str}...")
        
        # Mark critical operation
        self.in_critical_operation = True
        logger.info("CRITICAL OPERATION START: Robotic arm loading sample")
        critical_services = [
            ('robotic_arm', self.robotic_arm_id),
            ('microscope', microscope_id_str),
            ('incubator', self.incubator_id)
        ]
        self._mark_critical_services(critical_services)
        
        try:
            # Start parallel operations: prepare incubator and home the stage
            await asyncio.gather(
                self.incubator.get_sample_from_slot_to_transfer_station(incubator_slot),
                target_microscope_service.home_stage(),
            )
            
            # Move sample with robotic arm (unified transport API)
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await self.robotic_arm.transport_plate(from_device="incubator", to_device=microscope_id_str)
            
            # Return microscope stage and update location
            await asyncio.gather(
                self.incubator.update_sample_location(incubator_slot, microscope_id_str),
                target_microscope_service.return_stage()
            )
            
            logger.info(f"Sample loaded onto microscope {microscope_id_str}.")
            self.sample_on_microscope_flags[microscope_id_str] = True
            
        except Exception as e:
            error_msg = f"Failed to load sample from slot {incubator_slot} to microscope {microscope_id_str}: {e}"
            logger.error(error_msg)
            self.sample_on_microscope_flags[microscope_id_str] = False
            raise Exception(error_msg)
        finally:
            self.in_critical_operation = False
            logger.info("CRITICAL OPERATION END: Robotic arm load complete")
            self._unmark_critical_services(critical_services)

    async def _load_plate_api_wrapper(self, slot: int, microscope_id: str):
        """Internal wrapper for incubator -> microscope transport."""
        if microscope_id not in self.configured_microscopes_info:
            return {"success": False, "message": f"Microscope ID '{microscope_id}' not found in configured microscopes."}
        try:
            await self._run_manual_transport_operation("load", slot, microscope_id)
            return {"success": True, "message": f"Load from slot {slot} to {microscope_id} completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response(f"Load request rejected - orchestrator is busy.", busy_error)
        except Exception as e:
            logger.error(f"Load operation failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _unload_plate_api_wrapper(self, slot: int, microscope_id: str):
        """Internal wrapper for microscope -> incubator transport."""
        if microscope_id not in self.configured_microscopes_info:
            return {"success": False, "message": f"Microscope ID '{microscope_id}' not found in configured microscopes."}
        try:
            await self._run_manual_transport_operation("unload", slot, microscope_id)
            return {"success": True, "message": f"Unload from {microscope_id} to slot {slot} completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response(f"Unload request rejected - orchestrator is busy.", busy_error)
        except Exception as e:
            logger.error(f"Unload operation failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _microscope_to_microscope_api_wrapper(self, slot: int, from_microscope: str, to_microscope: str):
        """Internal wrapper for microscope -> microscope transport."""
        if from_microscope not in self.configured_microscopes_info:
            return {"success": False, "message": f"Source microscope ID '{from_microscope}' not found in configured microscopes."}
        if to_microscope not in self.configured_microscopes_info:
            return {"success": False, "message": f"Target microscope ID '{to_microscope}' not found in configured microscopes."}
        if from_microscope == to_microscope:
            return {"success": False, "message": f"Source and target microscope cannot be the same: '{from_microscope}'"}
        
        from_service = self.microscope_services.get(from_microscope)
        to_service = self.microscope_services.get(to_microscope)
        
        # Verify sample is on the source microscope
        try:
            actual_location = await self.incubator.get_sample_location(slot)
            logger.info(f"Actual sample location for slot {slot}: {actual_location}")
            if actual_location != from_microscope:
                return {"success": False, "message": f"Sample not on source microscope {from_microscope}, current location: {actual_location}"}
        except Exception as e:
            logger.warning(f"Could not verify sample location from incubator for slot {slot}: {e}. Proceeding anyway.")
        
        if not self.sample_on_microscope_flags.get(from_microscope, False):
            return {"success": False, "message": f"Sample plate not on microscope {from_microscope} according to flags"}
        
        try:
            logger.info(f"Microscope-to-microscope: Moving plate from {from_microscope} to {to_microscope}")
            
            # Mark critical operation
            self.in_critical_operation = True
            logger.info("CRITICAL OPERATION START: Robotic arm microscope-to-microscope transport")
            critical_services = [
                ('robotic_arm', self.robotic_arm_id),
                ('microscope', from_microscope),
                ('microscope', to_microscope),
            ]
            self._mark_critical_services(critical_services)
            
            try:
                # Home stages on both microscopes
                await asyncio.gather(
                    from_service.home_stage(),
                    to_service.home_stage(),
                )
                
                # Update location to robotic arm
                await self.incubator.update_sample_location(slot, "robotic_arm")
                
                # Direct transport: grab from microscope 1, put on microscope 2
                await self.robotic_arm.transport_plate(from_device=from_microscope, to_device=to_microscope)
                
                # Return stages and update location
                await asyncio.gather(
                    from_service.return_stage(),
                    to_service.return_stage(),
                )
                await self.incubator.update_sample_location(slot, to_microscope)
                
                # Update flags
                self.sample_on_microscope_flags[from_microscope] = False
                self.sample_on_microscope_flags[to_microscope] = True
                
                logger.info(f"Plate moved from {from_microscope} to {to_microscope}")
                
            finally:
                self.in_critical_operation = False
                logger.info("CRITICAL OPERATION END: Robotic arm microscope-to-microscope transport complete")
                self._unmark_critical_services(critical_services)
            
            return {"success": True, "message": f"Transport from {from_microscope} to {to_microscope} completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response(f"Microscope-to-microscope transport request rejected - orchestrator is busy.", busy_error)
        except Exception as e:
            logger.error(f"Microscope-to-microscope transport failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _execute_load_to_hamilton_operation(
        self,
        incubator_slot: int,
        *,
        manage_transport_resources: bool = True,
    ):
        """Execute the load operation: move sample from incubator to Hamilton."""
        if manage_transport_resources:
            request = self._build_request(
                "transport-load-hamilton",
                extra_resources=self._hamilton_transport_resources(),
                metadata={
                    "phase": "load_to_hamilton",
                    "incubator_slot": incubator_slot,
                },
            )
            async with self.admission_controller.hold(request, wait=True):
                await self._execute_load_to_hamilton_operation(
                    incubator_slot,
                    manage_transport_resources=False,
                )
                return

        # Verify sample is in incubator before loading
        try:
            actual_location = await self.incubator.get_sample_location(incubator_slot)
            logger.info(f"Incubator reports sample location for slot {incubator_slot}: {actual_location}")
            if actual_location != "incubator_slot":
                logger.warning(f"Sample not in incubator slot {incubator_slot}, location: {actual_location}")
                return
        except Exception as e:
            logger.warning(f"Could not verify sample location from incubator for slot {incubator_slot}: {e}. Proceeding anyway.")

        await self._assert_hamilton_idle_for_transport()
        logger.info(f"Loading sample from incubator slot {incubator_slot} to Hamilton...")

        # Mark critical operation
        self.in_critical_operation = True
        logger.info("CRITICAL OPERATION START: Robotic arm loading sample to Hamilton")
        critical_services = [
            ('robotic_arm', self.robotic_arm_id),
            ('incubator', self.incubator_id)
        ]
        self._mark_critical_services(critical_services)

        try:
            # Start parallel operations: prepare incubator
            await asyncio.gather(
                self.incubator.get_sample_from_slot_to_transfer_station(incubator_slot),
            )

            # Move sample with robotic arm: incubator -> Hamilton (unified transport API)
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await self.robotic_arm.transport_plate(from_device="incubator", to_device="hamilton")

            # Update sample location to Hamilton
            await self.incubator.update_sample_location(incubator_slot, "hamilton")

            logger.info("Sample loaded onto Hamilton.")

        except Exception as e:
            error_msg = f"Failed to load sample from slot {incubator_slot} to Hamilton: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)
        finally:
            self.in_critical_operation = False
            logger.info("CRITICAL OPERATION END: Robotic arm load to Hamilton complete")
            self._unmark_critical_services(critical_services)

    async def _execute_unload_from_hamilton_operation(
        self,
        incubator_slot: int,
        *,
        manage_transport_resources: bool = True,
    ):
        """Execute the unload operation: move sample from Hamilton to incubator."""
        if manage_transport_resources:
            request = self._build_request(
                "transport-unload-hamilton",
                extra_resources=self._hamilton_transport_resources(),
                metadata={
                    "phase": "unload_from_hamilton",
                    "incubator_slot": incubator_slot,
                },
            )
            async with self.admission_controller.hold(request, wait=True):
                await self._execute_unload_from_hamilton_operation(
                    incubator_slot,
                    manage_transport_resources=False,
                )
                return

        # Verify sample is at Hamilton before unloading
        try:
            actual_location = await self.incubator.get_sample_location(incubator_slot)
            logger.info(f"Actual sample location for slot {incubator_slot}: {actual_location}")

            if actual_location == "incubator_slot":
                logger.info(f"Sample already in incubator slot {incubator_slot}, no unload needed")
                return
            elif actual_location != "hamilton":
                logger.warning(f"Sample not at Hamilton, location: {actual_location}")
                return
        except Exception as e:
            logger.warning(f"Could not verify sample location from incubator for slot {incubator_slot}: {e}. Proceeding anyway.")

        await self._assert_hamilton_idle_for_transport()
        logger.info(f"Unloading sample from Hamilton to incubator slot {incubator_slot}...")

        # Mark critical operation
        self.in_critical_operation = True
        logger.info("CRITICAL OPERATION START: Robotic arm unloading sample from Hamilton")
        critical_services = [
            ('robotic_arm', self.robotic_arm_id),
            ('incubator', self.incubator_id)
        ]
        self._mark_critical_services(critical_services)

        try:
            # Move sample with robotic arm: Hamilton -> incubator (unified transport API)
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await self.robotic_arm.transport_plate(from_device="hamilton", to_device="incubator")

            # Put sample back to incubator slot
            await self.incubator.put_sample_from_transfer_station_to_slot(incubator_slot)
            await self.incubator.update_sample_location(incubator_slot, "incubator_slot")

            logger.info("Sample unloaded from Hamilton to incubator.")

        except Exception as e:
            error_msg = f"Failed to unload sample from Hamilton to slot {incubator_slot}: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)
        finally:
            self.in_critical_operation = False
            logger.info("CRITICAL OPERATION END: Robotic arm unload from Hamilton complete")
            self._unmark_critical_services(critical_services)

    async def _load_to_hamilton_api_wrapper(self, slot: int):
        """Internal wrapper for incubator -> hamilton transport."""
        try:
            if not self.incubator or not self.robotic_arm or not self.hamilton_executor:
                if not await self.setup_connections():
                    raise Exception("Transport services are not ready.")
            if not self.hamilton_executor:
                raise Exception("Hamilton executor service is not ready.")
            request = self._build_request(
                "manual-load-hamilton",
                incubator_slot=slot,
                extra_resources=self._hamilton_transport_resources(),
                metadata={"trigger": "api", "action": "load_to_hamilton"},
            )
            async with self.admission_controller.hold(request):
                await self._execute_load_to_hamilton_operation(slot, manage_transport_resources=False)
            return {"success": True, "message": f"Load from slot {slot} to Hamilton completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response("Load request rejected - orchestrator is busy.", busy_error)
        except HamiltonBusyError as busy_error:
            return self._hamilton_busy_response(str(busy_error))
        except Exception as e:
            logger.error(f"Load operation to Hamilton failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _unload_from_hamilton_api_wrapper(self, slot: int):
        """Internal wrapper for hamilton -> incubator transport."""
        try:
            if not self.incubator or not self.robotic_arm or not self.hamilton_executor:
                if not await self.setup_connections():
                    raise Exception("Transport services are not ready.")
            if not self.hamilton_executor:
                raise Exception("Hamilton executor service is not ready.")
            request = self._build_request(
                "manual-unload-hamilton",
                incubator_slot=slot,
                extra_resources=self._hamilton_transport_resources(),
                metadata={"trigger": "api", "action": "unload_from_hamilton"},
            )
            async with self.admission_controller.hold(request):
                await self._execute_unload_from_hamilton_operation(slot, manage_transport_resources=False)
            return {"success": True, "message": f"Unload from Hamilton to slot {slot} completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response("Unload request rejected - orchestrator is busy.", busy_error)
        except HamiltonBusyError as busy_error:
            return self._hamilton_busy_response(str(busy_error))
        except Exception as e:
            logger.error(f"Unload operation from Hamilton failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _execute_microscope_to_hamilton_operation(
        self,
        incubator_slot: int,
        microscope_id_str: str,
        *,
        manage_transport_resources: bool = True,
    ):
        """Execute the transport operation: move sample from microscope to Hamilton."""
        if manage_transport_resources:
            request = self._build_request(
                "transport-microscope-to-hamilton",
                microscope_id=microscope_id_str,
                incubator_slot=incubator_slot,
                extra_resources=self._hamilton_transport_resources(),
                metadata={
                    "phase": "microscope_to_hamilton",
                    "microscope_id": microscope_id_str,
                    "incubator_slot": incubator_slot,
                },
            )
            async with self.admission_controller.hold(request, wait=True):
                await self._execute_microscope_to_hamilton_operation(
                    incubator_slot,
                    microscope_id_str,
                    manage_transport_resources=False,
                )
                return

        target_microscope_service = self.microscope_services.get(microscope_id_str)
        if not target_microscope_service:
            raise Exception(f"Microscope service {microscope_id_str} is not connected.")

        # Verify sample is on the microscope
        try:
            actual_location = await self.incubator.get_sample_location(incubator_slot)
            logger.info(f"Actual sample location for slot {incubator_slot}: {actual_location}")

            if actual_location != microscope_id_str:
                logger.warning(f"Sample not on microscope {microscope_id_str}, location: {actual_location}")
                return
        except Exception as e:
            logger.warning(f"Could not verify sample location from incubator for slot {incubator_slot}: {e}. Proceeding anyway.")

        if not self.sample_on_microscope_flags.get(microscope_id_str, False):
            logger.info(f"Sample plate not on microscope {microscope_id_str} according to flags")
            return

        await self._assert_hamilton_idle_for_transport()
        logger.info(f"Transporting sample from microscope {microscope_id_str} to Hamilton (slot {incubator_slot})...")

        # Mark critical operation
        self.in_critical_operation = True
        logger.info("CRITICAL OPERATION START: Robotic arm transporting sample from microscope to Hamilton")
        critical_services = [
            ('robotic_arm', self.robotic_arm_id),
            ('microscope', microscope_id_str),
            ('incubator', self.incubator_id)
        ]
        self._mark_critical_services(critical_services)

        try:
            # Home microscope stage
            await target_microscope_service.home_stage()

            # Move sample with robotic arm: microscope -> Hamilton (unified transport API)
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await self.robotic_arm.transport_plate(from_device=microscope_id_str, to_device="hamilton")

            # Return stage and update location
            await asyncio.gather(
                target_microscope_service.return_stage(),
                self.incubator.update_sample_location(incubator_slot, "hamilton")
            )

            self.sample_on_microscope_flags[microscope_id_str] = False
            logger.info("Sample transported from microscope to Hamilton.")

        except Exception as e:
            error_msg = f"Failed to transport sample from microscope {microscope_id_str} to Hamilton: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)
        finally:
            self.in_critical_operation = False
            logger.info("CRITICAL OPERATION END: Robotic arm microscope to Hamilton complete")
            self._unmark_critical_services(critical_services)

    async def _execute_hamilton_to_microscope_operation(
        self,
        incubator_slot: int,
        microscope_id_str: str,
        *,
        manage_transport_resources: bool = True,
    ):
        """Execute the transport operation: move sample from Hamilton to microscope."""
        if manage_transport_resources:
            request = self._build_request(
                "transport-hamilton-to-microscope",
                microscope_id=microscope_id_str,
                incubator_slot=incubator_slot,
                extra_resources=self._hamilton_transport_resources(),
                metadata={
                    "phase": "hamilton_to_microscope",
                    "microscope_id": microscope_id_str,
                    "incubator_slot": incubator_slot,
                },
            )
            async with self.admission_controller.hold(request, wait=True):
                await self._execute_hamilton_to_microscope_operation(
                    incubator_slot,
                    microscope_id_str,
                    manage_transport_resources=False,
                )
                return

        target_microscope_service = self.microscope_services.get(microscope_id_str)
        if not target_microscope_service:
            raise Exception(f"Microscope service {microscope_id_str} is not connected.")

        # Verify sample is at Hamilton
        try:
            actual_location = await self.incubator.get_sample_location(incubator_slot)
            logger.info(f"Incubator reports sample location for slot {incubator_slot}: {actual_location}")
            if actual_location != "hamilton":
                logger.warning(f"Sample not at Hamilton, location: {actual_location}")
                return
        except Exception as e:
            logger.warning(f"Could not verify sample location from incubator for slot {incubator_slot}: {e}. Proceeding anyway.")

        if self.sample_on_microscope_flags.get(microscope_id_str, False):
            logger.info(f"Sample plate already on microscope {microscope_id_str}. Skipping load.")
            return

        await self._assert_hamilton_idle_for_transport()
        logger.info(f"Transporting sample from Hamilton to microscope {microscope_id_str} (slot {incubator_slot})...")

        # Mark critical operation
        self.in_critical_operation = True
        logger.info("CRITICAL OPERATION START: Robotic arm transporting sample from Hamilton to microscope")
        critical_services = [
            ('robotic_arm', self.robotic_arm_id),
            ('microscope', microscope_id_str),
            ('incubator', self.incubator_id)
        ]
        self._mark_critical_services(critical_services)

        try:
            # Prepare the microscope stage before the transfer
            await asyncio.gather(
                target_microscope_service.home_stage(),
            )

            # Move sample with robotic arm: Hamilton -> microscope (unified transport API)
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await self.robotic_arm.transport_plate(from_device="hamilton", to_device=microscope_id_str)

            # Return stage and update location
            await asyncio.gather(
                target_microscope_service.return_stage(),
                self.incubator.update_sample_location(incubator_slot, microscope_id_str)
            )

            self.sample_on_microscope_flags[microscope_id_str] = True
            logger.info("Sample transported from Hamilton to microscope.")

        except Exception as e:
            error_msg = f"Failed to transport sample from Hamilton to microscope {microscope_id_str}: {e}"
            logger.error(error_msg)
            self.sample_on_microscope_flags[microscope_id_str] = False
            raise Exception(error_msg)
        finally:
            self.in_critical_operation = False
            logger.info("CRITICAL OPERATION END: Robotic arm Hamilton to microscope complete")
            self._unmark_critical_services(critical_services)

    async def _microscope_to_hamilton_api_wrapper(self, slot: int, microscope_id: str):
        """Internal wrapper for microscope -> hamilton transport."""
        if microscope_id not in self.configured_microscopes_info:
            return {"success": False, "message": f"Microscope ID '{microscope_id}' not found in configured microscopes."}
        try:
            if not self.incubator or not self.robotic_arm or not self.hamilton_executor or microscope_id not in self.microscope_services:
                if not await self.setup_connections() or microscope_id not in self.microscope_services:
                    raise Exception(f"Transport services are not ready for microscope {microscope_id}.")
            if not self.hamilton_executor:
                raise Exception("Hamilton executor service is not ready.")
            request = self._build_request(
                "manual-microscope-to-hamilton",
                microscope_id=microscope_id,
                incubator_slot=slot,
                extra_resources=self._hamilton_transport_resources(),
                metadata={"trigger": "api", "action": "microscope_to_hamilton"},
            )
            async with self.admission_controller.hold(request):
                await self._execute_microscope_to_hamilton_operation(slot, microscope_id, manage_transport_resources=False)
            return {"success": True, "message": f"Transport from {microscope_id} to Hamilton completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response("Transport request rejected - orchestrator is busy.", busy_error)
        except HamiltonBusyError as busy_error:
            return self._hamilton_busy_response(str(busy_error))
        except Exception as e:
            logger.error(f"Transport from microscope to Hamilton failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _hamilton_to_microscope_api_wrapper(self, slot: int, microscope_id: str):
        """Internal wrapper for hamilton -> microscope transport."""
        if microscope_id not in self.configured_microscopes_info:
            return {"success": False, "message": f"Microscope ID '{microscope_id}' not found in configured microscopes."}
        try:
            if not self.incubator or not self.robotic_arm or not self.hamilton_executor or microscope_id not in self.microscope_services:
                if not await self.setup_connections() or microscope_id not in self.microscope_services:
                    raise Exception(f"Transport services are not ready for microscope {microscope_id}.")
            if not self.hamilton_executor:
                raise Exception("Hamilton executor service is not ready.")
            request = self._build_request(
                "manual-hamilton-to-microscope",
                microscope_id=microscope_id,
                incubator_slot=slot,
                extra_resources=self._hamilton_transport_resources(),
                metadata={"trigger": "api", "action": "hamilton_to_microscope"},
            )
            async with self.admission_controller.hold(request):
                await self._execute_hamilton_to_microscope_operation(slot, microscope_id, manage_transport_resources=False)
            return {"success": True, "message": f"Transport from Hamilton to {microscope_id} completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response("Transport request rejected - orchestrator is busy.", busy_error)
        except HamiltonBusyError as busy_error:
            return self._hamilton_busy_response(str(busy_error))
        except Exception as e:
            logger.error(f"Transport from Hamilton to microscope failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _execute_unload_operation(
        self,
        incubator_slot: int,
        microscope_id_str: str,
        *,
        manage_transport_resources: bool = True,
    ):
        """Execute the unload operation: move sample from microscope to incubator."""
        if manage_transport_resources:
            request = self._build_request(
                "transport-unload",
                extra_resources=self._transport_resources(),
                metadata={
                    "phase": "unload",
                    "microscope_id": microscope_id_str,
                    "incubator_slot": incubator_slot,
                },
            )
            async with self.admission_controller.hold(request, wait=True):
                await self._execute_unload_operation(
                    incubator_slot,
                    microscope_id_str,
                    manage_transport_resources=False,
                )
                return

        target_microscope_service = self.microscope_services.get(microscope_id_str)
        if not target_microscope_service:
            raise Exception(f"Microscope service {microscope_id_str} is not connected.")

        # Verify sample location from incubator service
        try:
            actual_location = await self.incubator.get_sample_location(incubator_slot)
            logger.info(f"Actual sample location for slot {incubator_slot}: {actual_location}")
            
            if actual_location == microscope_id_str:
                self.sample_on_microscope_flags[microscope_id_str] = True
                logger.info(f"Updated sample_on_microscope_flags[{microscope_id_str}] to True based on incubator location")
            elif actual_location == "incubator_slot":
                self.sample_on_microscope_flags[microscope_id_str] = False
                logger.info(f"Sample already in incubator slot {incubator_slot}, no unload needed")
                return
        except Exception as e:
            logger.warning(f"Could not verify sample location from incubator for slot {incubator_slot}: {e}. Proceeding with unload based on flag.")

        if not self.sample_on_microscope_flags.get(microscope_id_str, False):
            logger.info(f"Sample plate not on microscope {microscope_id_str} according to flags")
            return 
            
        logger.info(f"Unloading sample to incubator slot {incubator_slot} from microscope {microscope_id_str}...")

        # Mark critical operation
        self.in_critical_operation = True
        logger.info("CRITICAL OPERATION START: Robotic arm unloading sample")
        critical_services = [
            ('robotic_arm', self.robotic_arm_id),
            ('microscope', microscope_id_str),
            ('incubator', self.incubator_id)
        ]
        self._mark_critical_services(critical_services)

        try:
            # Home microscope stage
            await target_microscope_service.home_stage()
            
            # Move sample with robotic arm (unified transport API)
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await self.robotic_arm.transport_plate(from_device=microscope_id_str, to_device="incubator")
            
            # Put sample back and return stage in parallel
            await asyncio.gather(
                self.incubator.put_sample_from_transfer_station_to_slot(incubator_slot),
                target_microscope_service.return_stage()
            )
            await self.incubator.update_sample_location(incubator_slot, "incubator_slot")
            
            logger.info(f"Sample unloaded from microscope {microscope_id_str}.")
            self.sample_on_microscope_flags[microscope_id_str] = False
            
        except Exception as e:
            error_msg = f"Failed to unload sample to slot {incubator_slot} from microscope {microscope_id_str}: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)
        finally:
            self.in_critical_operation = False
            logger.info("CRITICAL OPERATION END: Robotic arm unload complete")
            self._unmark_critical_services(critical_services)

    async def _poll_scan_status(self, microscope_service):
        """
        Poll the scan status from microscope service using scan_get_status().

        This method continuously polls the microscope service every 10 seconds to check
        the scan progress. It handles WebSocket interruptions gracefully by retrying
        failed status checks. Also monitors busy_status for enhanced state tracking.

        Args:
            microscope_service: The microscope service proxy to poll

        Returns:
            None when scan completes successfully

        Raises:
            Exception: If scan fails or encounters an error
        """
        poll_interval = 10  # seconds between status checks
        consecutive_failures = 0
        max_consecutive_failures = 3

        logger.info(f"Starting scan status polling (interval: {poll_interval}s)")

        while True:
            try:
                # Poll status from microscope service
                status_response = await asyncio.wait_for(
                    microscope_service.scan_get_status(),
                    timeout=15  # Give extra time for the RPC call itself
                )
                
                # Reset failure counter on successful poll
                consecutive_failures = 0
                
                # Extract status information
                status = status_response.get("state", "unknown")
                busy_status = status_response.get("busy_status", "unknown")
                
                # Log full response for debugging when status is unknown
                if status == "unknown":
                    logger.debug(f"Full scan status response: {status_response}")
                
                # Check terminal states
                if status == "completed":
                    logger.info("Scan completed successfully")
                    return
                
                elif status == "failed":
                    error = status_response.get("error", "Unknown error")
                    error_msg = f"Scan failed: {error}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                
                elif status == "running":
                    # Log busy_status if available for enhanced debugging
                    if busy_status != "unknown":
                        logger.debug(f"Scan running - busy_status: {busy_status}")
                    # Continue polling
                    await asyncio.sleep(poll_interval)
                
                elif status == "idle":
                    # Microscope service indicates it's idle (scan completed)
                    logger.info("Scan completed - microscope service is idle")
                    return
                
                elif status == "unknown":
                    # Handle unknown status
                    logger.warning(f"Unknown scan status: {status}. Continuing to poll...")
                    await asyncio.sleep(poll_interval)
                
                else:
                    logger.warning(f"Unknown scan status: {status}. Continuing to poll...")
                    await asyncio.sleep(poll_interval)
                    
            except asyncio.TimeoutError:
                consecutive_failures += 1
                logger.warning(f"Status poll timed out (attempt {consecutive_failures}/{max_consecutive_failures})")
                
                if consecutive_failures >= max_consecutive_failures:
                    error_msg = f"Failed to get scan status after {max_consecutive_failures} consecutive attempts"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                
                # Wait a bit before retrying
                await asyncio.sleep(poll_interval)
                
            except Exception as e:
                consecutive_failures += 1
                logger.warning(f"Error polling scan status (attempt {consecutive_failures}/{max_consecutive_failures}): {e}")
                
                if consecutive_failures >= max_consecutive_failures:
                    error_msg = f"Failed to poll scan status after {max_consecutive_failures} consecutive attempts: {e}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                
                # Wait before retrying
                await asyncio.sleep(poll_interval)

    async def _reap_finished_cycle_tasks(self):
        """Clean up completed background cycle tasks."""
        completed_task_names = [
            task_name
            for task_name, task in self._scheduled_cycle_tasks.items()
            if task.done()
        ]

        for task_name in completed_task_names:
            finished_task = self._scheduled_cycle_tasks.pop(task_name)
            try:
                await finished_task
            except Exception as exc:
                logger.error(f"Background cycle task '{task_name}' exited with an unexpected error: {exc}")

    async def _start_due_task(self, task_name: str, earliest_pending_tp: datetime) -> bool:
        """Attempt to start a due task if its resources are currently idle."""
        if task_name in self._scheduled_cycle_tasks:
            return False

        task_data = self.tasks.get(task_name)
        if not task_data:
            logger.warning(f"Task '{task_name}' disappeared before scheduling.")
            return False

        task_config_for_cycle = copy.deepcopy(task_data["config"])
        allocated_microscope_id = task_config_for_cycle.get("allocated_microscope")
        if not allocated_microscope_id:
            logger.error(f"Task {task_name} does not have an 'allocated_microscope'. Marking as error.")
            await self._update_task_state_and_write_config(task_name, status="error")
            return False

        if allocated_microscope_id not in self.configured_microscopes_info:
            logger.error(f"Task '{task_name}' references unknown microscope '{allocated_microscope_id}'.")
            await self._update_task_state_and_write_config(task_name, status="error")
            return False

        scan_mode = task_config_for_cycle.get("scan_mode", "full_automation")
        incubator_slot = None
        if scan_mode == "full_automation":
            incubator_slot = task_config_for_cycle.get("incubator_slot")
            if incubator_slot is None:
                logger.error(f"Task '{task_name}' is full_automation but missing 'incubator_slot'.")
                await self._update_task_state_and_write_config(task_name, status="error")
                return False

        try:
            if scan_mode == "full_automation":
                if not self.incubator or not self.robotic_arm or allocated_microscope_id not in self.microscope_services:
                    await self.setup_connections()
                if not self.incubator or not self.robotic_arm:
                    logger.warning(f"Shared transport services are not ready for task '{task_name}'. Will retry later.")
                    return False
            elif allocated_microscope_id not in self.microscope_services:
                await self.setup_connections()
        except Exception as setup_error:
            logger.warning(f"Failed to prepare services for task '{task_name}': {setup_error}")
            return False

        target_microscope_service = self.microscope_services.get(allocated_microscope_id)
        if not target_microscope_service:
            logger.warning(f"Microscope '{allocated_microscope_id}' is not available for task '{task_name}'.")
            return False

        request = self._build_request(
            "scheduled-cycle",
            task_name=task_name,
            microscope_id=allocated_microscope_id,
            incubator_slot=incubator_slot,
            metadata={
                "trigger": "scheduler",
                "timepoint": earliest_pending_tp.strftime("%Y-%m-%dT%H:%M:%S"),
                "scan_mode": scan_mode,
            },
        )

        try:
            lease = await self.admission_controller.try_acquire(request)
        except ResourceBusyError as busy_error:
            blockers = ", ".join(
                f"{blocker.resource}:{blocker.operation_type}"
                for blocker in busy_error.blockers
            )
            logger.info(f"Task '{task_name}' is due but blocked by active resources: {blockers}")
            return False

        await self._update_task_state_and_write_config(task_name, status="active")
        self._mark_task_running(task_name)

        try:
            cycle_task = asyncio.create_task(
                self._run_scheduled_cycle(
                    task_name=task_name,
                    task_config_for_cycle=task_config_for_cycle,
                    target_microscope_service=target_microscope_service,
                    allocated_microscope_id=allocated_microscope_id,
                    current_pending_tp_to_process=earliest_pending_tp,
                    lease=lease,
                )
            )
            self._scheduled_cycle_tasks[task_name] = cycle_task
            return True
        except Exception:
            self._mark_task_not_running(task_name)
            await self.admission_controller.release(lease.operation_id)
            raise

    async def _run_scheduled_cycle(
        self,
        *,
        task_name: str,
        task_config_for_cycle: dict,
        target_microscope_service,
        allocated_microscope_id: str,
        current_pending_tp_to_process: datetime,
        lease,
    ):
        """Run one scheduled cycle in the background while holding the task lease."""
        scan_mode = task_config_for_cycle.get("scan_mode", "full_automation")

        try:
            if scan_mode == "full_automation":
                await self.run_cycle(task_config_for_cycle, target_microscope_service, allocated_microscope_id)
            else:
                await self.run_microscope_only_cycle(task_config_for_cycle, target_microscope_service, allocated_microscope_id)

            logger.info(
                f"Cycle for task {task_name} on {allocated_microscope_id}, "
                f"time point {current_pending_tp_to_process.isoformat()} success."
            )
            await self._update_task_state_and_write_config(
                task_name,
                status="waiting_for_next_run",
                current_tp_to_move_to_imaged=current_pending_tp_to_process,
            )
        except Exception as cycle_error:
            logger.error(
                f"Cycle for task {task_name} on {allocated_microscope_id}, "
                f"time point {current_pending_tp_to_process.isoformat()} failed: {cycle_error}"
            )
            await self._update_task_state_and_write_config(task_name, status="error")
        finally:
            self._mark_task_not_running(task_name)
            self._scheduled_cycle_tasks.pop(task_name, None)
            await self.admission_controller.release(lease.operation_id)

    async def run_cycle(self, task_config, microscope_service, allocated_microscope_id): # MODIFIED: added microscope_service, allocated_microscope_id
        """Run the complete load-scan-unload process for a given task on a specific microscope."""
        task_name = task_config["name"]
        incubator_slot = task_config["incubator_slot"]
        action_id = f"{task_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting imaging cycle for task: {task_name} on microscope {allocated_microscope_id} with action_id: {action_id}")

        # Verify essential services (incubator, arm) are available - microscope_service is passed in and presumed connected by caller
        if not self.incubator or not self.robotic_arm:
            error_msg = f"Incubator or Robotic Arm not available for task {task_name} on microscope {allocated_microscope_id}."
            logger.error(error_msg)
            # Attempt to reconnect essential shared services.
            # We don't try to reconnect the specific microscope here as that's handled by run_time_lapse logic.
            await self.setup_connections() 
            if not self.incubator or not self.robotic_arm: # Check again
                raise Exception(f"Essential services still unavailable after reconnect attempt: {error_msg}")
        
        # Reset all task status on the services themselves before starting a new cycle
        try:
            logger.info(f"Resetting task statuses on services for task {task_name} (microscope: {allocated_microscope_id})...")
            logger.info(f"Service task statuses reset for task {task_name} on {allocated_microscope_id}.")
        except Exception as e:
            logger.error(f"Error resetting task statuses on services for {task_name} on {allocated_microscope_id}: {e}. Proceeding with caution.")

        try:
            # Pass allocated_microscope_id to transport operations
            await self._execute_load_operation(incubator_slot=incubator_slot, microscope_id_str=allocated_microscope_id)
            
            # Get well plate type from incubator service (read-only from incubator)
            try:
                well_plate_type = await self.incubator.get_well_plate_type(incubator_slot)
                logger.info(f"Retrieved well plate type '{well_plate_type}' for slot {incubator_slot} from incubator service.")
            except Exception as e:
                logger.error(f"Failed to get well plate type from incubator for slot {incubator_slot}: {e}. Using default '96'.")
                well_plate_type = "96"  # Default fallback
            
            # Mark as critical operation - microscope will be scanning
            self.in_critical_operation = True
            logger.info("CRITICAL OPERATION START: Microscope scanning well plate")
            # Only the target microscope should be treated as critical during scan
            try:
                self.critical_services.add(('microscope', allocated_microscope_id))
            except Exception:
                pass
            
            try:
                saved_data_type = task_config.get("saved_data_type", "raw_images_well_plate")
                
                logger.info(f"Building scan config for task {task_name}: saved_data_type={saved_data_type}")
                
                scan_config = {
                    "saved_data_type": saved_data_type,
                    "illumination_settings": task_config["illumination_settings"],
                    "do_contrast_autofocus": task_config["do_contrast_autofocus"],
                    "do_reflection_af": task_config["do_reflection_af"],
                    "action_ID": action_id,
                }
                
                if saved_data_type == "raw_images_well_plate":
                    scan_config.update({
                        "well_plate_type": well_plate_type,
                        "wells_to_scan": task_config["wells_to_scan"],
                        "Nx": task_config["Nx"],
                        "Ny": task_config["Ny"],
                        "dx": task_config["dx"],
                        "dy": task_config["dy"],
                    })
                    # Optional focus_map_points for raw_images_well_plate
                    if "focus_map_points" in task_config:
                        scan_config["focus_map_points"] = task_config["focus_map_points"]
                        logger.info(f"Focus map enabled with {len(task_config['focus_map_points'])} reference points")
                    logger.info(f"Well plate scan config: wells={task_config['wells_to_scan']}, Nx={task_config['Nx']}, Ny={task_config['Ny']}")
                else:
                    scan_config["positions"] = task_config.get("positions", [])
                    # Optional focus_map_points for raw_image_flexible
                    if "focus_map_points" in task_config:
                        scan_config["focus_map_points"] = task_config["focus_map_points"]
                        logger.info(f"Focus map enabled with {len(task_config['focus_map_points'])} reference points")
                    # Optional move_for_autofocus for raw_image_flexible
                    if "move_for_autofocus" in task_config:
                        scan_config["move_for_autofocus"] = task_config["move_for_autofocus"]
                        logger.info(f"Move for autofocus enabled: {task_config['move_for_autofocus']}")
                    logger.info(f"Flexible scan config: {len(scan_config['positions'])} positions")
                
                logger.info(f"Sending scan_config to microscope: saved_data_type={scan_config['saved_data_type']}")
                scan_result = await microscope_service.scan_start(config=scan_config)

                await self._poll_scan_status(
                    microscope_service=microscope_service,
                )
                
            finally:
                # Always reset critical operation flag after scanning
                self.in_critical_operation = False
                logger.info("CRITICAL OPERATION END: Microscope scan complete")
                # Unmark microscope critical state
                try:
                    self.critical_services.discard(('microscope', allocated_microscope_id))
                except Exception:
                    pass
            
            await self._execute_unload_operation(incubator_slot=incubator_slot, microscope_id_str=allocated_microscope_id)
            
            logger.info(f"Imaging cycle for task {task_name} on microscope {allocated_microscope_id} (action_id: {action_id}) completed successfully.")
            
        except Exception as e:
            logger.error(f"Cycle failed for task {task_name} on microscope {allocated_microscope_id}: {e}")
            logger.critical("CRITICAL: Cycle failed - NOT attempting automatic unload for safety reasons.")
            logger.critical("Manual intervention required. Check microscope and sample status before proceeding.")
            # DO NOT attempt cleanup unload - this is dangerous if scan is still running or timed out
            # Manual intervention is required to safely assess the situation
            raise # Re-raise the original exception

    async def run_microscope_only_cycle(self, task_config, microscope_service, allocated_microscope_id):
        """Run microscope-only scan without robotic arm/incubator. Supports raw_images_well_plate and raw_image_flexible."""
        task_name = task_config["name"]
        action_id = f"{task_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        saved_data_type = task_config.get("saved_data_type", "raw_images_well_plate")
        
        self.in_critical_operation = True
        self.critical_services.add(('microscope', allocated_microscope_id))
        
        try:
            scan_config = {
                "saved_data_type": saved_data_type,
                "illumination_settings": task_config["illumination_settings"],
                "do_contrast_autofocus": task_config["do_contrast_autofocus"],
                "do_reflection_af": task_config["do_reflection_af"],
                "action_ID": action_id,
            }
            
            if saved_data_type == "raw_images_well_plate":
                scan_config.update({
                    "well_plate_type": task_config.get("well_plate_type", "96"),
                    "wells_to_scan": task_config["wells_to_scan"],
                    "Nx": task_config["Nx"],
                    "Ny": task_config["Ny"],
                    "dx": task_config.get("dx", 0.8),
                    "dy": task_config.get("dy", 0.8),
                })
                # Optional focus_map_points for raw_images_well_plate
                if "focus_map_points" in task_config:
                    scan_config["focus_map_points"] = task_config["focus_map_points"]
            else:
                scan_config["positions"] = task_config.get("positions", [])
                # Optional focus_map_points for raw_image_flexible
                if "focus_map_points" in task_config:
                    scan_config["focus_map_points"] = task_config["focus_map_points"]
                # Optional move_for_autofocus for raw_image_flexible
                if "move_for_autofocus" in task_config:
                    scan_config["move_for_autofocus"] = task_config["move_for_autofocus"]
            
            await microscope_service.scan_start(config=scan_config)
            await self._poll_scan_status(microscope_service)
            
        finally:
            self.in_critical_operation = False
            self.critical_services.discard(('microscope', allocated_microscope_id))

    async def run_time_lapse(self):
        """Main orchestration loop to manage and run imaging tasks based on config.json."""
        logger.info("Orchestrator run_time_lapse started.")
        loop = asyncio.get_running_loop()
        last_config_read_time = 0.0

        while True:
            current_time_naive = datetime.now()

            if (loop.time() - last_config_read_time) > CONFIG_READ_INTERVAL:
                await self._load_and_update_tasks()
                last_config_read_time = loop.time()

            await self._reap_finished_cycle_tasks()

            if not self.tasks:
                logger.debug("No tasks loaded yet.")

            eligible_tasks_for_run = []

            for task_name, task_data in list(self.tasks.items()):
                if task_name in self._scheduled_cycle_tasks:
                    continue

                internal_config = task_data["config"]
                status = task_data["status"]
                pending_datetimes = internal_config.get("pending_datetimes", [])

                if status in ["completed", "error", "uploading", "paused"]:
                    continue
                
                if not pending_datetimes:
                    logger.debug(f"Task '{task_name}' skipped, no pending time points.")
                    if status not in ["completed", "uploading"]:
                        logger.warning(f"Task '{task_name}' has status '{status}' but no pending points. Marking completed.")
                        await self._update_task_state_and_write_config(task_name, status="completed")
                    continue

                earliest_tp_for_this_task = pending_datetimes[0]
                if current_time_naive >= earliest_tp_for_this_task:
                    eligible_tasks_for_run.append((task_name, earliest_tp_for_this_task))
                    logger.debug(f"Task '{task_name}' is eligible with TP: {earliest_tp_for_this_task.isoformat()}")
                else:
                    logger.debug(f"Task '{task_name}' not due yet (earliest TP: {earliest_tp_for_this_task.isoformat()}).")
            eligible_tasks_for_run.sort(key=lambda item: item[1])
            started_any = False
            for task_name, due_time in eligible_tasks_for_run:
                started = await self._start_due_task(task_name, due_time)
                started_any = started_any or started

            # Determine minimum wait time before next loop iteration
            min_wait_time = ORCHESTRATOR_LOOP_SLEEP
            next_potential_run_time = None
            for task_name, task_data_val in self.tasks.items():
                if task_name in self._scheduled_cycle_tasks:
                    continue
                if task_data_val["status"] not in ["completed", "error", "uploading", "paused"] and task_data_val["config"]["pending_datetimes"]:
                    earliest_tp = task_data_val["config"]["pending_datetimes"][0]
                    if next_potential_run_time is None or earliest_tp < next_potential_run_time:
                        next_potential_run_time = earliest_tp

            if next_potential_run_time and next_potential_run_time > current_time_naive:
                wait_seconds = (next_potential_run_time - current_time_naive).total_seconds()
                min_wait_time = max(0.1, min(wait_seconds, ORCHESTRATOR_LOOP_SLEEP))
                logger.debug(f"Calculated dynamic sleep: {min_wait_time:.2f}s until next potential task time ({next_potential_run_time.isoformat()})")
            elif eligible_tasks_for_run and not started_any:
                min_wait_time = 1.0

            if self._scheduled_cycle_tasks:
                min_wait_time = min(min_wait_time, 1.0)

            await asyncio.sleep(min_wait_time)

    async def cancel_running_cycles(self):
        """Cancel all running scheduled cycle tasks during shutdown."""
        if not self._scheduled_cycle_tasks:
            return

        logger.info(f"Cancelling {len(self._scheduled_cycle_tasks)} running scheduled cycle task(s)...")
        running_items = list(self._scheduled_cycle_tasks.items())
        for _, task in running_items:
            task.cancel()

        for task_name, task in running_items:
            try:
                await task
            except asyncio.CancelledError:
                logger.info(f"Scheduled cycle task '{task_name}' cancelled during shutdown.")
            except Exception as exc:
                logger.error(f"Scheduled cycle task '{task_name}' failed while shutting down: {exc}")

        self._scheduled_cycle_tasks.clear()
        self._active_task_names.clear()
        self._refresh_legacy_active_task_name()

    async def _register_self_as_hypha_service(self):
        registration_succeeded = False

        logger.info(
            f"Registering orchestrator as a Hypha service with ID '{self.orchestrator_hypha_service_id}' "
            f"on cloud server '{self.orchestrator_hypha_server_url}' in workspace '{self.workspace}'"
        )
        if self.token_for_orchestrator_registration:
            server_config_for_registration = {
                "server_url": self.orchestrator_hypha_server_url,
                "ping_interval": 30,
                "workspace": self.workspace,
                "token": self.token_for_orchestrator_registration,
            }

            self.orchestrator_hypha_server_connection = await connect_to_server(server_config_for_registration)
            logger.info(
                f"Successfully connected to Hypha server: {self.orchestrator_hypha_server_url} "
                "for orchestrator registration"
            )
            registered_service = await self.orchestrator_hypha_server_connection.register_service(
                self._build_service_api(),
                overwrite=True,
            )
            logger.info(
                "Orchestrator management service registered successfully on cloud Hypha. "
                f"Service ID: {registered_service.id}"
            )
            registration_succeeded = True
        else:
            logger.warning(
                "REEF_WORKSPACE_TOKEN is not set in environment. Skipping cloud orchestrator registration."
            )

        logger.info(
            f"Registering orchestrator on local Hypha server '{self.server_url}' in workspace "
            f"'{self.local_workspace or '<missing>'}'"
        )
        try:
            local_connection = await self._ensure_local_server_connection()
            if local_connection:
                registered_service = await local_connection.register_service(
                    self._build_service_api(),
                    overwrite=True,
                )
                logger.info(
                    "Orchestrator management service registered successfully on local Hypha. "
                    f"Service ID: {registered_service.id}"
                )
                registration_succeeded = True
        except Exception as exc:
            logger.error(f"Failed to register orchestrator service on local Hypha: {exc}")

        if not registration_succeeded:
            logger.warning("Orchestrator service was not registered on any Hypha endpoint.")

    @schema_function(skip_self=True)
    async def ping(self):
        """Returns pong for health checks."""
        logger.info("ping service method called.")
        return "pong"

    @schema_function(skip_self=True)
    async def add_imaging_task(self, task_definition: dict):
        """Adds/updates imaging task. scan_mode: 'full_automation' (default) or 'microscope_only'. For microscope_only, saved_data_type: 'raw_images_well_plate' or 'raw_image_flexible'. Optional focus_map_points: List[List[float]] with 3 reference points [[x,y,z], [x,y,z], [x,y,z]] in mm for focus interpolation. For raw_image_flexible, optional move_for_autofocus: bool to enable stage movement for autofocus."""
        logger.info(f"Attempting to add/update imaging task: {task_definition.get('name')}")
        if not isinstance(task_definition, dict) or "name" not in task_definition or "settings" not in task_definition:
            raise ValueError("Invalid task definition: must be a dict with 'name' and 'settings'.")

        task_name = task_definition["name"]
        new_settings = task_definition["settings"]

        scan_mode = new_settings.get("scan_mode", "full_automation")
        if scan_mode not in ["full_automation", "microscope_only"]:
            raise ValueError(f"Invalid scan_mode '{scan_mode}'. Must be 'full_automation' or 'microscope_only'.")
        
        common_required = ["allocated_microscope", "pending_time_points", "illumination_settings", "do_contrast_autofocus", "do_reflection_af"]
        
        # Check if saved_data_type is provided (for both modes now)
        saved_data_type = new_settings.get("saved_data_type")
        if not saved_data_type:
            raise ValueError(f"Missing required field 'saved_data_type' for task '{task_name}'. Must be 'raw_images_well_plate' or 'raw_image_flexible'.")
        if saved_data_type not in ["raw_images_well_plate", "raw_image_flexible"]:
            raise ValueError(f"Invalid saved_data_type '{saved_data_type}'. Must be 'raw_images_well_plate' or 'raw_image_flexible'.")
        
        if scan_mode == "full_automation":
            required_settings = common_required + ["incubator_slot"]
            if saved_data_type == "raw_images_well_plate":
                required_settings.extend(["wells_to_scan", "Nx", "Ny"])
            else:
                required_settings.append("positions")
        else:
            required_settings = common_required + ["saved_data_type"]
            if saved_data_type == "raw_images_well_plate":
                required_settings.extend(["wells_to_scan", "Nx", "Ny"])
            else:
                required_settings.append("positions")
        
        for req_field in required_settings:
            if req_field not in new_settings:
                raise ValueError(f"Missing required field '{req_field}' for task '{task_name}' (scan_mode: {scan_mode}, saved_data_type: {saved_data_type}).")
        
        if not isinstance(new_settings["pending_time_points"], list):
            raise ValueError(f"'pending_time_points' must be a list for task '{task_name}'.")

        for tp_str in new_settings["pending_time_points"]:
            try:
                datetime.fromisoformat(tp_str)
                if 'Z' in tp_str or '+' in tp_str.split('T')[-1]:
                    raise ValueError("Time point must be naive local time")
            except ValueError as ve:
                raise ValueError(f"Invalid time point '{tp_str}' for task '{task_name}': {ve}")
        
        if "imaged_time_points" not in new_settings:
            new_settings["imaged_time_points"] = []
        elif not isinstance(new_settings["imaged_time_points"], list):
            raise ValueError(f"'imaged_time_points' must be a list for task '{task_name}'.")
        
        for tp_str in new_settings["imaged_time_points"]:
            try:
                datetime.fromisoformat(tp_str)
                if 'Z' in tp_str or '+' in tp_str.split('T')[-1]:
                    raise ValueError("Time point must be naive local time")
            except ValueError as ve:
                raise ValueError(f"Invalid imaged_time_point '{tp_str}' for task '{task_name}': {ve}")
        
        if saved_data_type == "raw_image_flexible":
            positions = new_settings.get("positions", [])
            if not isinstance(positions, list) or len(positions) == 0:
                raise ValueError(f"'positions' must be a non-empty list for task '{task_name}'.")
            for idx, pos in enumerate(positions):
                if not isinstance(pos, dict) or "x" not in pos or "y" not in pos:
                    raise ValueError(f"Position {idx} must be a dict with 'x' and 'y' for task '{task_name}'.")
            
            # Validate move_for_autofocus if provided (optional for raw_image_flexible)
            if "move_for_autofocus" in new_settings:
                if not isinstance(new_settings["move_for_autofocus"], bool):
                    raise ValueError(f"'move_for_autofocus' must be a boolean for task '{task_name}'.")

        # Validate focus_map_points if provided (optional for both scan types)
        if "focus_map_points" in new_settings:
            focus_map_points = new_settings["focus_map_points"]
            if not isinstance(focus_map_points, list):
                raise ValueError(f"'focus_map_points' must be a list for task '{task_name}'.")
            if len(focus_map_points) != 3:
                raise ValueError(f"'focus_map_points' must contain exactly 3 reference points for task '{task_name}' (got {len(focus_map_points)}).")
            for idx, point in enumerate(focus_map_points):
                if not isinstance(point, list):
                    raise ValueError(f"Focus map point {idx} must be a list [x, y, z] for task '{task_name}'.")
                if len(point) != 3:
                    raise ValueError(f"Focus map point {idx} must have exactly 3 coordinates [x, y, z] for task '{task_name}' (got {len(point)}).")
                try:
                    # Validate that all elements are numeric
                    [float(coord) for coord in point]
                except (ValueError, TypeError):
                    raise ValueError(f"Focus map point {idx} must contain numeric coordinates [x, y, z] in mm for task '{task_name}'.")

        has_pending = bool(new_settings["pending_time_points"])
        has_imaged = bool(new_settings.get("imaged_time_points", []))

        current_status = "pending"
        if not has_pending:
            current_status = "completed"
        
        new_settings["imaging_completed"] = not has_pending
        new_settings["imaging_started"] = has_imaged or (not has_pending and has_imaged)

        async with self._config_lock:
            try:
                config_data = {"samples": []}
                try:
                    with open(CONFIG_FILE_PATH, 'r') as f:
                        config_data = json.load(f)
                except FileNotFoundError:
                    logger.warning(f"{CONFIG_FILE_PATH} not found. Will create a new one.")
                except (json.JSONDecodeError, OSError) as e:
                    logger.warning(f"{CONFIG_FILE_PATH} is corrupted or unreadable: {e}. Will create a new one.")
                
                if "samples" not in config_data or not isinstance(config_data["samples"], list):
                     config_data["samples"] = []

                task_exists_at_index = -1
                existing_task_status = None
                for i, existing_task in enumerate(config_data["samples"]):
                    if existing_task.get("name") == task_name:
                        task_exists_at_index = i
                        existing_task_status = existing_task.get("operational_state", {}).get("status", "pending")
                        break
                
                # For existing tasks, preserve "uploading" status if it was uploading
                final_status = current_status
                if task_exists_at_index != -1 and existing_task_status == "uploading" and not has_pending:
                    final_status = "uploading"  # Keep uploading status for existing tasks
                    logger.info(f"Task '{task_name}' is currently uploading. Preserving uploading status.")
                
                op_state = {
                    "status": final_status,
                    "last_updated_by_orchestrator": datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                }

                if task_exists_at_index != -1:
                    logger.info(f"Task '{task_name}' already exists. Updating its settings and operational_state.")
                    config_data["samples"][task_exists_at_index]["settings"] = new_settings
                    config_data["samples"][task_exists_at_index]["operational_state"] = op_state
                else:
                    logger.info(f"Adding new task '{task_name}'.")
                    new_task_entry = {
                        "name": task_name,
                        "settings": new_settings,
                        "operational_state": op_state
                    }
                    config_data["samples"].append(new_task_entry)

                with open(CONFIG_FILE_PATH_TMP, 'w') as f:
                    json.dump(config_data, f, indent=4)
                os.replace(CONFIG_FILE_PATH_TMP, CONFIG_FILE_PATH)
                logger.info(f"Task '{task_name}' processed (added/updated) in {CONFIG_FILE_PATH}.")

            except Exception as e:
                logger.error(f"Failed to add/update imaging task '{task_name}' in config: {e}", exc_info=True)
                return {"success": False, "message": f"Error processing task: {str(e)}"}

        await self._load_and_update_tasks() # Refresh orchestrator's internal task list
        return {"success": True, "message": f"Task '{task_name}' added/updated successfully."}

    @schema_function(skip_self=True)
    async def delete_imaging_task(self, task_name: str):
        """Deletes an imaging task from the configuration."""
        logger.info(f"Attempting to delete imaging task: {task_name}")
        if not task_name:
            return {"success": False, "message": "Task name cannot be empty."}
        if task_name in self._scheduled_cycle_tasks:
            return {
                "success": False,
                "message": f"Task '{task_name}' is currently running and cannot be deleted.",
                "state": "busy",
            }

        async with self._config_lock:
            try:
                config_data = {"samples": []}
                try:
                    with open(CONFIG_FILE_PATH, 'r') as f:
                        config_data = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError):
                    logger.warning(f"{CONFIG_FILE_PATH} not found or corrupted. Cannot delete task.")
                    return {"success": False, "message": f"{CONFIG_FILE_PATH} not found or corrupted."}

                if "samples" not in config_data or not isinstance(config_data["samples"], list):
                    logger.warning(f"No 'samples' list in {CONFIG_FILE_PATH}. Cannot delete task.")
                    return {"success": False, "message": "No 'samples' list in configuration."}

                original_count = len(config_data["samples"])
                config_data["samples"] = [task for task in config_data["samples"] if task.get("name") != task_name]
                
                if len(config_data["samples"]) == original_count:
                    logger.warning(f"Task '{task_name}' not found in {CONFIG_FILE_PATH}. No deletion occurred.")
                    return {"success": False, "message": f"Task '{task_name}' not found."}

                with open(CONFIG_FILE_PATH_TMP, 'w') as f:
                    json.dump(config_data, f, indent=4)
                os.replace(CONFIG_FILE_PATH_TMP, CONFIG_FILE_PATH)
                logger.info(f"Task '{task_name}' deleted from {CONFIG_FILE_PATH}.")

            except Exception as e:
                logger.error(f"Failed to delete imaging task '{task_name}' from config: {e}", exc_info=True)
                return {"success": False, "message": f"Error deleting task: {str(e)}"}
        
        await self._load_and_update_tasks() # Refresh orchestrator's internal task list
        return {"success": True, "message": f"Task '{task_name}' deleted successfully."}

    @schema_function(skip_self=True)
    async def pause_imaging_task(self, task_name: str):
        """Pauses an imaging task, preventing it from being processed until resumed."""
        if task_name not in self.tasks:
            return {"success": False, "message": f"Task '{task_name}' not found."}
        if task_name in self._scheduled_cycle_tasks:
            return {
                "success": False,
                "message": f"Task '{task_name}' is currently running and cannot be paused.",
                "state": "busy",
            }
        if self.tasks[task_name]["status"] == "paused":
            return {"success": True, "message": f"Task '{task_name}' is already paused."}
        await self._update_task_state_and_write_config(task_name, status="paused")
        return {"success": True, "message": f"Task '{task_name}' paused."}

    @schema_function(skip_self=True)
    async def resume_imaging_task(self, task_name: str):
        """Resumes a paused imaging task, allowing it to be processed again."""
        if task_name not in self.tasks:
            return {"success": False, "message": f"Task '{task_name}' not found."}
        if self.tasks[task_name]["status"] != "paused":
            return {"success": False, "message": f"Task '{task_name}' is not paused."}
        new_status = "completed" if not self.tasks[task_name]["config"].get("pending_datetimes") else "pending"
        await self._update_task_state_and_write_config(task_name, status=new_status)
        return {"success": True, "message": f"Task '{task_name}' resumed."}

    @schema_function(skip_self=True)
    async def get_all_imaging_tasks(self):
        """Retrieves all imaging task configurations from config.json."""
        logger.debug(f"Attempting to read all imaging tasks from {CONFIG_FILE_PATH}")
        async with self._config_lock:
            try:
                with open(CONFIG_FILE_PATH, 'r') as f:
                    config_data = json.load(f)
                return config_data.get("samples", []) # Return the list of samples
            except FileNotFoundError:
                logger.warning(f"{CONFIG_FILE_PATH} not found when trying to get all tasks.")
                return [] 
            except json.JSONDecodeError:
                logger.error(f"Error decoding JSON from {CONFIG_FILE_PATH} when getting all tasks.")
                return []
            except Exception as e:
                logger.error(f"Failed to get all imaging tasks: {e}", exc_info=True)
                return {"error": str(e), "success": False}

    @schema_function(skip_self=True)
    async def get_runtime_status(self):
        """Return a runtime snapshot for operator preflight checks and failure triage."""
        try:
            if not self.configured_microscopes_info and not self.tasks:
                await self._load_and_update_tasks()

            expected_microscopes = list(self.configured_microscopes_info.keys())
            local_services_ready = (
                self.incubator is not None
                and self.robotic_arm is not None
                and all(microscope_id in self.microscope_services for microscope_id in expected_microscopes)
            )
            if expected_microscopes and not local_services_ready:
                try:
                    await self.setup_connections()
                except Exception as setup_error:
                    logger.warning(f"Could not refresh local service proxies while building runtime status: {setup_error}")

            admission_snapshot = await self.admission_controller.snapshot()
            connected_microscopes = sorted(self.microscope_services.keys())
            sample_on_flags_per_microscope = {
                mic_id: self.sample_on_microscope_flags.get(mic_id, False)
                for mic_id in expected_microscopes
            }
            connected_services = {
                "incubator": self.incubator is not None,
                "robotic_arm": self.robotic_arm is not None,
                "hamilton": self.hamilton_executor is not None,
                "microscopes": {
                    mic_id: mic_id in self.microscope_services
                    for mic_id in expected_microscopes
                },
            }

            return {
                "success": True,
                "active_task": self.active_task_name,
                "active_tasks": sorted(self._active_task_names),
                "active_operations": admission_snapshot["active_operations"],
                "held_resources": admission_snapshot["held_resources"],
                "connected_services": connected_services,
                "connected_microscopes": connected_microscopes,
                "configured_microscopes": expected_microscopes,
                "critical_services": sorted(
                    [
                        {"service_type": service_type, "service_id": service_id}
                        for service_type, service_id in self.critical_services
                    ],
                    key=lambda item: (item["service_type"], item["service_id"]),
                ),
                "in_critical_operation": self.in_critical_operation,
                "sample_on_microscope_flags": sample_on_flags_per_microscope,
                "local_hypha": {
                    "server_url": self.server_url,
                    "workspace": self.local_workspace,
                    "connected": self.local_server_connection is not None,
                },
                "cloud_hypha": {
                    "server_url": self.orchestrator_hypha_server_url,
                    "workspace": self.workspace,
                    "connected": self.orchestrator_hypha_server_connection is not None,
                },
            }
        except Exception as e:
            logger.error(f"Failed to get runtime status: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    @schema_function(skip_self=True)
    async def cancel_microscope_scan(self, microscope_id: str):
        """Operator emergency API to cancel a running microscope scan.
        
        Note: scan_cancel() performs async cancellation. The scan may continue briefly
        in the background before stopping. The result message indicates whether the
        scan was stopped immediately or is stopping gracefully.
        """
        if microscope_id not in self.configured_microscopes_info:
            return {"success": False, "message": f"Unknown microscope ID '{microscope_id}'."}

        try:
            if microscope_id not in self.microscope_services:
                await self.setup_connections()
            microscope_service = self.microscope_services.get(microscope_id)
            if not microscope_service:
                raise Exception(f"Microscope service {microscope_id} is not available.")

            result = await microscope_service.scan_cancel()
            
            # Handle new async cancellation behavior
            # scan_cancel() may return a message indicating the scan is stopping in background
            result_msg = result.get("message", "") if isinstance(result, dict) else str(result)
            is_async_cancel = "stopping in the background" in result_msg.lower()
            
            if is_async_cancel:
                logger.info(f"Scan on {microscope_id} is stopping in the background. "
                           f"Result: {result_msg}")
            else:
                logger.info(f"Scan cancelled on {microscope_id}: {result_msg}")
            
            return {
                "success": True,
                "message": f"Cancel command sent to microscope {microscope_id}.",
                "result": result,
                "async_cancellation": is_async_cancel,
            }
        except Exception as e:
            logger.error(f"Failed to cancel scan on {microscope_id}: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    @schema_function(skip_self=True)
    async def halt_robotic_arm(self):
        """Operator emergency API to halt the robotic arm immediately."""
        try:
            if not self.robotic_arm:
                setup_ok = await self.setup_connections()
                if not setup_ok or not self.robotic_arm:
                    raise Exception("Robotic arm service is not available.")

            result = await self.robotic_arm.halt()
            return {
                "success": True,
                "message": "Robotic arm halt command sent.",
                "result": result,
            }
        except Exception as e:
            logger.error(f"Failed to halt robotic arm: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    @schema_function(skip_self=True)
    async def get_lab_video_stream_urls(self):
        """Returns public Hypha URLs for all lab video stream services (lab cameras)."""
        base = f"{self.orchestrator_hypha_server_url}/{self.workspace}/apps"
        return {
            "reef-lab-camera-1": f"{base}/reef-lab-camera-1",
            "reef-lab-camera-2": f"{base}/reef-lab-camera-2",
        }

    @schema_function(skip_self=True)
    async def process_timelapse_offline_api(self, experiment_id: str, upload_immediately: bool = True, cleanup_temp_files: bool = True):
        """API wrapper for offline stitching and upload timelapse functionality."""
        logger.info(f"API call: process_timelapse_offline for experiment_id: {experiment_id}")

        # Find matching tasks
        matching_tasks = [name for name in self.tasks.keys() if experiment_id in name]
        if not matching_tasks:
            return {"success": False, "message": f"No tasks found matching experiment_id: {experiment_id}"}

        allocated_microscopes = {
            self.tasks[task_name]["config"].get("allocated_microscope")
            for task_name in matching_tasks
            if task_name in self.tasks
        }
        allocated_microscopes.discard(None)

        if not allocated_microscopes:
            return {"success": False, "message": f"No allocated microscope found for experiment_id: {experiment_id}"}
        if len(allocated_microscopes) != 1:
            return {
                "success": False,
                "message": (
                    f"Experiment '{experiment_id}' spans multiple microscopes {sorted(allocated_microscopes)}. "
                    "Offline processing must be run per microscope."
                ),
            }

        microscope_id = next(iter(allocated_microscopes))
        request = self._build_request(
            "offline-processing",
            microscope_id=microscope_id,
            extra_resources=tuple(self._task_resource(task_name) for task_name in matching_tasks),
            metadata={"experiment_id": experiment_id},
        )

        try:
            if microscope_id not in self.microscope_services:
                logger.info(f"Microscope {microscope_id} not connected. Attempting to setup connections...")
                await self.setup_connections()

            microscope_service = self.microscope_services.get(microscope_id)
            if not microscope_service:
                raise Exception(f"Microscope service {microscope_id} is not available for offline processing")

            async with self.admission_controller.hold(request):
                for task_name in matching_tasks:
                    await self._update_task_state_and_write_config(task_name, status="uploading")

                result = await microscope_service.process_timelapse_offline(
                    experiment_id=experiment_id,
                    upload_immediately=upload_immediately,
                    cleanup_temp_files=cleanup_temp_files
                )

                for task_name in matching_tasks:
                    await self._update_task_state_and_write_config(task_name, status="completed")

                return {
                    "success": True,
                    "message": f"Offline processing completed for {len(matching_tasks)} tasks on {microscope_id}",
                    "result": result,
                    "microscope_id": microscope_id,
                }
        except ResourceBusyError as busy_error:
            message = (
                f"Offline processing for experiment_id '{experiment_id}' rejected because microscope "
                f"{microscope_id} or related tasks are busy."
            )
            logger.warning(message)
            return self._busy_response(message, busy_error)
        except Exception as e:
            logger.error(f"Offline processing failed for experiment_id {experiment_id}: {e}")
            # Set tasks to error status
            for task_name in matching_tasks:
                await self._update_task_state_and_write_config(task_name, status="error")
            return {"success": False, "message": str(e)}

    @schema_function(skip_self=True)
    async def scan_microscope_only_api(self, microscope_id: str, scan_config: dict,
                                       task_name: str = None, action_id: str = None):
        """
        API endpoint to run a scan directly on a microscope without load/unload operations.
        
        This bypasses robotic arm and incubator integration, assuming the sample is already
        manually placed on the microscope. Can optionally link to an existing task for status tracking.
        
        Args:
            microscope_id: ID of the microscope to use (e.g., 'microscope-squid-1')
            scan_config: Scan configuration dictionary containing:
                - saved_data_type: 'raw_images_well_plate' or 'raw_image_flexible'
                - For 'raw_images_well_plate':
                    - wells_to_scan: List[str] (e.g., ['A1', 'B2'])
                    - Nx, Ny: int (grid dimensions)
                    - dx, dy: float (position intervals in mm)
                    - well_plate_type: str (optional, default '96')
                    - focus_map_points: List[List[float]] (optional): 3 reference points [[x,y,z], [x,y,z], [x,y,z]] in mm for focus interpolation
                - For 'raw_image_flexible':
                    - positions: List[dict] with x, y, z, Nx, Ny, Nz, dx, dy, dz, name
                    - focus_map_points: List[List[float]] (optional): 3 reference points [[x,y,z], [x,y,z], [x,y,z]] in mm for focus interpolation
                    - move_for_autofocus: bool (optional): Whether to move stage for autofocus
                - illumination_settings: List[dict] (required for both)
                - do_contrast_autofocus: bool (required for both)
                - do_reflection_af: bool (required for both)
            task_name: Optional task name to link for status tracking
            action_id: Optional custom action ID (auto-generated if not provided)
            
        Returns:
            Dictionary with success status, message, and scan details
        """
        logger.info(f"API call: scan_microscope_only for microscope {microscope_id}, task_name={task_name}")
        
        # Validate microscope_id
        if microscope_id not in self.configured_microscopes_info:
            msg = f"Microscope ID '{microscope_id}' not found in configured microscopes."
            logger.error(msg)
            return {"success": False, "message": msg}
        
        # Validate scan_config
        if not isinstance(scan_config, dict):
            msg = "scan_config must be a dictionary"
            logger.error(msg)
            return {"success": False, "message": msg}
        
        # Validate required fields
        required_fields = ["saved_data_type", "illumination_settings", "do_contrast_autofocus", "do_reflection_af"]
        for field in required_fields:
            if field not in scan_config:
                msg = f"Missing required field '{field}' in scan_config"
                logger.error(msg)
                return {"success": False, "message": msg}
        
        saved_data_type = scan_config["saved_data_type"]
        if saved_data_type not in ["raw_images_well_plate", "raw_image_flexible"]:
            msg = f"Invalid saved_data_type '{saved_data_type}'. Must be 'raw_images_well_plate' or 'raw_image_flexible'"
            logger.error(msg)
            return {"success": False, "message": msg}
        
        # Validate type-specific fields
        if saved_data_type == "raw_images_well_plate":
            required_raw_images_fields = ["wells_to_scan", "Nx", "Ny", "dx", "dy"]
            for field in required_raw_images_fields:
                if field not in scan_config:
                    msg = f"Missing required field '{field}' for raw_images_well_plate scan type"
                    logger.error(msg)
                    return {"success": False, "message": msg}
        elif saved_data_type == "raw_image_flexible":
            if "positions" not in scan_config:
                msg = "Missing required field 'positions' for raw_image_flexible scan type"
                logger.error(msg)
                return {"success": False, "message": msg}
            if not isinstance(scan_config["positions"], list) or len(scan_config["positions"]) == 0:
                msg = "'positions' must be a non-empty list for raw_image_flexible scan type"
                logger.error(msg)
                return {"success": False, "message": msg}
            # Validate move_for_autofocus if provided (optional for raw_image_flexible)
            if "move_for_autofocus" in scan_config:
                if not isinstance(scan_config["move_for_autofocus"], bool):
                    msg = "'move_for_autofocus' must be a boolean for raw_image_flexible scan type"
                    logger.error(msg)
                    return {"success": False, "message": msg}
        
        # Validate focus_map_points if provided (optional for both scan types)
        if "focus_map_points" in scan_config:
            focus_map_points = scan_config["focus_map_points"]
            if not isinstance(focus_map_points, list):
                msg = "'focus_map_points' must be a list"
                logger.error(msg)
                return {"success": False, "message": msg}
            if len(focus_map_points) != 3:
                msg = f"'focus_map_points' must contain exactly 3 reference points (got {len(focus_map_points)})"
                logger.error(msg)
                return {"success": False, "message": msg}
            for idx, point in enumerate(focus_map_points):
                if not isinstance(point, list):
                    msg = f"Focus map point {idx} must be a list [x, y, z]"
                    logger.error(msg)
                    return {"success": False, "message": msg}
                if len(point) != 3:
                    msg = f"Focus map point {idx} must have exactly 3 coordinates [x, y, z] (got {len(point)})"
                    logger.error(msg)
                    return {"success": False, "message": msg}
                try:
                    # Validate that all elements are numeric
                    [float(coord) for coord in point]
                except (ValueError, TypeError):
                    msg = f"Focus map point {idx} must contain numeric coordinates [x, y, z] in mm"
                    logger.error(msg)
                    return {"success": False, "message": msg}
        
        # Ensure microscope service is connected
        try:
            if microscope_id not in self.microscope_services:
                logger.info(f"Microscope {microscope_id} not connected. Attempting to setup connections...")
                await self.setup_connections()
            
            microscope_service = self.microscope_services.get(microscope_id)
            if not microscope_service:
                raise Exception(f"Microscope service {microscope_id} is not available after connection attempt")
        except Exception as e:
            msg = f"Failed to connect to microscope {microscope_id}: {e}"
            logger.error(msg)
            return {"success": False, "message": msg}

        linked_task_name = None
        if task_name:
            if task_name not in self.tasks:
                logger.warning(f"Task '{task_name}' not found for linking")
            else:
                linked_task_name = task_name

        # Generate action_id if not provided
        if not action_id:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            action_id = f"microscope_only_scan_{microscope_id}_{timestamp}"

        request = self._build_request(
            "manual-scan",
            task_name=linked_task_name,
            microscope_id=microscope_id,
            metadata={"action_id": action_id, "saved_data_type": saved_data_type},
        )

        logger.info(f"Starting microscope-only scan with action_id: {action_id}")

        try:
            async with self.admission_controller.hold(request):
                if linked_task_name:
                    logger.info(f"Linking scan to task '{linked_task_name}', updating status to 'active'")
                    await self._update_task_state_and_write_config(linked_task_name, status="active")

                # Mark microscope as critical during scan
                self.in_critical_operation = True
                logger.info(f"CRITICAL OPERATION START: Microscope-only scan on {microscope_id}")
                try:
                    self.critical_services.add(('microscope', microscope_id))
                except Exception:
                    pass

                try:
                    # Build scan configuration based on saved_data_type
                    full_scan_config = {
                        "saved_data_type": saved_data_type,
                        "illumination_settings": scan_config["illumination_settings"],
                        "do_contrast_autofocus": scan_config["do_contrast_autofocus"],
                        "do_reflection_af": scan_config["do_reflection_af"],
                        "action_ID": action_id,
                    }

                    if saved_data_type == "raw_images_well_plate":
                        # Add well plate type (default to '96' if not provided)
                        full_scan_config["well_plate_type"] = scan_config.get("well_plate_type", "96")
                        full_scan_config["wells_to_scan"] = scan_config["wells_to_scan"]
                        full_scan_config["Nx"] = scan_config["Nx"]
                        full_scan_config["Ny"] = scan_config["Ny"]
                        full_scan_config["dx"] = scan_config["dx"]
                        full_scan_config["dy"] = scan_config["dy"]
                        # Optional focus_map_points for raw_images_well_plate
                        if "focus_map_points" in scan_config:
                            full_scan_config["focus_map_points"] = scan_config["focus_map_points"]
                    elif saved_data_type == "raw_image_flexible":
                        full_scan_config["positions"] = scan_config["positions"]
                        # Optional focus_map_points for raw_image_flexible
                        if "focus_map_points" in scan_config:
                            full_scan_config["focus_map_points"] = scan_config["focus_map_points"]
                        # Optional move_for_autofocus for raw_image_flexible
                        if "move_for_autofocus" in scan_config:
                            full_scan_config["move_for_autofocus"] = scan_config["move_for_autofocus"]

                    logger.info(f"Initiating scan with config: {full_scan_config}")

                    scan_result = await microscope_service.scan_start(config=full_scan_config)
                    logger.info(f"Scan initiated successfully: {scan_result}")

                    await self._poll_scan_status(
                        microscope_service=microscope_service,
                    )

                    logger.info(f"Microscope-only scan completed successfully for action_id: {action_id}")

                    if linked_task_name and linked_task_name in self.tasks:
                        logger.info(f"Updating linked task '{linked_task_name}' status to 'waiting_for_next_run'")
                        await self._update_task_state_and_write_config(linked_task_name, status="waiting_for_next_run")

                    return {
                        "success": True,
                        "message": f"Microscope-only scan completed successfully on {microscope_id}",
                        "action_id": action_id,
                        "microscope_id": microscope_id,
                        "scan_type": saved_data_type
                    }
                finally:
                    self.in_critical_operation = False
                    logger.info(f"CRITICAL OPERATION END: Microscope-only scan on {microscope_id}")
                    try:
                        self.critical_services.discard(('microscope', microscope_id))
                    except Exception:
                        pass
        except ResourceBusyError as busy_error:
            message = f"Microscope-only scan on {microscope_id} rejected because the microscope is busy."
            logger.warning(message)
            return self._busy_response(message, busy_error)
        except Exception as e:
            error_msg = f"Microscope-only scan failed: {e}"
            logger.error(error_msg, exc_info=True)
            
            # Update linked task to error status
            if linked_task_name and linked_task_name in self.tasks:
                await self._update_task_state_and_write_config(linked_task_name, status="error")
            
            return {"success": False, "message": error_msg, "action_id": action_id}

async def main():
    # parser = argparse.ArgumentParser(description='Run the Orchestration System.')
    # parser.add_argument('--local', action='store_true', help='Run in local mode using REEF_LOCAL_TOKEN and REEF_LOCAL_WORKSPACE')
    # args = parser.parse_args()
    
    # Initialize logger with fixed filename - will automatically rotate between orchestrator.log, orchestrator.log.1, etc.
    global logger
    logger = setup_logging(log_file="orchestrator.log")

    orchestrator = OrchestrationSystem()
    try:
        await orchestrator._register_self_as_hypha_service() # Register orchestrator's own Hypha service
        await orchestrator.run_time_lapse() # Removed round_time
    except KeyboardInterrupt:
        logger.info("Orchestrator shutting down due to KeyboardInterrupt...")
    finally:
        logger.info("Performing cleanup... disconnecting services.")
        if orchestrator:
            await orchestrator.cancel_running_cycles()
            if orchestrator.orchestrator_hypha_server_connection:
                try:
                    await orchestrator.orchestrator_hypha_server_connection.disconnect()
                    logger.info("Disconnected from Hypha server for orchestrator's own service.")
                except Exception as e:
                    logger.error(f"Error disconnecting orchestrator's Hypha service: {e}")
            await orchestrator.disconnect_services()
        logger.info("Cleanup complete. Orchestrator shutdown.")

if __name__ == '__main__':
    asyncio.run(main())
