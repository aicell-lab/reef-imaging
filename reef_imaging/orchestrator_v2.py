"""
This code is the orchestrator for the reef-imaging project.
Task: 
1. Load a plate from incubator to microscope
2. Scan the plate
3. Unload the plate from microscope to incubator
"""
import asyncio
import time
import base64
from IPython.display import Image, display
from hypha_rpc import connect_to_server, login
from hypha_rpc.utils.schema import schema_function
import os
import dotenv
import logging
import sys
import logging.handlers
from datetime import datetime, timezone, timedelta
import argparse
import json
import copy

# Set up logging
def setup_logging(log_file="orchestrator.log", max_bytes=10*1024*1024, backup_count=5):
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Rotating file handler
    file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

# add date and time to the log file name
# log_file = f"orchestrator-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
# logger = setup_logging(log_file=log_file)

dotenv.load_dotenv()
ENV_FILE = dotenv.find_dotenv()
if ENV_FILE:
    dotenv.load_dotenv(ENV_FILE)

CONFIG_FILE_PATH = "config.json"
CONFIG_READ_INTERVAL = 10 # Seconds to wait before re-reading config.json
ORCHESTRATOR_LOOP_SLEEP = 5 # Seconds to sleep in main loop when no immediate task is due

class OrchestrationSystem:
    def __init__(self, local=False):
        self.local = local
        self.server_url = "http://reef.dyn.scilifelab.se:9527" if local else "https://hypha.aicell.io"
        
        # Orchestrator's own Hypha service registration details
        self.orchestrator_hypha_server_url = "https://hypha.aicell.io"
        self.workspace = "reef-imaging" # Default workspace for aicell.io
        self.token_for_orchestrator_registration = os.environ.get("REEF_WORKSPACE_TOKEN")
        
        self.orchestrator_hypha_service_id = "orchestrator-manager"
        self.orchestrator_hypha_server_connection = None
        
        self.incubator = None
        self.microscope_services = {} # microscope_id -> service object
        self.configured_microscopes_info = {} # microscope_id -> config dict from config.json
        self.robotic_arm = None
        self.sample_on_microscope_flags = {} # microscope_id -> bool, True if sample on that microscope

        self.incubator_id = "incubator-control"
        self.robotic_arm_id = "robotic-arm-control"

        self.tasks = {} # Stores task configurations and states
        self.health_check_tasks = {} # Stores asyncio tasks for health checks, keyed by (service_type, service_id)
        self.active_task_name = None # Name of the task currently being processed or None
        self._config_lock = asyncio.Lock()

        # Transport Queue and Worker Task
        self.transport_queue = asyncio.Queue()
        self._transport_worker_task = None # Will be created in _register_self_as_hypha_service

    async def _start_health_check(self, service_type, service_instance, service_identifier=None): # MODIFIED signature
        key = (service_type, service_identifier) if service_identifier else service_type
        if key in self.health_check_tasks and not self.health_check_tasks[key].done():
            logger.info(f"Health check for {service_type} ({service_identifier if service_identifier else ''}) already running.")
            return
        logger.info(f"Starting health check for {service_type} ({service_identifier if service_identifier else ''})...")
        task = asyncio.create_task(self.check_service_health(service_instance, service_type, service_identifier)) # Pass identifier
        self.health_check_tasks[key] = task

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
        raw_config_data = None 

        async with self._config_lock:
            try:
                with open(CONFIG_FILE_PATH, 'r') as f:
                    raw_config_data = json.load(f) 
            except FileNotFoundError:
                logger.error(f"Configuration file {CONFIG_FILE_PATH} not found.")
                raw_config_data = {"samples": []} 
            except json.JSONDecodeError:
                logger.error(f"Error decoding JSON from {CONFIG_FILE_PATH}. Will not update tasks from file this cycle.")
                return 

        current_time_naive = datetime.now() # Used for intelligent flag setting, not scheduling here

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
                settings["imaging_completed"] = not has_pending
                settings["imaging_started"] = has_imaged or (not has_pending and has_imaged) # Started if imaged, or completed & imaged

                parsed_settings_config = {
                    "name": task_name,
                    "incubator_slot": settings["incubator_slot"],
                    "allocated_microscope": settings.get("allocated_microscope", "microscope-control-squid-1"),
                    "imaging_zone": settings["imaging_zone"],
                    "Nx": settings["Nx"],
                    "Ny": settings["Ny"],
                    "illuminate_channels": settings["illuminate_channels"],
                    "do_reflection_af": settings["do_reflection_af"],
                    "pending_datetimes": pending_datetimes, 
                    "imaged_datetimes": imaged_datetimes,
                    # The flags below are derived for internal logic if needed, but primary truth is pending/imaged_datetimes counts
                    "imaging_started_flag": settings["imaging_started"], 
                    "imaging_completed_flag": settings["imaging_completed"]
                }
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
            if self.active_task_name == task_name:
                logger.warning(f"Active task {task_name} was removed from config.")
                self.active_task_name = None 
            del self.tasks[task_name]

        a_task_state_changed_for_write = False
        for task_name, current_settings_config in new_task_configs.items():
            operational_state_from_file = {}
            for sample_in_file in raw_config_data.get("samples", []):
                if sample_in_file.get("name") == task_name:
                    operational_state_from_file = sample_in_file.get("operational_state", {})
                    break
            
            persisted_status = operational_state_from_file.get("status", "pending")

            # Determine actual status based on current pending_datetimes
            current_actual_status = persisted_status
            if not current_settings_config["pending_datetimes"]:
                current_actual_status = "completed"
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
                    "_raw_settings_from_input": copy.deepcopy(sample_config_from_file.get("settings", {}))
                }
                a_task_state_changed_for_write = True # Status might have been determined above

            else: # Task already exists, update it
                existing_task_data = self.tasks[task_name]
                # Check for config changes that might warrant a state reset
                config_changed_significantly = (
                    existing_task_data["config"]["pending_datetimes"] != current_settings_config["pending_datetimes"] or
                    existing_task_data["config"]["imaged_datetimes"] != current_settings_config["imaged_datetimes"] or
                    any(existing_task_data["config"].get(k) != current_settings_config.get(k) 
                        for k in ["incubator_slot", "allocated_microscope", "imaging_zone", "Nx", "Ny"])
                )

                existing_task_data["config"] = current_settings_config # Always update config
                existing_task_data["_raw_settings_from_input"] = copy.deepcopy(sample_config_from_file.get("settings", {}))
                a_task_state_changed_for_write = True # Assume config change implies write needed

                if existing_task_data["status"] != current_actual_status:
                    logger.info(f"Task '{task_name}' status changing from '{existing_task_data['status']}' to '{current_actual_status}' due to config load/re-evaluation.")
                    existing_task_data["status"] = current_actual_status

                if config_changed_significantly and existing_task_data["status"] not in ["pending", "completed"]:
                    if current_settings_config["pending_datetimes"]:
                        if existing_task_data["status"] != "pending":
                            logger.info(f"Task '{task_name}' had significant config changes while in status '{existing_task_data['status']}'. Resetting to pending as new points exist.")
                            existing_task_data["status"] = "pending"
                    elif not existing_task_data["config"]["imaged_datetimes"]:
                        # No pending, no imaged, but config changed? Should be completed. Or if no pending but imaged.
                         if existing_task_data["status"] != "completed":
                            logger.info(f"Task '{task_name}' had significant config changes. No pending points. Marking completed.")
                            existing_task_data["status"] = "completed"
            
            # Final status check: if a task somehow ends up with status != completed but no pending_datetimes, fix it.
            task_state_dict = self.tasks[task_name]
            if not task_state_dict["config"]["pending_datetimes"] and task_state_dict["status"] != "completed":
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
            except (FileNotFoundError, json.JSONDecodeError):
                 logger.warning(f"Could not re-read {CONFIG_FILE_PATH} before writing, or it was missing/corrupt. Will create/overwrite with current task data only.")

            for task_name, task_data_internal in self.tasks.items():
                settings_to_write = copy.deepcopy(task_data_internal.get("_raw_settings_from_input", {}))
                current_internal_config = task_data_internal["config"]

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
                with open(CONFIG_FILE_PATH, 'w') as f_write:
                    json.dump(output_config_data, f_write, indent=4)
            except IOError as e:
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

        # Update status based on pending points
        if not task_config_internal["pending_datetimes"]: 
            if task_state["status"] != "completed":
                logger.info(f"Task '{task_name}' has no more pending time points. Marking as completed.")
                task_state["status"] = "completed"
                changed = True
        elif status == "completed" and task_config_internal["pending_datetimes"]:
            logger.warning(f"Task '{task_name}' set to completed, but still has pending points. Reverting to pending.")
            task_state["status"] = "pending"
            changed = True

        if changed:
            await self._write_tasks_to_config()

    async def check_service_health(self, service, service_type, service_identifier=None): # MODIFIED signature
        """Check if the service is healthy and reset if needed"""
        # Use service_identifier for logging if available, otherwise fallback
        log_service_name_part = service_identifier if service_identifier else (service.id if hasattr(service, "id") else service_type)
        service_name = f"{service_type} ({log_service_name_part})"
            
        while True:
            try:
                # Get all task statuses
                task_statuses = await service.get_all_task_status()
                # Check if any task has failed
                if any(status == "failed" for status in task_statuses.values()):
                    logger.error(f"{service_name} service has failed tasks: {task_statuses}")
                    raise Exception("Service not healthy")

                # check hello_world
                hello_world_result = await service.hello_world()

                if hello_world_result != "Hello world": #also retry
                    logger.error(f"{service_name} service hello_world check failed: {hello_world_result}")
                    raise Exception("Service not healthy")
                
            except Exception as e:
                logger.error(f"{service_name} service health check failed: {e}")
                logger.info(f"Attempting to reset only the {service_type} service...")
                
                # Disconnect only the specific service
                await self.disconnect_single_service(service_type, service_identifier) # Pass identifier
                
                # Reconnect only the specific service
                await self.reconnect_single_service(service_type, service_identifier) # Pass identifier
                
            await asyncio.sleep(30)  # Check every half minute

    async def disconnect_single_service(self, service_type, service_id_to_disconnect=None): # MODIFIED signature
        """Disconnect a specific service and its health check."""
        # Determine the correct identifier for stopping health check and for logging
        actual_service_id = service_id_to_disconnect
        if service_type == 'incubator':
            actual_service_id = self.incubator_id
        elif service_type == 'robotic_arm':
            actual_service_id = self.robotic_arm_id
        
        if actual_service_id: # Only stop health check if we have a valid ID for it
             await self._stop_health_check(service_type, actual_service_id) # Stop health check first

        try:
            if service_type == 'incubator' and self.incubator:
                logger.info(f"Disconnecting incubator service ({self.incubator_id})...")
                await self.incubator.disconnect()
                self.incubator = None
                logger.info(f"Incubator service ({self.incubator_id}) disconnected.")
            elif service_type == 'microscope':
                if service_id_to_disconnect and service_id_to_disconnect in self.microscope_services:
                    logger.info(f"Disconnecting microscope service ({service_id_to_disconnect})...")
                    mic_service = self.microscope_services.pop(service_id_to_disconnect)
                    await mic_service.disconnect()
                    if service_id_to_disconnect in self.sample_on_microscope_flags: # Keep flag consistent
                        self.sample_on_microscope_flags[service_id_to_disconnect] = False 
                    logger.info(f"Microscope service ({service_id_to_disconnect}) disconnected.")
                elif not service_id_to_disconnect:
                    logger.warning("disconnect_single_service called for microscope without specifying ID. Cannot disconnect.")
            elif service_type == 'robotic_arm' and self.robotic_arm:
                reef_server = await connect_to_server({
                    "server_url": self.server_url,
                    "token": os.environ.get("REEF_LOCAL_TOKEN"),
                    "workspace": os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "reef-imaging",
                    "ping_interval": None
                })
                self.robotic_arm = await reef_server.get_service(self.robotic_arm_id)
                logger.info(f"Robotic arm service ({self.robotic_arm_id}) reconnected successfully.")
                await self._start_health_check('robotic_arm', self.robotic_arm) # Restart health check
                
        except Exception as e:
            logger.error(f"Error disconnecting {service_type} service ({service_id_to_disconnect if service_id_to_disconnect else ''}): {e}")

    async def reconnect_single_service(self, service_type, service_id_to_reconnect=None): # MODIFIED signature
        """Reconnect a specific service."""
        try:
            reef_token = os.environ.get("REEF_LOCAL_TOKEN") if self.local else os.environ.get("REEF_WORKSPACE_TOKEN")
            squid_token = os.environ.get("REEF_LOCAL_TOKEN") if self.local else os.environ.get("SQUID_WORKSPACE_TOKEN")
            
            if not reef_token or not squid_token:
                token = await login({"server_url": self.server_url})
                if not token: # check if login failed
                    logger.error(f"Hypha login failed during reconnect for {service_type} ({service_id_to_reconnect}). Cannot obtain token.")
                    return # Cannot proceed without token
                reef_token = token
                squid_token = token
            
            if service_type == 'incubator':
                if self.incubator: # Should ideally be disconnected first
                    logger.warning("Incubator already connected during reconnect attempt. Skipping.")
                    return
                reef_server = await connect_to_server({
                    "server_url": self.server_url,
                    "token": reef_token,
                    "workspace": os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "reef-imaging",
                    "ping_interval": None
                })
                self.incubator = await reef_server.get_service(self.incubator_id)
                logger.info(f"Incubator service ({self.incubator_id}) reconnected successfully.")
                await self._start_health_check('incubator', self.incubator, self.incubator_id)
                
            elif service_type == 'microscope':
                if not service_id_to_reconnect:
                    logger.error("Cannot reconnect microscope: service_id_to_reconnect is not provided.")
                    return
                if service_id_to_reconnect in self.microscope_services:
                    logger.warning(f"Microscope {service_id_to_reconnect} already connected during reconnect attempt. Skipping.")
                    return

                # Ensure this microscope ID is still in the current configuration
                if service_id_to_reconnect not in self.configured_microscopes_info:
                    logger.error(f"Cannot reconnect microscope {service_id_to_reconnect}: no longer in configuration.")
                    return

                squid_server = await connect_to_server({
                    "server_url": self.server_url,
                    "token": squid_token,
                    # Assuming squid-control workspace for all microscopes for now
                    "workspace": os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "squid-control", 
                    "ping_interval": None
                })
                microscope_service_instance = await squid_server.get_service(service_id_to_reconnect)
                self.microscope_services[service_id_to_reconnect] = microscope_service_instance
                logger.info(f"Microscope service ({service_id_to_reconnect}) reconnected successfully.")
                await self._start_health_check('microscope', microscope_service_instance, service_id_to_reconnect)
                
            elif service_type == 'robotic_arm':
                if self.robotic_arm: # Should ideally be disconnected first
                    logger.warning("Robotic arm already connected during reconnect attempt. Skipping.")
                    return
                reef_server = await connect_to_server({
                    "server_url": self.server_url,
                    "token": reef_token,
                    "workspace": os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "reef-imaging",
                    "ping_interval": None
                })
                self.robotic_arm = await reef_server.get_service(self.robotic_arm_id)
                logger.info(f"Robotic arm service ({self.robotic_arm_id}) reconnected successfully.")
                await self._start_health_check('robotic_arm', self.robotic_arm, self.robotic_arm_id)
                
        except Exception as e:
            logger.error(f"Error reconnecting {service_type} service ({service_id_to_reconnect if service_id_to_reconnect else ''}): {e}")

    async def setup_connections(self): # MODIFIED: target_microscope_id parameter removed
        """Set up connections to incubator, robotic arm, and all configured microscopes."""
        reef_token = os.environ.get("REEF_LOCAL_TOKEN") if self.local else os.environ.get("REEF_WORKSPACE_TOKEN")
        squid_token = os.environ.get("REEF_LOCAL_TOKEN") if self.local else os.environ.get("SQUID_WORKSPACE_TOKEN")
        
        if not reef_token or not squid_token:
            token = await login({"server_url": self.server_url})
            if not token:
                logger.error("Failed to login to Hypha server. Cannot setup connections.")
                return False
            reef_token = token
            squid_token = token

        # Connect to REEF services (Incubator, Robotic Arm)
        try:
            reef_server = await connect_to_server({
                "server_url": self.server_url,
                "token": reef_token,
                "workspace": os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "reef-imaging",
                "ping_interval": None
            })
            if not self.incubator:
                self.incubator = await reef_server.get_service(self.incubator_id)
                logger.info(f"Incubator ({self.incubator_id}) connected.")
                await self._start_health_check('incubator', self.incubator, self.incubator_id)
            if not self.robotic_arm:
                self.robotic_arm = await reef_server.get_service(self.robotic_arm_id)
                logger.info(f"Robotic arm ({self.robotic_arm_id}) connected.")
                await self._start_health_check('robotic_arm', self.robotic_arm, self.robotic_arm_id)
        except Exception as e:
            logger.error(f"Failed to connect to REEF services (incubator/robotic arm): {e}")
            return False # Critical failure

        # Connect to SQUID services (All Configured Microscopes)
        connected_microscope_count = 0
        if not self.configured_microscopes_info:
            logger.warning("No microscopes defined in the configuration (self.configured_microscopes_info is empty).")
        
        for mic_id, mic_config in self.configured_microscopes_info.items():
            if mic_id not in self.microscope_services: # If not already connected
                logger.info(f"Attempting to connect to microscope: {mic_id}...")
                try:
                    # Assuming squid-control workspace for all microscopes, adjust if needed per mic_config
                    squid_workspace = os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else mic_config.get("workspace", "squid-control") # Allow override from config
                    
                    squid_server_conn_for_mic = await connect_to_server({
                        "server_url": self.server_url, # Assuming all microscopes on the same Hypha server instance
                        "token": squid_token, # Assuming same token for all SQUID services
                        "workspace": squid_workspace,
                        "ping_interval": None
                    })
                    microscope_service_instance = await squid_server_conn_for_mic.get_service(mic_id)
                    self.microscope_services[mic_id] = microscope_service_instance
                    # Initialize sample on microscope flag if it wasn't (e.g. if config reloaded)
                    if mic_id not in self.sample_on_microscope_flags:
                        self.sample_on_microscope_flags[mic_id] = False
                    logger.info(f"Microscope {mic_id} connected.")
                    await self._start_health_check('microscope', microscope_service_instance, mic_id)
                    connected_microscope_count +=1
                except Exception as e:
                    logger.error(f"Failed to connect to microscope {mic_id}: {e}")
                    if mic_id in self.microscope_services: # Should not happen if connection failed, but as a safeguard
                        del self.microscope_services[mic_id]
                    # We don't return False here, allow orchestrator to run if other services are up.
            else:
                logger.info(f"Microscope {mic_id} already connected.")
                connected_microscope_count +=1
        
        # Disconnect any microscope services that are connected but no longer in configured_microscopes_info
        # This might happen if config is reloaded and a microscope is removed
        connected_ids = list(self.microscope_services.keys())
        for mid in connected_ids:
            if mid not in self.configured_microscopes_info:
                logger.info(f"Microscope {mid} is connected but no longer in configuration. Disconnecting.")
                await self.disconnect_single_service('microscope', mid)


        logger.info(f'Device connection setup process completed. Connected {connected_microscope_count}/{len(self.configured_microscopes_info)} configured microscopes.')
        # Return true if essential services (incubator, arm) are connected.
        # Individual tasks will check for their specific allocated microscope.
        return bool(self.incubator and self.robotic_arm)

    async def disconnect_services(self):
        """Disconnect from all services and stop their health checks."""
        logger.info("Disconnecting all services...")
        
        # Disconnect Incubator
        if self.incubator:
            await self.disconnect_single_service('incubator', self.incubator_id) 
        
        # Disconnect all Microscopes
        microscope_ids_to_disconnect = list(self.microscope_services.keys())
        for mic_id in microscope_ids_to_disconnect:
            await self.disconnect_single_service('microscope', mic_id)
        
        # Disconnect Robotic Arm
        if self.robotic_arm:
            await self.disconnect_single_service('robotic_arm', self.robotic_arm_id)
                
        logger.info("Disconnect process completed for all services.")

    async def load_plate_from_incubator_to_microscope_api(self, incubator_slot: int, microscope_id: str): # MODIFIED: added microscope_id
        logger.info(f"API call: Queuing load_plate_from_incubator_to_microscope for slot {incubator_slot} to microscope {microscope_id}")
        if microscope_id not in self.configured_microscopes_info:
            msg = f"Microscope ID '{microscope_id}' not found in configured microscopes."
            logger.error(msg)
            return {"success": False, "message": msg}

        op_future = asyncio.get_event_loop().create_future()
        await self.transport_queue.put({
            "action": "load",
            "incubator_slot": incubator_slot,
            "microscope_id": microscope_id, # Added microscope_id to queue item
            "future": op_future
        })
        #wait for load to be completed
        await op_future
        return {"success": True, "message": f"Load task for slot {incubator_slot} to microscope {microscope_id} queued."}

    async def _execute_load_operation(self, incubator_slot, microscope_id_str): # MODIFIED: added microscope_id_str
        target_microscope_service = self.microscope_services.get(microscope_id_str)
        if not target_microscope_service:
            error_msg = f"Failed to load: Microscope service {microscope_id_str} is not connected."
            logger.error(error_msg)
            raise Exception(error_msg)

        if self.sample_on_microscope_flags.get(microscope_id_str, False):
            logger.info(f"Sample plate already on microscope {microscope_id_str}")
            return 
            
        logger.info(f"Loading sample from incubator slot {incubator_slot} to microscope {microscope_id_str}...")
        
        try:
            # Determine the robot arm's target microscope ID (e.g., 1 or 2)
            # This logic might need to be more robust or configurable
            robot_microscope_target_id = 1 
            if microscope_id_str.endswith('2'):
                robot_microscope_target_id = 2
            elif microscope_id_str.endswith('1'):
                robot_microscope_target_id = 1
            # Add more sophisticated mapping if microscope IDs are not simply ending with 1 or 2
            else:
                logger.warning(f"Could not determine robot target ID for microscope {microscope_id_str}, defaulting to 1. This might be incorrect.")


            # Start parallel operations
            await asyncio.gather(
                self.incubator.get_sample_from_slot_to_transfer_station(incubator_slot),
                target_microscope_service.home_stage()
            )
            # Move sample with robotic arm
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await self.robotic_arm.incubator_to_microscope(robot_microscope_target_id) # Use derived robot_microscope_target_id
            
            # Return microscope stage
            await self.incubator.update_sample_location(incubator_slot, f"microscope{robot_microscope_target_id}") # Log with robot target ID
            await target_microscope_service.return_stage()
            
            logger.info(f"Sample loaded onto microscope {microscope_id_str}.")
            self.sample_on_microscope_flags[microscope_id_str] = True
            
        except Exception as e:
            error_msg = f"Failed to load sample from slot {incubator_slot} to microscope {microscope_id_str}: {e}"
            logger.error(error_msg)
            # Reset flag on failure if it was set prematurely or state is uncertain
            self.sample_on_microscope_flags[microscope_id_str] = False
            raise Exception(error_msg)

    async def unload_plate_from_microscope_api(self, incubator_slot: int, microscope_id: str): # MODIFIED: added microscope_id
        logger.info(f"API call: Queuing unload_plate_from_microscope for slot {incubator_slot} from microscope {microscope_id}")
        if microscope_id not in self.configured_microscopes_info:
            msg = f"Microscope ID '{microscope_id}' not found in configured microscopes."
            logger.error(msg)
            return {"success": False, "message": msg}

        op_future = asyncio.get_event_loop().create_future()
        await self.transport_queue.put({
            "action": "unload",
            "incubator_slot": incubator_slot,
            "microscope_id": microscope_id, # Added microscope_id to queue item
            "future": op_future
        })
        #wait for unload to be completed
        await op_future
        return {"success": True, "message": f"Unload task for slot {incubator_slot} from microscope {microscope_id} queued."}

    async def _execute_unload_operation(self, incubator_slot, microscope_id_str): # MODIFIED: added microscope_id_str
        target_microscope_service = self.microscope_services.get(microscope_id_str)
        if not target_microscope_service:
            error_msg = f"Failed to unload: Microscope service {microscope_id_str} is not connected."
            logger.error(error_msg)
            raise Exception(error_msg)

        if not self.sample_on_microscope_flags.get(microscope_id_str, False):
            logger.info(f"Sample plate not on microscope {microscope_id_str}")
            return 
            
        logger.info(f"Unloading sample to incubator slot {incubator_slot} from microscope {microscope_id_str}...")

        try:
            # Determine the robot arm's target microscope ID (e.g., 1 or 2)
            robot_microscope_target_id = 1
            if microscope_id_str.endswith('2'):
                robot_microscope_target_id = 2
            elif microscope_id_str.endswith('1'):
                robot_microscope_target_id = 1
            else:
                logger.warning(f"Could not determine robot target ID for microscope {microscope_id_str}, defaulting to 1. This might be incorrect.")

            # Home microscope stage
            await target_microscope_service.home_stage()
            
            # Move sample with robotic arm
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await self.robotic_arm.microscope_to_incubator(robot_microscope_target_id) # Use derived robot_microscope_target_id
            
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
            # State of sample_on_microscope_flags[microscope_id_str] is uncertain on failure, could leave as True or try to verify.
            # For now, we assume it might still be there if unload fails critically.
            raise Exception(error_msg)

    async def run_cycle(self, task_config, microscope_service, allocated_microscope_id): # MODIFIED: added microscope_service, allocated_microscope_id
        """Run the complete load-scan-unload process for a given task on a specific microscope."""
        task_name = task_config["name"]
        incubator_slot = task_config["incubator_slot"]
        action_id = f"{task_name.replace(' ', '_')}-{datetime.now().strftime('%Y%m%dT%H%M%S')}"
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
            await microscope_service.reset_all_task_status()
            if self.incubator: await self.incubator.reset_all_task_status()
            if self.robotic_arm: await self.robotic_arm.reset_all_task_status()
            logger.info(f"Service task statuses reset for task {task_name} on {allocated_microscope_id}.")
        except Exception as e:
            logger.error(f"Error resetting task statuses on services for {task_name} on {allocated_microscope_id}: {e}. Proceeding with caution.")

        # Ensure the specific microscope's sample flag is false before load, other microscopes' flags are untouched.
        self.sample_on_microscope_flags[allocated_microscope_id] = False 

        try:
            # Pass allocated_microscope_id to transport operations
            await self._execute_load_operation(incubator_slot=incubator_slot, microscope_id_str=allocated_microscope_id)
            
            # Scan with the provided microscope_service
            await microscope_service.scan_well_plate(
                illuminate_channels=task_config["illuminate_channels"],
                do_reflection_af=task_config["do_reflection_af"],
                scanning_zone=task_config["imaging_zone"],
                Nx=task_config["Nx"],
                Ny=task_config["Ny"],
                action_ID=action_id,
            )
            
            await self._execute_unload_operation(incubator_slot=incubator_slot, microscope_id_str=allocated_microscope_id)
            
            logger.info(f"Imaging cycle for task {task_name} on microscope {allocated_microscope_id} (action_id: {action_id}) completed successfully.")
            
        except Exception as e:
            logger.error(f"Cycle failed for task {task_name} on microscope {allocated_microscope_id}: {e}")
            # Attempt cleanup - unload if possible from the specific microscope
            try:
                logger.info(f"Attempting cleanup unload for task {task_name} from microscope {allocated_microscope_id} after cycle failure.")
                await self._execute_unload_operation(incubator_slot=incubator_slot, microscope_id_str=allocated_microscope_id)
                logger.info(f"Cleanup unload completed for task {task_name} from {allocated_microscope_id} after cycle failure.")
            except Exception as cleanup_error:
                logger.error(f"Cleanup unload also failed for task {task_name} from {allocated_microscope_id}: {cleanup_error}")
            
            raise # Re-raise the original exception that caused the cycle failure

    async def run_time_lapse(self):
        """Main orchestration loop to manage and run imaging tasks based on config.json."""
        logger.info("Orchestrator run_time_lapse started.")
        last_config_read_time = 0

        while True:
            current_time_naive = datetime.now()

            if (asyncio.get_event_loop().time() - last_config_read_time) > CONFIG_READ_INTERVAL:
                await self._load_and_update_tasks()
                last_config_read_time = asyncio.get_event_loop().time()

            next_task_to_run = None
            earliest_pending_tp_for_selection = None 

            if not self.tasks:
                logger.debug("No tasks loaded yet.")
            
            eligible_tasks_for_run = []

            for task_name, task_data in list(self.tasks.items()):
                internal_config = task_data["config"]
                status = task_data["status"]
                pending_datetimes = internal_config.get("pending_datetimes", [])

                if status in ["completed", "error"]:
                    continue
                
                if not pending_datetimes:
                    logger.debug(f"Task '{task_name}' skipped, no pending time points.")
                    if status != "completed":
                        logger.warning(f"Task '{task_name}' has status '{status}' but no pending points. Marking completed.")
                        await self._update_task_state_and_write_config(task_name, status="completed")
                    continue

                earliest_tp_for_this_task = pending_datetimes[0]
                if current_time_naive >= earliest_tp_for_this_task:
                    eligible_tasks_for_run.append((task_name, earliest_tp_for_this_task))
                    logger.debug(f"Task '{task_name}' is eligible with TP: {earliest_tp_for_this_task.isoformat()}")
                else:
                    logger.debug(f"Task '{task_name}' not due yet (earliest TP: {earliest_tp_for_this_task.isoformat()}).")
            
            # Select the task with the overall earliest time point from eligible tasks
            if eligible_tasks_for_run:
                eligible_tasks_for_run.sort(key=lambda x: x[1])
                next_task_to_run, earliest_pending_tp_for_selection = eligible_tasks_for_run[0]
                logger.info(f"Selected task '{next_task_to_run}' for TP: {earliest_pending_tp_for_selection.isoformat()} from eligible tasks.")

            if next_task_to_run and earliest_pending_tp_for_selection:
                self.active_task_name = next_task_to_run
                task_data = self.tasks[self.active_task_name]
                task_config_for_cycle = task_data["config"]
                current_pending_tp_to_process = earliest_pending_tp_for_selection
                
                allocated_microscope_id = task_config_for_cycle.get("allocated_microscope")
                if not allocated_microscope_id:
                    logger.error(f"Task {self.active_task_name} does not have an 'allocated_microscope'. Skipping.")
                    await self._update_task_state_and_write_config(self.active_task_name, status="error")
                    self.active_task_name = None
                    await asyncio.sleep(ORCHESTRATOR_LOOP_SLEEP) # Prevent rapid looping on misconfiguration
                    continue

                logger.info(f"Preparing to run task {self.active_task_name} for time point {current_pending_tp_to_process.isoformat()} on microscope {allocated_microscope_id}. Current state: status='{task_data['status']}'")

                # Ensure connections to shared services and the specific allocated microscope
                try:
                    # setup_connections now handles all configured microscopes.
                    # We must ensure it has run recently enough or run it if the allocated one is missing.
                    if not self.incubator or not self.robotic_arm or allocated_microscope_id not in self.microscope_services:
                         logger.info(f"Essential services or allocated microscope {allocated_microscope_id} not ready. Running setup_connections.")
                         await self.setup_connections() 
                except Exception as setup_error:
                    logger.error(f"Failed to setup/verify connections for task {self.active_task_name} (microscope {allocated_microscope_id}): {setup_error}")
                    await self._update_task_state_and_write_config(self.active_task_name, status="error")
                    self.active_task_name = None
                    await asyncio.sleep(ORCHESTRATOR_LOOP_SLEEP)
                    continue
                
                # After setup_connections, check again for the specific microscope
                target_microscope_service = self.microscope_services.get(allocated_microscope_id)
                if not target_microscope_service:
                    logger.error(f"Microscope {allocated_microscope_id} for task {self.active_task_name} is not available/connected even after setup_connections attempt.")
                    await self._update_task_state_and_write_config(self.active_task_name, status="error")
                    self.active_task_name = None
                    await asyncio.sleep(ORCHESTRATOR_LOOP_SLEEP)
                    continue

                logger.info(f"Starting cycle for task: {self.active_task_name} on microscope {allocated_microscope_id}, time point: {current_pending_tp_to_process.isoformat()}")
                await self._update_task_state_and_write_config(self.active_task_name, status="active")
                
                try:
                    # Pass the specific microscope service and its ID to run_cycle
                    await self.run_cycle(task_config_for_cycle, target_microscope_service, allocated_microscope_id) 
                    logger.info(f"Cycle for task {self.active_task_name} on {allocated_microscope_id}, time point {current_pending_tp_to_process.isoformat()} success.")
                    await self._update_task_state_and_write_config(
                        self.active_task_name,
                        status="waiting_for_next_run",
                        current_tp_to_move_to_imaged=current_pending_tp_to_process
                    )
                except Exception as cycle_error:
                    logger.error(f"Cycle for task {self.active_task_name} on {allocated_microscope_id}, time point {current_pending_tp_to_process.isoformat()} failed: {cycle_error}")
                    await self._update_task_state_and_write_config(
                        self.active_task_name,
                        status="error"
                    )

                self.active_task_name = None
            else:
                if self.active_task_name:
                    logger.warning("Active task was set but no task selected for run. Clearing active_task_name.")
                    self.active_task_name = None
                
                # Determine minimum wait time before next loop iteration
                min_wait_time = ORCHESTRATOR_LOOP_SLEEP
                next_potential_run_time = None
                for task_data_val in self.tasks.values():
                    if task_data_val["status"] not in ["completed", "error"] and task_data_val["config"]["pending_datetimes"]:
                        earliest_tp = task_data_val["config"]["pending_datetimes"][0]
                        if next_potential_run_time is None or earliest_tp < next_potential_run_time:
                            next_potential_run_time = earliest_tp
                
                if next_potential_run_time and next_potential_run_time > current_time_naive:
                    wait_seconds = (next_potential_run_time - current_time_naive).total_seconds()
                    min_wait_time = max(0.1, min(wait_seconds, ORCHESTRATOR_LOOP_SLEEP))
                    logger.debug(f"Calculated dynamic sleep: {min_wait_time:.2f}s until next potential task time ({next_potential_run_time.isoformat()})")

                await asyncio.sleep(min_wait_time)

    async def _transport_worker_loop(self):
        logger.info("Transport worker loop started.")
        while True:
            try:
                # Get a transport task from the queue
                task_details = await self.transport_queue.get()
                action = task_details.get("action")
                incubator_slot = task_details.get("incubator_slot")
                microscope_id_for_transport = task_details.get("microscope_id") # Get microscope_id
                future_to_resolve = task_details.get("future")

                logger.info(f"Transport worker picked up task: {action} for slot {incubator_slot} on microscope {microscope_id_for_transport}")
                
                if not microscope_id_for_transport:
                    error_msg = f"Transport task {action} for slot {incubator_slot} missing microscope_id."
                    logger.error(error_msg)
                    if future_to_resolve: future_to_resolve.set_exception(ValueError(error_msg))
                    self.transport_queue.task_done()
                    continue

                target_microscope_service = self.microscope_services.get(microscope_id_for_transport)
                if not target_microscope_service:
                    error_msg = f"Microscope service {microscope_id_for_transport} not connected for transport operation {action}."
                    logger.error(error_msg)
                    if future_to_resolve: future_to_resolve.set_exception(Exception(error_msg))
                    self.transport_queue.task_done()
                    continue
                
                try:
                    if action == "load":
                        await self._execute_load_operation(incubator_slot, microscope_id_for_transport) # Pass microscope_id
                        if future_to_resolve: future_to_resolve.set_result(True)
                    elif action == "unload":
                        await self._execute_unload_operation(incubator_slot, microscope_id_for_transport) # Pass microscope_id
                        if future_to_resolve: future_to_resolve.set_result(True)
                    else:
                        error_msg = f"Unknown transport action: {action}"
                        logger.warning(f"Transport worker received unknown action: {action}")
                        if future_to_resolve: future_to_resolve.set_exception(ValueError(error_msg))

                except Exception as e:
                    logger.error(f"Transport worker failed for {action} on slot {incubator_slot}, microscope {microscope_id_for_transport}: {e}")
                    if future_to_resolve and not future_to_resolve.done():
                        future_to_resolve.set_exception(e)
                
                self.transport_queue.task_done()
                logger.info(f"Transport task {action} for slot {incubator_slot}, microscope {microscope_id_for_transport} completed")

            except asyncio.CancelledError:
                logger.info("Transport worker loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Exception in transport worker loop: {e}", exc_info=True)

    async def _start_transport_worker(self):
        if self._transport_worker_task is None or self._transport_worker_task.done():
            self._transport_worker_task = asyncio.create_task(self._transport_worker_loop())
            logger.info("Transport worker task created and started.")
        else:
            logger.info("Transport worker task already running.")

    async def _stop_transport_worker(self):
        if self._transport_worker_task and not self._transport_worker_task.done():
            logger.info("Stopping transport worker task...")
            self.transport_queue.put_nowait(None)
            self._transport_worker_task.cancel()
            try:
                await self._transport_worker_task
            except asyncio.CancelledError:
                logger.info("Transport worker task successfully cancelled.")
            self._transport_worker_task = None
        else:
            logger.info("Transport worker task not running or already stopped.")

    async def _register_self_as_hypha_service(self):
        logger.info(f"Registering orchestrator as a Hypha service with ID '{self.orchestrator_hypha_service_id}' on server '{self.orchestrator_hypha_server_url}' in workspace '{self.workspace}'")
        if not self.token_for_orchestrator_registration:
            logger.error("REEF_WORKSPACE_TOKEN is not set in environment. Cannot register orchestrator service.")
            return

        server_config_for_registration = {
            "server_url": self.orchestrator_hypha_server_url,
            "ping_interval": None,
            "workspace": self.workspace,
            "token": self.token_for_orchestrator_registration
        }
        
        self.orchestrator_hypha_server_connection = await connect_to_server(server_config_for_registration)
        logger.info(f"Successfully connected to Hypha server: {self.orchestrator_hypha_server_url} for orchestrator registration")

        service_api = {
            "name": "Orchestrator Manager",
            "id": self.orchestrator_hypha_service_id,
            "config": {
                "visibility": "public", 
                "run_in_executor": True,
            },
            "hello_orchestrator": self.hello_orchestrator,
            "add_imaging_task": self.add_imaging_task,
            "delete_imaging_task": self.delete_imaging_task,
            "get_all_imaging_tasks": self.get_all_imaging_tasks,
            "load_plate_from_incubator_to_microscope": self.load_plate_from_incubator_to_microscope_api,
            "unload_plate_from_microscope": self.unload_plate_from_microscope_api,
            "get_transport_queue_status": self.get_transport_queue_status,
        }
        
        registered_service = await self.orchestrator_hypha_server_connection.register_service(service_api)
        logger.info(f"Orchestrator management service registered successfully. Service ID: {registered_service.id}")
        
        # Start the transport worker after successful registration
        await self._start_transport_worker()

    @schema_function(skip_self=True)
    async def hello_orchestrator(self):
        """Returns a hello message from the orchestrator."""
        logger.info("hello_orchestrator service method called.")
        return "Hello from the Orchestrator!"

    @schema_function(skip_self=True)
    async def add_imaging_task(self, task_definition: dict):
        """Adds a new imaging task to config.json or updates it if name exists."""
        logger.info(f"Attempting to add/update imaging task: {task_definition.get('name')}")
        if not isinstance(task_definition, dict) or "name" not in task_definition or "settings" not in task_definition:
            msg = "Invalid task definition: must be a dict with 'name' and 'settings'."
            logger.error(msg)
            return {"success": False, "message": msg}

        task_name = task_definition["name"]
        new_settings = task_definition["settings"]

        required_settings = ["incubator_slot", "allocated_microscope", "pending_time_points", "imaging_zone", "Nx", "Ny", "illuminate_channels", "do_reflection_af"]
        for req_field in required_settings:
            if req_field not in new_settings:
                msg = f"Missing required field '{req_field}' in settings for task '{task_name}'."
                logger.error(msg)
                return {"success": False, "message": msg}
        
        if not isinstance(new_settings["pending_time_points"], list):
            msg = f"'pending_time_points' must be a list for task '{task_name}'."
            logger.error(msg)
            return {"success": False, "message": msg}

        parsed_pending_time_points_str = []
        if not new_settings["pending_time_points"]:
            logger.warning(f"Task '{task_name}' has an empty 'pending_time_points' list.")
        
        for tp_str in new_settings["pending_time_points"]:
            try:
                datetime.fromisoformat(tp_str) # Validate naive ISO format
                if 'Z' in tp_str or '+' in tp_str.split('T')[-1]: # Basic check for unwanted timezone indicators
                    raise ValueError("Time point string should be naive local time.")
                parsed_pending_time_points_str.append(tp_str)
            except ValueError as ve:
                msg = f"Invalid naive ISO format for a time point in 'pending_time_points' for task '{task_name}': {tp_str} ({ve})"
                logger.error(msg)
                return {"success": False, "message": msg}
        
        # Ensure "imaged_time_points" exists and is a list, defaulting to empty if not provided
        if "imaged_time_points" not in new_settings:
            new_settings["imaged_time_points"] = []
        elif not isinstance(new_settings["imaged_time_points"], list):
            msg = f"'imaged_time_points' must be a list if provided for task '{task_name}'."
            logger.error(msg)
            return {"success": False, "message": msg}
        
        for tp_str in new_settings["imaged_time_points"]: # Also validate imaged if provided
            try:
                datetime.fromisoformat(tp_str)
                if 'Z' in tp_str or '+' in tp_str.split('T')[-1]:
                     raise ValueError("Time point string should be naive local time.")
            except ValueError as ve:
                msg = f"Invalid naive ISO format for a time point in 'imaged_time_points' for task '{task_name}': {tp_str} ({ve})"
                logger.error(msg)
                return {"success": False, "message": msg}

        # Determine status and flags based on the new/updated time points
        has_pending = bool(parsed_pending_time_points_str)
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
                except json.JSONDecodeError:
                    logger.warning(f"{CONFIG_FILE_PATH} is corrupted. Will create a new one.")
                
                if "samples" not in config_data or not isinstance(config_data["samples"], list):
                     config_data["samples"] = []

                task_exists_at_index = -1
                for i, existing_task in enumerate(config_data["samples"]):
                    if existing_task.get("name") == task_name:
                        task_exists_at_index = i
                        break
                
                op_state = {
                    "status": current_status,
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

                with open(CONFIG_FILE_PATH, 'w') as f:
                    json.dump(config_data, f, indent=4)
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

                with open(CONFIG_FILE_PATH, 'w') as f:
                    json.dump(config_data, f, indent=4)
                logger.info(f"Task '{task_name}' deleted from {CONFIG_FILE_PATH}.")

            except Exception as e:
                logger.error(f"Failed to delete imaging task '{task_name}' from config: {e}", exc_info=True)
                return {"success": False, "message": f"Error deleting task: {str(e)}"}
        
        await self._load_and_update_tasks() # Refresh orchestrator's internal task list
        return {"success": True, "message": f"Task '{task_name}' deleted successfully."}

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
                return {"error": "Failed to decode configuration file.", "success": False}
            except Exception as e:
                logger.error(f"Failed to get all imaging tasks: {e}", exc_info=True)
                return {"error": str(e), "success": False}

    @schema_function(skip_self=True)
    async def get_transport_queue_status(self):
        """Returns the current status of the transport queue and worker."""
        logger.debug("Getting transport queue status")
        try:
            # Get queue size
            queue_size = self.transport_queue.qsize()
            
            # Check if transport worker is running
            worker_running = (
                self._transport_worker_task is not None and 
                not self._transport_worker_task.done()
            )
            
            # Get worker status details
            worker_status = "stopped" # Default if not running or completed/cancelled/error
            if self._transport_worker_task is None:
                worker_status = "not_started"
            elif not self._transport_worker_task.done(): # Explicitly check if running
                 worker_status = "running"
            elif self._transport_worker_task.done():
                if self._transport_worker_task.cancelled():
                    worker_status = "cancelled"
                elif self._transport_worker_task.exception():
                    worker_status = "error"
                else: # Successfully completed its loop (should not happen for a continuous worker unless explicitly stopped)
                    worker_status = "completed_normally" 
            
            # Get sample on microscope flags for all configured microscopes
            sample_on_flags_per_microscope = {}
            for mic_id in self.configured_microscopes_info.keys():
                sample_on_flags_per_microscope[mic_id] = self.sample_on_microscope_flags.get(mic_id, False)

            status_info = {
                "queue_size": queue_size,
                "worker_running": worker_running, # This is a more direct interpretation
                "worker_detailed_status": worker_status, # More granular status
                "sample_on_microscope_flags": sample_on_flags_per_microscope, # Changed to flags per microscope
                "connected_microscopes": list(self.microscope_services.keys()), # List of connected microscope IDs
                "active_task": self.active_task_name
            }
            
            # Add worker exception info if there was an error
            if worker_status == "error" and self._transport_worker_task and self._transport_worker_task.done(): # Check done for safety
                try:
                    exception = self._transport_worker_task.exception()
                    status_info["worker_error"] = str(exception)
                except Exception: # Broad catch if .exception() itself fails or returns non-stringable
                    status_info["worker_error"] = "Unknown error retrieving exception details"
            
            logger.debug(f"Transport queue status: {status_info}")
            return status_info
            
        except Exception as e:
            logger.error(f"Failed to get transport queue status: {e}", exc_info=True)
            return {"error": str(e), "success": False}

async def main():
    parser = argparse.ArgumentParser(description='Run the Orchestration System.')
    parser.add_argument('--local', action='store_true', help='Run in local mode using REEF_LOCAL_TOKEN and REEF_LOCAL_WORKSPACE')
    args = parser.parse_args()
    
    # Initialize logger here after argument parsing, if args are needed for logging setup
    # For now, assuming log file name doesn't depend on args.
    log_file_name = f"orchestrator-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    global logger
    logger = setup_logging(log_file=log_file_name)

    orchestrator = OrchestrationSystem(local=args.local)
    try:
        await orchestrator._register_self_as_hypha_service() # Register orchestrator's own Hypha service
        await orchestrator.run_time_lapse() # Removed round_time
    except KeyboardInterrupt:
        logger.info("Orchestrator shutting down due to KeyboardInterrupt...")
    finally:
        logger.info("Performing cleanup... disconnecting services.")
        if orchestrator:
            await orchestrator._stop_transport_worker() # Stop the worker
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
