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
import os
import dotenv
import logging
import sys
import logging.handlers
from datetime import datetime, timezone, timedelta
import argparse
import json

# Set up logging
def setup_logging(log_file="orchestrator.log", max_bytes=10*1024*1024, backup_count=5):
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

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
CONFIG_READ_INTERVAL = 6 # Seconds to wait before re-reading config.json
MAX_CYCLE_RETRIES = 3
CYCLE_RETRY_DELAY = timedelta(minutes=1)
ORCHESTRATOR_LOOP_SLEEP = 10 # Seconds to sleep in main loop when no immediate task is due

class OrchestrationSystem:
    def __init__(self, local=False):
        self.local = local
        self.server_url = "http://reef.dyn.scilifelab.se:9527" if local else "https://hypha.aicell.io"
        self.incubator = None
        self.microscope = None # Will be set based on task
        self.robotic_arm = None
        self.sample_on_microscope_flag = False # Tracks if a sample (any sample) is currently on the microscope

        self.incubator_id = "incubator-control"
        # self.microscope_id = "microscope-control-squid-1" # Now dynamic from config
        self.current_microscope_id = None # ID of the currently connected microscope
        self.robotic_arm_id = "robotic-arm-control"

        self.tasks = {} # Stores task configurations and states
        self.health_check_tasks = {} # Stores asyncio tasks for health checks
        self.active_task_name = None # Name of the task currently being processed or None

    async def _start_health_check(self, service_type, service_instance):
        if service_type in self.health_check_tasks and not self.health_check_tasks[service_type].done():
            logger.info(f"Health check for {service_type} already running.")
            return
        logger.info(f"Starting health check for {service_type}...")
        task = asyncio.create_task(self.check_service_health(service_instance, service_type))
        self.health_check_tasks[service_type] = task

    async def _stop_health_check(self, service_type):
        if service_type in self.health_check_tasks:
            task = self.health_check_tasks.pop(service_type)
            if task and not task.done():
                logger.info(f"Stopping health check for {service_type}...")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.info(f"Health check for {service_type} cancelled.")

    async def _load_and_update_tasks(self):
        logger.info(f"Loading and updating tasks from {CONFIG_FILE_PATH}")
        new_task_configs = {}
        try:
            with open(CONFIG_FILE_PATH, 'r') as f:
                config_data = json.load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file {CONFIG_FILE_PATH} not found.")
            return
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {CONFIG_FILE_PATH}.")
            return

        for sample_config in config_data.get("samples", []):
            task_name = sample_config.get("name")
            if not task_name:
                logger.warning("Found a sample configuration without a name. Skipping.")
                continue

            try:
                settings = sample_config["settings"]
                # Convert time strings to timezone-aware datetime objects
                # Assuming 'Z' means UTC as per ISO 8601
                time_start_imaging_str = settings["time_start_imaging"]
                time_end_imaging_str = settings["time_end_imaging"]

                # Ensure 'Z' is treated as UTC
                if time_start_imaging_str.endswith('Z'):
                    time_start_imaging = datetime.fromisoformat(time_start_imaging_str[:-1] + '+00:00')
                else:
                    time_start_imaging = datetime.fromisoformat(time_start_imaging_str).astimezone(timezone.utc)
                
                if time_end_imaging_str.endswith('Z'):
                    time_end_imaging = datetime.fromisoformat(time_end_imaging_str[:-1] + '+00:00')
                else:
                    time_end_imaging = datetime.fromisoformat(time_end_imaging_str).astimezone(timezone.utc)

                parsed_config = {
                    "name": task_name,
                    "incubator_slot": settings["incubator_slot"],
                    "allocated_microscope": settings["allocated_microscope"],
                    "time_start_imaging": time_start_imaging,
                    "time_end_imaging": time_end_imaging,
                    "imaging_interval": timedelta(seconds=settings["imaging_interval"]),
                    "imaging_zone": settings["imaging_zone"],
                    "Nx": settings["Nx"],
                    "Ny": settings["Ny"],
                    "illuminate_channels": settings["illuminate_channels"],
                    "do_reflection_af": settings["do_reflection_af"]
                }
                new_task_configs[task_name] = parsed_config
            except KeyError as e:
                logger.error(f"Missing key {e} in configuration for sample {task_name}. Skipping.")
                continue
            except ValueError as e:
                logger.error(f"Error parsing configuration for sample {task_name}: {e}. Skipping.")
                continue
        
        # Update internal tasks state
        current_time = datetime.now(timezone.utc)
        
        # Remove tasks that are no longer in the config
        tasks_to_remove = [name for name in self.tasks if name not in new_task_configs]
        for task_name in tasks_to_remove:
            logger.info(f"Task {task_name} removed from configuration. Deactivating.")
            if self.active_task_name == task_name:
                # Handle stopping the active task if it's removed, potentially complicated
                # For now, just log and remove. Unloading plate might be needed.
                logger.warning(f"Active task {task_name} was removed from config. This might require manual intervention.")
                self.active_task_name = None 
            del self.tasks[task_name]

        # Add new tasks or update existing ones
        for task_name, config in new_task_configs.items():
            if task_name not in self.tasks:
                logger.info(f"New task added: {task_name}")
                self.tasks[task_name] = {
                    "config": config,
                    "status": "pending", # pending, active, waiting_for_next_run, completed, error
                    "next_run_time": config["time_start_imaging"], # Start with the task's start time
                    "retries": 0
                }
                # Ensure next_run_time is not in the past for new tasks unless it's within start/end
                if self.tasks[task_name]["next_run_time"] < current_time and current_time < config["time_end_imaging"]:
                   # If already past start time but before end time, schedule it "now" (or very soon)
                   # This ensures it gets picked up quickly if orchestrator starts mid-task
                   self.tasks[task_name]["next_run_time"] = current_time 
            else:
                # Task exists, update its configuration if necessary
                # Note: Changing times or microscope for an active task can be complex.
                # For now, a simple config update.
                if self.tasks[task_name]["config"] != config:
                    logger.info(f"Configuration for task {task_name} updated.")
                    self.tasks[task_name]["config"] = config
                    # If start time changed, it might affect next_run_time for pending tasks
                    if self.tasks[task_name]["status"] == "pending":
                         self.tasks[task_name]["next_run_time"] = config["time_start_imaging"]
                         if self.tasks[task_name]["next_run_time"] < current_time and current_time < config["time_end_imaging"]:
                            self.tasks[task_name]["next_run_time"] = current_time

    async def check_service_health(self, service, service_type):
        """Check if the service is healthy and reset if needed"""
        service_name = service.id if hasattr(service, "id") else f"{service_type}_service"
            
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
                await self.disconnect_single_service(service_type)
                
                # Reconnect only the specific service
                await self.reconnect_single_service(service_type)
                
            await asyncio.sleep(30)  # Check every half minute

    async def disconnect_single_service(self, service_type):
        """Disconnect a specific service and its health check."""
        await self._stop_health_check(service_type) # Stop health check first
        try:
            if service_type == 'incubator' and self.incubator:
                logger.info(f"Disconnecting incubator service...")
                await self.incubator.disconnect()
                self.incubator = None
                logger.info(f"Incubator service disconnected.")
            elif service_type == 'microscope' and self.microscope:
                logger.info(f"Disconnecting microscope service ({self.current_microscope_id})...")
                await self.microscope.disconnect()
                self.microscope = None
                self.current_microscope_id = None
                logger.info(f"Microscope service disconnected.")
            elif service_type == 'robotic_arm' and self.robotic_arm:
                logger.info(f"Disconnecting robotic_arm service...")
                await self.robotic_arm.disconnect()
                self.robotic_arm = None
                logger.info(f"Robotic arm service disconnected.")
        except Exception as e:
            logger.error(f"Error disconnecting {service_type} service: {e}")
    
    async def reconnect_single_service(self, service_type):
        """Reconnect a specific service."""
        try:
            reef_token = os.environ.get("REEF_LOCAL_TOKEN") if self.local else os.environ.get("REEF_WORKSPACE_TOKEN")
            squid_token = os.environ.get("REEF_LOCAL_TOKEN") if self.local else os.environ.get("SQUID_WORKSPACE_TOKEN")
            
            if not reef_token or not squid_token:
                token = await login({"server_url": self.server_url})
                reef_token = token
                squid_token = token
            
            if service_type == 'incubator':
                reef_server = await connect_to_server({
                    "server_url": self.server_url,
                    "token": reef_token,
                    "workspace": os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "reef-imaging",
                    "ping_interval": None
                })
                self.incubator = await reef_server.get_service(self.incubator_id)
                logger.info(f"Incubator service reconnected successfully.")
                await self._start_health_check('incubator', self.incubator) # Restart health check
                
            elif service_type == 'microscope':
                squid_server = await connect_to_server({
                    "server_url": self.server_url,
                    "token": squid_token,
                    "workspace": os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "squid-control", # This workspace might need to be dynamic too if multiple microscope providers
                    "ping_interval": None
                })
                # self.microscope = await squid_server.get_service(self.microscope_id) # self.microscope_id is not set globally
                if not self.current_microscope_id:
                    logger.error("Cannot reconnect microscope: current_microscope_id is not set.")
                    return
                self.microscope = await squid_server.get_service(self.current_microscope_id)
                logger.info(f"Microscope service ({self.current_microscope_id}) reconnected successfully.")
                await self._start_health_check('microscope', self.microscope) # Restart health check
                
            elif service_type == 'robotic_arm':
                reef_server = await connect_to_server({
                    "server_url": self.server_url,
                    "token": reef_token,
                    "workspace": os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "reef-imaging",
                    "ping_interval": None
                })
                self.robotic_arm = await reef_server.get_service(self.robotic_arm_id)
                logger.info(f"Robotic arm service reconnected successfully.")
                await self._start_health_check('robotic_arm', self.robotic_arm) # Restart health check
                
        except Exception as e:
            logger.error(f"Error reconnecting {service_type} service: {e}")

    async def setup_connections(self, target_microscope_id=None):
        """Set up connections to incubator, robotic arm, and the target microscope."""
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
                logger.info("Incubator connected.")
                await self._start_health_check('incubator', self.incubator)
            if not self.robotic_arm:
                self.robotic_arm = await reef_server.get_service(self.robotic_arm_id)
                logger.info("Robotic arm connected.")
                await self._start_health_check('robotic_arm', self.robotic_arm)
        except Exception as e:
            logger.error(f"Failed to connect to REEF services (incubator/robotic arm): {e}")
            return False # Critical failure

        # Connect to SQUID service (Microscope)
        if target_microscope_id:
            if self.current_microscope_id != target_microscope_id or not self.microscope:
                if self.microscope: # Connected to a different microscope
                    logger.info(f"Switching microscope from {self.current_microscope_id} to {target_microscope_id}")
                    await self.disconnect_single_service('microscope') # This will also stop its health check
                
                logger.info(f"Connecting to microscope: {target_microscope_id}...")
                try:
                    # Assuming squid_server might need re-establishing if tokens/workspaces are very different per microscope type
                    # For now, using the SQUID_WORKSPACE_TOKEN and squid-control workspace as per original
                    squid_workspace = os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "squid-control"
                    squid_server = await connect_to_server({
                        "server_url": self.server_url,
                        "token": squid_token,
                        "workspace": squid_workspace,
                        "ping_interval": None
                    })
                    self.microscope = await squid_server.get_service(target_microscope_id)
                    self.current_microscope_id = target_microscope_id
                    logger.info(f"Microscope {self.current_microscope_id} connected.")
                    await self._start_health_check('microscope', self.microscope)
                except Exception as e:
                    logger.error(f"Failed to connect to microscope {target_microscope_id}: {e}")
                    self.microscope = None
                    self.current_microscope_id = None
                    # Do not return False here, as other services might be up, and run_time_lapse can decide
            else:
                logger.info(f"Microscope {target_microscope_id} already connected.")
        else: # No target microscope ID specified, ensure any existing microscope connection is closed
            if self.microscope:
                logger.info(f"No target microscope specified. Disconnecting current microscope {self.current_microscope_id}.")
                await self.disconnect_single_service('microscope')

        logger.info('Device connection setup process completed.')
        # Return true if essential services (incubator, arm) are connected.
        # Microscope connection status is handled by the task scheduler.
        return bool(self.incubator and self.robotic_arm)

    async def disconnect_services(self):
        """Disconnect from all services and stop their health checks."""
        logger.info("Disconnecting all services...")
        
        service_types_to_disconnect = []
        if self.incubator: service_types_to_disconnect.append('incubator')
        if self.microscope: service_types_to_disconnect.append('microscope')
        if self.robotic_arm: service_types_to_disconnect.append('robotic_arm')

        for service_type in service_types_to_disconnect:
            await self.disconnect_single_service(service_type) # This now handles health checks too
                
        logger.info("Disconnect process completed for all services.")

    async def call_service_with_retries(self, service_type, method_name, *args, max_retries=30, timeout=30, **kwargs):
        """
        Call a service method with retries, automatically using the most up-to-date service reference.
        service_type: string, one of 'incubator', 'microscope', 'robotic_arm'
        """
        # Resolve the service instance dynamically at the beginning of each attempt
        # This ensures that if a service was reconnected (e.g. self.microscope changed),
        # the fresh instance is used.
        
        retries = 0
        while retries < max_retries:
            # Get the current service instance for this attempt
            service = None
            if service_type == 'incubator':
                service = self.incubator
            elif service_type == 'microscope':
                service = self.microscope
            elif service_type == 'robotic_arm':
                service = self.robotic_arm
            
            if not service:
                logger.error(f"Service {service_type} is not available. Retrying... ({retries + 1}/{max_retries})")
                retries += 1
                await asyncio.sleep(timeout) # Wait before retrying if service is None
                continue

            try:
                # Check the status of the task
                status = await service.get_task_status(method_name)
                logger.info(f"Task {method_name} status: {status}")
                if status == "failed":
                    message = f"Task {method_name} failed. Stopping execution."
                    logger.error(message)
                    return False

                if status == "not_started":
                    logger.info(f"Starting the task {method_name}...")
                    try:
                        await asyncio.wait_for(getattr(service, method_name)(*args, **kwargs), timeout=timeout)
                    except asyncio.TimeoutError:
                        logger.warning(f"Operation {method_name} timed out, but continuing to check status")
                        # Continue to the status checking loop below

                # Wait for the task to complete
                while True:
                    status = await service.get_task_status(method_name)
                    logger.info(f"Task {method_name} status: {status}")
                    if status == "finished":
                        logger.info(f"Task {method_name} completed successfully.")
                        await service.reset_task_status(method_name)
                        return True
                    elif status == "failed":
                        logger.error(f"Task {method_name} failed.")
                        return False
                    await asyncio.sleep(1)  # Check status every 5 seconds

            except Exception as e:
                logger.error(f"Error: {e}. Retrying... ({retries + 1}/{max_retries})")
            retries += 1
            await asyncio.sleep(timeout)

        logger.error(f"Max retries reached for task {method_name}. Terminating.")
        return False

    async def load_plate_from_incubator_to_microscope(self, incubator_slot):
        if self.sample_on_microscope_flag:
            logger.info("Sample plate has already been loaded onto the microscope")
            return True

        # Figure out which microscope we're currently connected to
        microscope_id = 1  # Default to microscope 1
        if self.current_microscope_id:
            # Extract the microscope ID from the service ID
            if self.current_microscope_id.endswith('2'):
                microscope_id = 2
            elif self.current_microscope_id.endswith('1'):
                microscope_id = 1
        
        logger.info(f"Loading sample from incubator slot {incubator_slot} to microscope {microscope_id}...")

        logger.info(f"Homing the microscope stage...")
        p1 = self.call_service_with_retries('incubator', "get_sample_from_slot_to_transfer_station", incubator_slot, timeout=60)
        p2 = self.call_service_with_retries('microscope', "home_stage", timeout=30)
        gather = await asyncio.gather(p1, p2)
        if not all(gather):
            return False

        # Use the new parameterized function
        logger.info(f"Moving sample from incubator to microscope {microscope_id}...")
        if not await self.call_service_with_retries('robotic_arm', "incubator_to_microscope", microscope_id, timeout=300):
            return False

        logger.info(f"Returning microscope stage to loading position...")
        if not await self.call_service_with_retries('microscope', "return_stage", timeout=30):
            return False

        logger.info("Sample plate successfully loaded onto microscope stage.")
        self.sample_on_microscope_flag = True
        return True

    async def unload_plate_from_microscope(self, incubator_slot):
        if not self.sample_on_microscope_flag:
            logger.info("Sample plate is not on the microscope")
            return True

        # Figure out which microscope we're currently connected to
        microscope_id = 1  # Default to microscope 1
        if self.current_microscope_id:
            # Extract the microscope ID from the service ID
            if self.current_microscope_id.endswith('2'):
                microscope_id = 2
            elif self.current_microscope_id.endswith('1'):
                microscope_id = 1

        logger.info(f"Homing the microscope stage...")
        if not await self.call_service_with_retries('microscope', "home_stage", timeout=30):
            return False

        # Use the new parameterized function
        logger.info(f"Moving sample from microscope {microscope_id} to incubator...")
        if not await self.call_service_with_retries('robotic_arm', "microscope_to_incubator", microscope_id, timeout=300):
            return False

        logger.info(f"Putting sample on incubator slot {incubator_slot}...")    
        logger.info(f"Returning microscope stage to loading position...")
        p1 = self.call_service_with_retries('incubator', "put_sample_from_transfer_station_to_slot", incubator_slot, timeout=60)
        p2 = self.call_service_with_retries('microscope', "return_stage", timeout=30)
        gather = await asyncio.gather(p1, p2)
        if not all(gather):
            return False

        logger.info("Sample successfully unloaded from the microscopy stage.")
        self.sample_on_microscope_flag = False
        return True

    async def run_cycle(self, task_config):
        """Run the complete load-scan-unload process for a given task."""
        task_name = task_config["name"]
        incubator_slot = task_config["incubator_slot"]
        action_id = f"{task_name.replace(' ', '_')}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%Z')}"
        logger.info(f"Starting imaging cycle for task: {task_name} with action_id: {action_id}")

        # Ensure all services required for the cycle are connected and healthy
        if not self.incubator or not self.microscope or not self.robotic_arm:
            logger.error(f"One or more services are not available for task {task_name}. Aborting cycle.")
            # Attempt to establish connections again before failing hard, might be part of a higher level retry
            if not await self.setup_connections(target_microscope_id=task_config["allocated_microscope"]):
                logger.error(f"Failed to re-establish service connections for task {task_name}. Cycle aborted.")
                return False
            if not self.microscope: # Check specifically for microscope after setup
                logger.error(f"Microscope {task_config['allocated_microscope']} still not available after setup attempt. Cycle aborted.")
                return False

        # Reset all task status on the services themselves before starting a new cycle
        try:
            logger.info(f"Resetting task statuses on services for task {task_name}...")
            if self.microscope: await self.microscope.reset_all_task_status()
            if self.incubator: await self.incubator.reset_all_task_status()
            if self.robotic_arm: await self.robotic_arm.reset_all_task_status()
            logger.info(f"Service task statuses reset for task {task_name}.")
        except Exception as e:
            logger.error(f"Error resetting task statuses on services for {task_name}: {e}. Proceeding with caution.")

        # Reset sample_on_microscope_flag based on actual state if possible, or assume it needs loading
        self.sample_on_microscope_flag = False 

        if not await self.load_plate_from_incubator_to_microscope(incubator_slot=incubator_slot):
            logger.error(f"Failed to load sample for task {task_name} - aborting cycle")
            # Note: Plate might be in a transit state. Unloading is not attempted here as load failed.
            return False

        scan_successful = await self.call_service_with_retries(
            'microscope',
            "scan_well_plate",
            illuminate_channels=task_config["illuminate_channels"],
            do_reflection_af=task_config["do_reflection_af"],
            scanning_zone=task_config["imaging_zone"],
            Nx=task_config["Nx"],
            Ny=task_config["Ny"],
            action_ID=action_id,
            timeout=2400 # TODO: Consider making this configurable per task
        )

        if not scan_successful:
            logger.error(f"Microscope scanning failed for task {task_name}.")
            logger.info(f"Attempting to unload plate for task {task_name} after scan failure.")
            if not await self.unload_plate_from_microscope(incubator_slot=incubator_slot):
                logger.error(f"Failed to unload sample for task {task_name} after scan error - manual intervention may be required")
            return False # Cycle failed due to scan error

        if not await self.unload_plate_from_microscope(incubator_slot=incubator_slot):
            logger.error(f"Failed to unload sample for task {task_name} after successful scan - manual intervention may be required")
            return False

        logger.info(f"Imaging cycle for task {task_name} (action_id: {action_id}) completed successfully.")
        return True

    async def run_time_lapse(self):
        """Main orchestration loop to manage and run imaging tasks based on config.json."""
        logger.info("Orchestrator run_time_lapse started.")
        last_config_read_time = 0

        while True:
            current_time = datetime.now(timezone.utc)

            # Periodically load/update tasks from config
            if (asyncio.get_event_loop().time() - last_config_read_time) > CONFIG_READ_INTERVAL:
                await self._load_and_update_tasks()
                last_config_read_time = asyncio.get_event_loop().time()

            next_task_to_run = None
            earliest_next_run = None

            # Find the next eligible task
            for task_name, task_data in list(self.tasks.items()): # list() for safe iteration if dict changes
                config = task_data["config"]
                status = task_data["status"]
                next_run_time = task_data["next_run_time"]

                if status == "completed" or status == "error_max_retries":
                    continue # Skip tasks that are done or have hit max errors
                
                # Check if task is within its overall active window
                if current_time >= config["time_end_imaging"]:
                    logger.info(f"Task {task_name} has passed its end time ({config['time_end_imaging']}). Marking as completed.")
                    self.tasks[task_name]["status"] = "completed"
                    if self.active_task_name == task_name:
                         self.active_task_name = None # Clear active task if it's this one
                    continue

                if current_time >= config["time_start_imaging"] and current_time >= next_run_time:
                    if earliest_next_run is None or next_run_time < earliest_next_run:
                        earliest_next_run = next_run_time
                        next_task_to_run = task_name
            
            if next_task_to_run:
                self.active_task_name = next_task_to_run
                task_data = self.tasks[self.active_task_name]
                task_config = task_data["config"]
                logger.info(f"Selected task {self.active_task_name} for execution. Next scheduled run: {task_data['next_run_time']}")

                # Ensure connections are up for this task, especially the correct microscope
                if not await self.setup_connections(target_microscope_id=task_config["allocated_microscope"]):
                    logger.error(f"Failed to setup connections for task {self.active_task_name}. Retrying task later.")
                    self.tasks[self.active_task_name]["status"] = "error_connection_failed"
                    self.tasks[self.active_task_name]["next_run_time"] = current_time + CYCLE_RETRY_DELAY
                    self.active_task_name = None
                    await asyncio.sleep(ORCHESTRATOR_LOOP_SLEEP)
                    continue
                
                if not self.microscope or self.current_microscope_id != task_config["allocated_microscope"]:
                    logger.error(f"Microscope {task_config['allocated_microscope']} still not available after setup attempt. Cycle aborted.")
                    self.tasks[self.active_task_name]["status"] = "error_microscope_unavailable"
                    self.tasks[self.active_task_name]["next_run_time"] = current_time + CYCLE_RETRY_DELAY
                    self.active_task_name = None
                    await asyncio.sleep(ORCHESTRATOR_LOOP_SLEEP)
                    continue

                logger.info(f"Starting cycle for task: {self.active_task_name}")
                self.tasks[self.active_task_name]["status"] = "active"
                cycle_success = await self.run_cycle(task_config)

                if cycle_success:
                    logger.info(f"Cycle for task {self.active_task_name} completed successfully.")
                    self.tasks[self.active_task_name]["next_run_time"] = current_time + task_config["imaging_interval"]
                    self.tasks[self.active_task_name]["status"] = "waiting_for_next_run"
                    self.tasks[self.active_task_name]["retries"] = 0 # Reset retries on success
                else:
                    logger.error(f"Cycle for task {self.active_task_name} failed.")
                    task_data["retries"] += 1
                    if task_data["retries"] >= MAX_CYCLE_RETRIES:
                        logger.error(f"Max retries ({MAX_CYCLE_RETRIES}) reached for task {self.active_task_name}. Marking as error.")
                        self.tasks[self.active_task_name]["status"] = "error_max_retries"
                        # Optionally, could set next_run_time far in the future or require manual reset
                    else:
                        logger.info(f"Scheduling retry {task_data['retries']}/{MAX_CYCLE_RETRIES} for task {self.active_task_name} after delay.")
                        self.tasks[self.active_task_name]["next_run_time"] = current_time + CYCLE_RETRY_DELAY
                        self.tasks[self.active_task_name]["status"] = "error_cycle_failed"
                
                self.active_task_name = None # Clear active task after processing

            else: # No task currently due
                if self.active_task_name:
                     # This case should ideally not be hit if active_task_name is cleared after processing
                     logger.info(f"No task immediately due, but {self.active_task_name} was active. Clearing.")
                     self.active_task_name = None
                
                # If there are tasks but none are due, find the earliest next run time to sleep until (or for ORCHESTRATOR_LOOP_SLEEP)
                min_wait_time = ORCHESTRATOR_LOOP_SLEEP
                if self.tasks:
                    next_possible_run = None
                    for task_name, task_data in self.tasks.items():
                        config = task_data["config"]
                        status = task_data["status"]
                        if status not in ["completed", "error_max_retries"] and current_time < config["time_end_imaging"]:
                            potential_next_run = max(task_data["next_run_time"], config["time_start_imaging"])
                            if next_possible_run is None or potential_next_run < next_possible_run:
                                next_possible_run = potential_next_run
                    
                    if next_possible_run:
                        wait_duration = (next_possible_run - current_time).total_seconds()
                        min_wait_time = max(0, min(wait_duration, ORCHESTRATOR_LOOP_SLEEP))
                
                logger.debug(f"No tasks immediately due. Sleeping for {min_wait_time:.2f} seconds.")
                await asyncio.sleep(min_wait_time)

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
        await orchestrator.run_time_lapse() # Removed round_time
    except KeyboardInterrupt:
        logger.info("Orchestrator shutting down due to KeyboardInterrupt...")
    finally:
        logger.info("Performing cleanup... disconnecting services.")
        # Ensure disconnect_services is awaitable if it needs to be
        # It is already async, so await is correct.
        if orchestrator: # orchestrator might not be initialized if error occurs before
            await orchestrator.disconnect_services()
        logger.info("Cleanup complete. Orchestrator shutdown.")

if __name__ == '__main__':
    asyncio.run(main())
