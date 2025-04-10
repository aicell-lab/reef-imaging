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
from datetime import datetime
import argparse

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
log_file = f"orchestrator-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
logger = setup_logging(log_file=log_file)

dotenv.load_dotenv()
ENV_FILE = dotenv.find_dotenv()
if ENV_FILE:
    dotenv.load_dotenv(ENV_FILE)

# Configuration settings
IMAGING_INTERVAL = 3600  # Time between cycles in seconds
INCUBATOR_SLOT = 32  # Slot number in the incubator
ILLUMINATE_CHANNELS = ['BF LED matrix full', 'Fluorescence 488 nm Ex', 'Fluorescence 561 nm Ex']
SCANNING_ZONE = [(0, 0), (7, 9)] # the last 2 rows have no cells
Nx = 3
Ny = 3
ACTION_ID = '20250410-fucci-time-lapse-scan'

class OrchestrationSystem:
    def __init__(self, local=False):
        self.local = local
        self.server_url = "http://reef.dyn.scilifelab.se:9527" if local else "https://hypha.aicell.io"
        self.incubator = None
        self.microscope = None
        self.robotic_arm = None
        self.sample_loaded = False
        self.incubator_id = "incubator-control"
        self.microscope_id = "microscope-control-squid-1"
        self.robotic_arm_id = "robotic-arm-control"

    async def check_service_health(self, service):
        """Check if the service is healthy and reset if needed"""
        service_name = service.id if hasattr(service, "id") else "unknown"
        service_type = None
        
        # Determine which service this is
        if service == self.incubator:
            service_type = 'incubator'
        elif service == self.microscope:
            service_type = 'microscope'
        elif service == self.robotic_arm:
            service_type = 'robotic_arm'
        else:
            logger.error(f"Unknown service: {service_name}")
            return
            
        while True:
            try:
                # Get all task statuses
                task_statuses = await service.get_all_task_status()
                logger.info(f"{service_name} task statuses: {task_statuses}")

                # Check if any task has failed
                if any(status == "failed" for status in task_statuses.values()):
                    logger.error(f"{service_name} service has failed tasks: {task_statuses}")
                    raise Exception("Service not healthy")

                # check hello_world
                hello_world_result = await service.hello_world()

                if hello_world_result != "Hello world": #also retry
                    logger.error(f"{service_name} service hello_world check failed: {hello_world_result}")
                    raise Exception("Service not healthy")
                
                logger.info(f"{service_name} service is healthy")
            except Exception as e:
                logger.error(f"{service_name} service health check failed: {e}")
                logger.info(f"Attempting to reset only the {service_type} service...")
                
                # Disconnect only the specific service
                await self.disconnect_single_service(service_type)
                
                # Reconnect only the specific service
                await self.reconnect_single_service(service_type)
                
            await asyncio.sleep(30)  # Check every half minute

    async def disconnect_single_service(self, service_type):
        """Disconnect a specific service."""
        try:
            if service_type == 'incubator' and self.incubator:
                logger.info(f"Disconnecting incubator service...")
                await self.incubator.disconnect()
                self.incubator = None
                logger.info(f"Incubator service disconnected.")
            elif service_type == 'microscope' and self.microscope:
                logger.info(f"Disconnecting microscope service...")
                await self.microscope.disconnect()
                self.microscope = None
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
                
            elif service_type == 'microscope':
                squid_server = await connect_to_server({
                    "server_url": self.server_url,
                    "token": squid_token,
                    "workspace": os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "squid-control",
                    "ping_interval": None
                })
                self.microscope = await squid_server.get_service(self.microscope_id)
                logger.info(f"Microscope service reconnected successfully.")
                
            elif service_type == 'robotic_arm':
                reef_server = await connect_to_server({
                    "server_url": self.server_url,
                    "token": reef_token,
                    "workspace": os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "reef-imaging",
                    "ping_interval": None
                })
                self.robotic_arm = await reef_server.get_service(self.robotic_arm_id)
                logger.info(f"Robotic arm service reconnected successfully.")
                
        except Exception as e:
            logger.error(f"Error reconnecting {service_type} service: {e}")

    async def setup_connections(self):
        reef_token = os.environ.get("REEF_LOCAL_TOKEN") if self.local else os.environ.get("REEF_WORKSPACE_TOKEN")
        squid_token = os.environ.get("REEF_LOCAL_TOKEN") if self.local else os.environ.get("SQUID_WORKSPACE_TOKEN")
        
        if not reef_token or not squid_token:
            token = await login({"server_url": self.server_url})
            reef_token = token
            squid_token = token

        reef_server = await connect_to_server({
            "server_url": self.server_url,
            "token": reef_token,
            "workspace": os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "reef-imaging",
            "ping_interval": None
        })
        squid_server = await connect_to_server({
            "server_url": self.server_url,
            "token": squid_token,
            "workspace": os.environ.get("REEF_LOCAL_WORKSPACE") if self.local else "squid-control",
            "ping_interval": None
        })

        self.incubator = await reef_server.get_service(self.incubator_id)
        self.microscope = await squid_server.get_service(self.microscope_id)
        self.robotic_arm = await reef_server.get_service(self.robotic_arm_id)
        print('Connected to devices.')

        # Start health check tasks
        asyncio.create_task(self.check_service_health(self.incubator))
        asyncio.create_task(self.check_service_health(self.microscope))
        asyncio.create_task(self.check_service_health(self.robotic_arm))

    async def disconnect_services(self):
        """Disconnect from all services."""
        logger.info("Disconnecting all services...")
        
        # Disconnect each service individually, catching errors for each one
        if self.incubator:
            try:
                await self.incubator.disconnect()
                self.incubator = None
                logger.info("Incubator service disconnected successfully.")
            except Exception as e:
                logger.error(f"Error disconnecting incubator service: {e}")
        
        if self.microscope:
            try:
                await self.microscope.disconnect()
                self.microscope = None
                logger.info("Microscope service disconnected successfully.")
            except Exception as e:
                logger.error(f"Error disconnecting microscope service: {e}")
        
        if self.robotic_arm:
            try:
                await self.robotic_arm.disconnect()
                self.robotic_arm = None
                logger.info("Robotic arm service disconnected successfully.")
            except Exception as e:
                logger.error(f"Error disconnecting robotic arm service: {e}")
                
        logger.info("Disconnect process completed for all services.")

    async def call_service_with_retries(self, service_type, method_name, *args, max_retries=30, timeout=30, **kwargs):
        """
        Call a service method with retries, automatically using the most up-to-date service reference.
        service_type: string, one of 'incubator', 'microscope', 'robotic_arm'
        """
        if service_type == 'incubator':
            service = self.incubator
        elif service_type == 'microscope':
            service = self.microscope
        elif service_type == 'robotic_arm':
            service = self.robotic_arm
        else:
            raise ValueError(f"Unknown service type: {service_type}")
        
        retries = 0
        while retries < max_retries:
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

    async def load_plate_from_incubator_to_microscope(self, incubator_slot=INCUBATOR_SLOT):
        if self.sample_loaded:
            logger.info("Sample plate has already been loaded onto the microscope")
            return True

        logger.info(f"Loading sample from incubator slot {incubator_slot} to transfer station...")

        logger.info(f"Homing the microscope stage...")
        p1 = self.call_service_with_retries('incubator', "get_sample_from_slot_to_transfer_station", incubator_slot, timeout=60)
        p2 = self.call_service_with_retries('microscope', "home_stage", timeout=30)
        gather = await asyncio.gather(p1, p2)
        if not all(gather):
            return False

        logger.info(f"Grabbing sample from incubator...")
        if not await self.call_service_with_retries('robotic_arm', "grab_sample_from_incubator", timeout=120):
            return False

        logger.info(f"Transporting sample from incubator to microscope...")
        if not await self.call_service_with_retries('robotic_arm', "transport_from_incubator_to_microscope1", timeout=120):
            return False

        logger.info(f"Putting sample on microscope...")
        if not await self.call_service_with_retries('robotic_arm', "put_sample_on_microscope1", timeout=120):
            return False

        logger.info(f"Returning microscope stage to loading position...")
        if not await self.call_service_with_retries('microscope', "return_stage", timeout=30):
            return False

        logger.info("Sample plate successfully loaded onto microscope stage.")
        self.sample_loaded = True
        return True

    async def unload_plate_from_microscope(self, incubator_slot=INCUBATOR_SLOT):
        if not self.sample_loaded:
            logger.info("Sample plate is not on the microscope")
            return True

        logger.info(f"Homing the microscope stage...")
        if not await self.call_service_with_retries('microscope', "home_stage", timeout=30):
            return False

        logger.info(f"Grabbing sample from microscope...")
        if not await self.call_service_with_retries('robotic_arm', "grab_sample_from_microscope1", timeout=120):
            return False

        logger.info(f"Transporting sample from microscope to incubator...")
        if not await self.call_service_with_retries('robotic_arm', "transport_from_microscope1_to_incubator", timeout=120):
            return False

        logger.info(f"Putting sample on incubator...")
        if not await self.call_service_with_retries('robotic_arm', "put_sample_on_incubator", timeout=120):
            return False

        logger.info(f"Putting sample on incubator slot {incubator_slot}...")    
        logger.info(f"Returning microscope stage to loading position...")
        p1 = self.call_service_with_retries('incubator', "put_sample_from_transfer_station_to_slot", incubator_slot, timeout=60)
        p2 = self.call_service_with_retries('microscope', "return_stage", timeout=30)
        gather = await asyncio.gather(p1, p2)
        if not all(gather):
            return False

        logger.info("Sample successfully unloaded from the microscopy stage.")
        self.sample_loaded = False
        return True

    async def run_cycle(self):
        """Run the complete load-scan-unload process."""
        # Reset all task status
        self.microscope.reset_all_task_status()
        self.incubator.reset_all_task_status()
        self.robotic_arm.reset_all_task_status()

        if not await self.load_plate_from_incubator_to_microscope(incubator_slot=INCUBATOR_SLOT):
            logger.error("Failed to load sample - aborting cycle")
            return False

        if not await self.call_service_with_retries(
            'microscope',
            "scan_well_plate",
            illuminate_channels=ILLUMINATE_CHANNELS,
            do_reflection_af=True,
            scanning_zone=SCANNING_ZONE,
            Nx=Nx,
            Ny=Ny,
            action_ID=ACTION_ID,
            timeout=2400
        ):
            logger.error("Failed to complete microscope scanning")
            return False

        if not await self.unload_plate_from_microscope(incubator_slot=INCUBATOR_SLOT):
            logger.error("Failed to unload sample - manual intervention may be required")
            return False

        return True

    async def run_time_lapse(self, round_time=IMAGING_INTERVAL):
        """Run the cycle every hour (xxx seconds)."""
        while True:
            await self.setup_connections()
            start_time = asyncio.get_event_loop().time()
            logger.info("Starting new cycle...")
            success = await self.run_cycle()
            if success:
                logger.info("Cycle completed successfully")
            else:
                logger.warning("Cycle completed with errors")
                await self.disconnect_services()
                sys.exit(1)

            await self.disconnect_services()
            end_time = asyncio.get_event_loop().time()
            elapsed = end_time - start_time
            sleep_time = max(0, round_time - elapsed)
            logger.info(f"Elapsed time: {elapsed:.2f} seconds. Waiting {sleep_time:.2f} seconds until next cycle.")
            await asyncio.sleep(sleep_time)

async def main():
    parser = argparse.ArgumentParser(description='Run the Orchestration System.')
    parser.add_argument('--local', action='store_true', help='Run in local mode using REEF_LOCAL_TOKEN and REEF_LOCAL_WORKSPACE')
    args = parser.parse_args()

    orchestrator = OrchestrationSystem(local=args.local)
    await orchestrator.run_time_lapse(round_time=IMAGING_INTERVAL)

if __name__ == '__main__':
    asyncio.run(main())
