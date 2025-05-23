import os
import logging
import logging.handlers
import time
import argparse
import asyncio
import traceback
import dotenv
from hypha_rpc import login, connect_to_server
from typing import Optional, List

dotenv.load_dotenv()  
ENV_FILE = dotenv.find_dotenv()  
if ENV_FILE:  
    dotenv.load_dotenv(ENV_FILE)  

# Set up logging
def setup_logging(log_file="mirror_incubator_service.log", max_bytes=100000, backup_count=3):
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

logger = setup_logging()

class MirrorIncubatorService:
    def __init__(self):
        # Connection to cloud service
        self.cloud_server_url = "https://hypha.aicell.io"
        self.cloud_workspace = "reef-imaging"
        self.cloud_token = os.environ.get("REEF_WORKSPACE_TOKEN")
        self.cloud_service_id = "mirror-incubator-control"
        self.cloud_server = None
        
        # Connection to local service
        self.local_server_url = "http://reef.dyn.scilifelab.se:9527"
        self.local_token = os.environ.get("REEF_LOCAL_TOKEN")
        self.local_service_id = "incubator-control"
        self.local_server = None
        self.local_service = None

        # Task tracking
        self.setup_task = None
        self.task_status = {
            "connect_to_local_service": "not_started",
            "initialize": "not_started",
            "put_sample_from_transfer_station_to_slot": "not_started",
            "get_sample_from_slot_to_transfer_station": "not_started",
            "get_status": "not_started",
            "is_busy": "not_started",
            "reset_error_status": "not_started",
            "get_sample_status": "not_started",
            "get_temperature": "not_started",
            "get_co2_level": "not_started",
            "get_slot_information": "not_started",
            "update_sample_location": "not_started",
            "get_sample_location": "not_started"
        }

    async def connect_to_local_service(self):
        """Connect to the local incubator service"""
        task_name = "connect_to_local_service"
        self.task_status[task_name] = "started"
        try:
            self.local_server = await connect_to_server({
                "server_url": self.local_server_url, 
                "token": self.local_token,
                "ping_interval": None
            })
            
            # Connect to the local service
            self.local_service = await self.local_server.get_service(self.local_service_id)
            logger.info(f"Connected to local service {self.local_service_id}")
            self.task_status[task_name] = "finished"
            return True
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to connect to local service: {e}")
            return False

    async def check_service_health(self):
        """Check if the service is healthy and rerun setup if needed"""
        logger.info("Starting service health check task")
        while True:
            try:
                # Try to get the service status
                if self.cloud_service_id:
                    service = await self.cloud_server.get_service(self.cloud_service_id)
                    # Try a simple operation to verify service is working
                    hello_world_result = await asyncio.wait_for(service.hello_world(), timeout=10)
                    if hello_world_result != "Hello world":
                        logger.error(f"Service health check failed: {hello_world_result}")
                        raise Exception("Service not healthy")
                else:
                    logger.info("Service ID not set, waiting for service registration")
                    
                # Always check local service regardless of whether it's None
                try:
                    if self.local_service is None:
                        logger.info("Local service connection lost, attempting to reconnect")
                        await self.connect_to_local_service()
                        if self.local_service is None:
                            raise Exception("Failed to connect to local service")
                    
                    #logger.info("Checking local service health...")
                    local_hello_world_result = await asyncio.wait_for(self.local_service.hello_world(), timeout=10)
                    #logger.info(f"Local service response: {local_hello_world_result}")
                    
                    if local_hello_world_result != "Hello world":
                        logger.error(f"Local service health check failed: {local_hello_world_result}")
                        raise Exception("Local service not healthy")
                    
                    #logger.info("Local service health check passed")
                except Exception as e:
                    logger.error(f"Local service health check failed: {e}")
                    self.local_service = None  # Reset connection so it will reconnect next time
                    raise Exception(f"Local service not healthy: {e}")
            except Exception as e:
                logger.error(f"Service health check failed: {e}")
                logger.info("Attempting to rerun setup...")
                # Clean up Hypha service-related connections and variables
                try:
                    if self.cloud_server:
                        await self.cloud_server.disconnect()
                    if self.local_server:
                        await self.local_server.disconnect()
                    if self.setup_task:
                        self.setup_task.cancel()  # Cancel the previous setup task
                except Exception as disconnect_error:
                    logger.error(f"Error during disconnect: {disconnect_error}")
                finally:
                    self.cloud_server = None
                    self.local_server = None
                    self.local_service = None

                retry_count = 0
                while retry_count < 30:
                    try:
                        # Rerun the setup method
                        self.setup_task = asyncio.create_task(self.setup())
                        await self.setup_task
                        logger.info("Setup successful")
                        break  # Exit the loop if setup is successful
                    except Exception as setup_error:
                        logger.error(f"Failed to rerun setup: {setup_error}")
                        await asyncio.sleep(30)  # Wait before retrying
            
            await asyncio.sleep(10)  # Check more frequently (was 30)

    async def start_hypha_service(self, server):
        self.cloud_server = server
        svc = await server.register_service({
            "name": "Mirror Incubator Control",
            "id": self.cloud_service_id,
            "config": {
                "visibility": "public",
                "run_in_executor": True
            },
            "hello_world": self.hello_world,
            "initialize": self.initialize,
            "put_sample_from_transfer_station_to_slot": self.put_sample_from_transfer_station_to_slot,
            "get_sample_from_slot_to_transfer_station": self.get_sample_from_slot_to_transfer_station,
            "get_status": self.get_status,
            "is_busy": self.is_busy,
            "reset_error_status": self.reset_error_status,
            "get_sample_status": self.get_sample_status,
            "get_temperature": self.get_temperature,
            "get_co2_level": self.get_co2_level,
            "get_slot_information": self.get_slot_information,
            # Add new location-related functions
            "update_sample_location": self.update_sample_location,
            "get_sample_location": self.get_sample_location,
            # Add status functions
            "get_task_status": self.get_task_status,
            "get_all_task_status": self.get_all_task_status,
            "reset_task_status": self.reset_task_status,
            "reset_all_task_status": self.reset_all_task_status
        })

        logger.info(
            f"Mirror service (service_id={self.cloud_service_id}) started successfully, available at {self.cloud_server_url}/services"
        )

        logger.info(f'You can use this service using the service id: {svc.id}')
        id = svc.id.split(":")[1]

        logger.info(f"You can also test the service via the HTTP proxy: {self.cloud_server_url}/{server.config.workspace}/services/{id}")



    async def setup(self):
        # Connect to cloud workspace
        logger.info(f"Connecting to cloud workspace {self.cloud_workspace} at {self.cloud_server_url}")
        server = await connect_to_server({
            "server_url": self.cloud_server_url, 
            "token": self.cloud_token, 
            "workspace": self.cloud_workspace,
            "ping_interval": None
        })
        
        # Start the cloud service
        await self.start_hypha_service(server)
        
        # Connect to local service
        await self.connect_to_local_service()

    def hello_world(self):
        """Hello world"""
        return "Hello world"

    def get_task_status(self, task_name):
        """Get the status of a specific task"""
        return self.task_status.get(task_name, "unknown")
    
    def get_all_task_status(self):
        """Get the status of all tasks"""
        return self.task_status

    def reset_task_status(self, task_name):
        """Reset the status of a specific task"""
        if task_name in self.task_status:
            self.task_status[task_name] = "not_started"
    
    def reset_all_task_status(self):
        """Reset the status of all tasks"""
        for task_name in self.task_status:
            self.task_status[task_name] = "not_started"

    # Mirrored functions that call the local service
    async def initialize(self):
        """Mirror function for initialize"""
        task_name = "initialize"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.initialize()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to initialize: {e}")
            raise e

    async def put_sample_from_transfer_station_to_slot(self, slot: int = 5):
        """Mirror function for put_sample_from_transfer_station_to_slot"""
        task_name = "put_sample_from_transfer_station_to_slot"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.put_sample_from_transfer_station_to_slot(slot)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to put sample from transfer station to slot: {e}")
            raise e

    async def get_sample_from_slot_to_transfer_station(self, slot: int = 5):
        """Mirror function for get_sample_from_slot_to_transfer_station"""
        task_name = "get_sample_from_slot_to_transfer_station"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.get_sample_from_slot_to_transfer_station(slot)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get sample from slot to transfer station: {e}")
            raise e

    async def get_status(self):
        """Mirror function for get_status"""
        task_name = "get_status"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.get_status()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get status: {e}")
            raise e

    async def is_busy(self):
        """Mirror function for is_busy"""
        task_name = "is_busy"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.is_busy()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to check if busy: {e}")
            raise e

    async def reset_error_status(self):
        """Mirror function for reset_error_status"""
        task_name = "reset_error_status"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.reset_error_status()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to reset error status: {e}")
            raise e

    async def get_sample_status(self, slot: Optional[int] = None):
        """Mirror function for get_sample_status"""
        task_name = "get_sample_status"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.get_sample_status(slot)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get sample status: {e}")
            raise e

    async def get_temperature(self):
        """Mirror function for get_temperature"""
        task_name = "get_temperature"
        self.task_status[task_name] = "started"
        try:
            # if self.local_service is None:
            #     await self.connect_to_local_service()
            
            # result = await self.local_service.get_temperature()
            result = 37.0
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get temperature: {e}")
            raise e

    async def get_co2_level(self):
        """Mirror function for get_co2_level"""
        task_name = "get_co2_level"
        self.task_status[task_name] = "started"
        try:
            # if self.local_service is None:
            #     await self.connect_to_local_service()
            
            # result = await self.local_service.get_co2_level()
            result = 4.9
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get CO2 level: {e}")
            raise e

    async def get_slot_information(self, slot: Optional[int] = None):
        """Mirror function for get_slot_information"""
        task_name = "get_slot_information"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.get_slot_information(slot)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get slot information: {e}")
            raise e

    async def update_sample_location(self, slot: int = 5, location: str = "incubator_slot"):
        """Mirror function for update_sample_location"""
        task_name = "update_sample_location"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.update_sample_location(slot, location)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to update sample location: {e}")
            raise e
    
    async def get_sample_location(self, slot: Optional[int] = None):
        """Mirror function for get_sample_location"""
        task_name = "get_sample_location"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.get_sample_location(slot)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get sample location: {e}")
            raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Mirror service for Incubator control."
    )
    args = parser.parse_args()

    mirror_service = MirrorIncubatorService()

    loop = asyncio.get_event_loop()

    async def main():
        try:
            mirror_service.setup_task = asyncio.create_task(mirror_service.setup())
            await mirror_service.setup_task
            # Start the health check task
            asyncio.create_task(mirror_service.check_service_health())
        except Exception:
            traceback.print_exc()

    loop.create_task(main())
    loop.run_forever() 