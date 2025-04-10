import asyncio
import argparse
import os
from hypha_rpc import connect_to_server, login
from cytomat import Cytomat
from pydantic import Field
from hypha_rpc.utils.schema import schema_function
from typing import Optional
import dotenv
import json
import logging
import logging.handlers
import time

dotenv.load_dotenv()  
ENV_FILE = dotenv.find_dotenv()  
if ENV_FILE:  
    dotenv.load_dotenv(ENV_FILE)  


ERROR_CODES = {
    0: "No error",
    1: "Motor communication disrupted",
    2: "Plate not mounted on shovel",
    3: "Plate not dropped from shovel",
    4: "Shovel not extended",
    5: "Procedure timeout",
    6: "Transfer door not opened",
    7: "Transfer door not closed",
    8: "Shovel not retracted",
    10: "Step motor temperature too high",
    11: "Other step motor error",
    12: "Transfer station not rotated",
    13: "Heating or CO2 communication disrupted",
    14: "Shaker communication disrupted",
    15: "Shaker configuration out of order",
    16: "Shaker not started",
    19: "Shaker clamp not open",
    20: "Shaker clamp not closed",
    255: "Critical"
}

# Set up logging

def setup_logging(log_file="incubator_service.log", max_bytes=100000, backup_count=3):
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

class IncubatorService:
    def __init__(self, local, simulation=False):
        self.local = local
        self.simulation = simulation
        self.server_url = "http://localhost:9527" if local else "https://hypha.aicell.io"
        self.c = Cytomat("/dev/ttyUSB0") if not simulation else None
        self.samples_file = "/home/tao/workspace/reef-imaging/reef_imaging/control/cytomat-control/samples.json"
        self.server = None
        self.service_id = "incubator-control" + ("-simulation" if simulation else "")
        self.setup_task = None
        # Add task status tracking
        self.task_status = {
            "initialize": "not_started",
            "put_sample_from_transfer_station_to_slot": "not_started",
            "get_sample_from_slot_to_transfer_station": "not_started",
            "get_status": "not_started",
            "is_busy": "not_started",
            "reset_error_status": "not_started",
            "get_sample_status": "not_started",
            "get_temperature": "not_started",
            "get_co2_level": "not_started",
            "get_slot_information": "not_started"
        }

    async def check_service_health(self):
        """Check if the service is healthy and rerun setup if needed"""
        while True:
            try:
                # Try to get the service status
                if self.service_id:
                    service = await self.server.get_service(self.service_id)
                    await service.hello_world()
                    # Try a simple operation to verify service is working
                    #await service.get_status()
                    #print("Service health check passed")
                else:
                    logger.info("Service ID not set, waiting for service registration")
            except Exception as e:
                logger.error(f"Service health check failed: {e}")
                logger.info("Attempting to rerun setup...")
                # Clean up Hypha service-related connections and variables
                try:
                    if self.server:
                        await self.server.disconnect()
                        self.server = None
                    if self.setup_task:
                        self.setup_task.cancel()  # Cancel the previous setup task
                        self.setup_task = None
                except Exception as disconnect_error:
                    logger.error(f"Error during disconnect: {disconnect_error}")
                finally:
                    self.server = None

                while True:
                    try:
                        # Rerun the setup method
                        self.setup_task = asyncio.create_task(self.setup())
                        await self.setup_task
                        logger.info("Setup successful")
                        break  # Exit the loop if setup is successful
                    except Exception as setup_error:
                        logger.error(f"Failed to rerun setup: {setup_error}")
                        await asyncio.sleep(30)  # Wait before retrying
            
            await asyncio.sleep(30)  # Check every half minute

    async def start_hypha_service(self, server):
        self.server = server
        svc = await server.register_service({
            "name": "Incubator Control",
            "id": self.service_id,  # Use the defined service ID
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
            # Add status functions
            "get_task_status": self.get_task_status,
            "get_all_task_status": self.get_all_task_status,
            "reset_task_status": self.reset_task_status,
            "reset_all_task_status": self.reset_all_task_status
        })

        logger.info(f"Incubator control service registered at workspace: {server.config.workspace}, id: {svc.id}")
        logger.info(f'You can use this service using the service id: {svc.id}')
        id = svc.id.split(":")[1]
        logger.info(f"You can also test the service via the HTTP proxy: {self.server_url}/{server.config.workspace}/services/{id}/initialize")

        # Start the health check task
        asyncio.create_task(self.check_service_health())

    async def setup(self):
        if not self.simulation:
            self.c.maintenance_controller.reset_error_status()
        if self.local:
            token = os.environ.get("REEF_LOCAL_TOKEN")
            server = await connect_to_server({"server_url": self.server_url, "token": token, "ping_interval": None})
        else:
            try:
                token = os.environ.get("REEF_WORKSPACE_TOKEN")
            except:
                token = await login({"server_url": self.server_url})
            server = await connect_to_server({"server_url": self.server_url, "token": token, "workspace": "reef-imaging", "ping_interval": None})

        await self.start_hypha_service(server)

    def get_task_status(self, task_name):
        """Get the status of a specific task"""
        try:
            status = self.task_status.get(task_name, "unknown")
            logger.info(f"Task {task_name} status: {status}")
            return status
        except Exception as e:
            logger.error(f"Error getting task status for {task_name}: {e}")
            return "unknown"
    
    @schema_function(skip_self=True)
    def get_all_task_status(self):
        """Get the status of all tasks"""
        logger.info(f"Task status: {self.task_status}")
        return self.task_status
    
    def reset_task_status(self, task_name):
        """Reset the status of a specific task"""
        if task_name in self.task_status:
            self.task_status[task_name] = "not_started"

    def reset_all_task_status(self):
        """Reset the status of all tasks"""
        for task_name in self.task_status:
            self.task_status[task_name] = "not_started"

    @schema_function(skip_self=True)
    def initialize(self):
        """
        Clean up error status and initialize the incubator
        Returns:A string message        
        """
        task_name = "initialize"
        self.task_status[task_name] = "started"
        try:
            if not self.simulation:
                self.c.plate_handler.initialize()
                self.c.wait_until_not_busy(timeout=60)
                assert self.c.error_status == 0, f"Error status: {ERROR_CODES[self.c.error_status]}"
            else:
                time.sleep(10)
            self.task_status[task_name] = "finished"
            return "Incubator initialized."
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to initialize incubator: {e}")
            return f"Failed to initialize incubator: {e}"
    
    @schema_function(skip_self=True)
    def get_temperature(self):
        """Get the current temperature of the incubator"""
        task_name = "get_temperature"
        self.task_status[task_name] = "started"
        try:
            if not self.simulation:
                self.c.wait_until_not_busy(timeout=50)
                temperature = self.c.climate_controller.current_temperature
                logger.info(f"Temperature: {temperature}")
            else:
                time.sleep(10)
                temperature = 37.0  # Simulated temperature
            self.task_status[task_name] = "finished"
            return temperature
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get temperature: {e}")
            return None
    
    @schema_function(skip_self=True)
    def hello_world(self):
        """Hello world"""
        task_name = "hello_world"
        self.task_status[task_name] = "started"
        self.task_status[task_name] = "finished"
        return "Hello world"
    
    @schema_function(skip_self=True)
    def get_slot_information(self, slot: int = Field(1, description="Slot number, range: 1-42")):
        """Get the current slot information of the incubator"""
        task_name = "get_slot_information"
        self.task_status[task_name] = "started"
        try:
            if not self.simulation:
                with open(self.samples_file, 'r') as file:
                    samples = json.load(file)
                slot_info = next((sample for sample in samples if sample["incubator_slot"] == slot), None)
            else:
                time.sleep(10)
                slot_info = {"incubator_slot": slot, "status": "Simulated"}
            self.task_status[task_name] = "finished"
            return slot_info
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get slot information: {e}")
            return None

    @schema_function(skip_self=True)
    def get_co2_level(self):
        """Get the current CO2 level of the incubator"""
        task_name = "get_co2_level"
        self.task_status[task_name] = "started"
        try:
            if not self.simulation:
                self.c.wait_until_not_busy(timeout=50)
                co2_level = self.c.climate_controller.current_co2
                logger.info(f"CO2 level: {co2_level}")
            else:
                time.sleep(10)
                co2_level = 5.0  # Simulated CO2 level
            self.task_status[task_name] = "finished"
            return co2_level
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get CO2 level: {e}")
            return None

    @schema_function(skip_self=True)
    def reset_error_status(self):
        """Reset the error status of the incubator"""
        task_name = "reset_error_status"
        self.task_status[task_name] = "started"
        try:
            if not self.simulation:
                self.c.maintenance_controller.reset_error_status()
            else:
                time.sleep(10)
            self.task_status[task_name] = "finished"
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to reset error status: {e}")

    @schema_function(skip_self=True)
    def get_status(self):
        """
        Get the status of the incubator
        Returns: A dictionary
        """
        task_name = "get_status"
        self.task_status[task_name] = "started"
        try:
            if not self.simulation:
                status = {
                    "error_status": self.c.error_status,
                    "action_status": self.c.action_status,
                    "busy": self.c.overview_status.busy
                }
            else:
                time.sleep(10)
                status = {"error_status": 0, "action_status": "Simulated", "busy": False}
            self.task_status[task_name] = "finished"
            return status
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get status: {e}")
            return {}

    @schema_function(skip_self=True)
    def move_plate(self, slot:int=Field(5, description="Slot number,range: 1-42")):
        """
        Move plate from slot to transfer station and back
        Returns: A string message
        """
        task_name = "move_plate"
        self.task_status[task_name] = "started"
        try:
            if not self.simulation:
                c = self.c
                c.wait_until_not_busy(timeout=50)
                assert c.error_status == 0, f"Error status: {ERROR_CODES[self.c.error_status]}"
                c.plate_handler.move_plate_from_slot_to_transfer_station(slot)
                c.wait_until_not_busy(timeout=100)
                assert c.error_status == 0, f"Error status: {ERROR_CODES[self.c.error_status]}"
                c.plate_handler.move_plate_from_transfer_station_to_slot(slot)
                c.wait_until_not_busy(timeout=50)
                assert c.error_status == 0, f"Error status: {ERROR_CODES[self.c.error_status]}"
            else:
                time.sleep(10)
            self.task_status[task_name] = "finished"
            return f"Plate moved to slot {slot} and back to transfer station."
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to move plate: {e}")
            return f"Failed to move plate: {e}"
    
    def update_sample_status(self, slot: int, status: str):
        # Update the sample's status in samples.json
        with open(self.samples_file, 'r') as file:
            samples = json.load(file)
        for sample in samples:
            if sample["incubator_slot"] == slot:
                sample["status"] = status
                break
        with open(self.samples_file, 'w') as file:
            json.dump(samples, file, indent=4)

    @schema_function(skip_self=True)
    def put_sample_from_transfer_station_to_slot(self, slot: int = Field(5, description="Slot number,range: 1-42")):
        """
        Collect sample from transfer station to a slot
        Returns: A string message
        """
        task_name = "put_sample_from_transfer_station_to_slot"
        self.task_status[task_name] = "started"
        try:
            if not self.simulation:
                # Access status directly from JSON file
                with open(self.samples_file, 'r') as file:
                    samples = json.load(file)
                current_status = next((sample["status"] for sample in samples if sample["incubator_slot"] == slot), None)
                assert current_status != "IN", "Plate is already inside the incubator"
                c = self.c
                c.plate_handler.move_plate_from_transfer_station_to_slot(slot)
                c.wait_until_not_busy(timeout=50)
                assert c.error_status == 0, f"Error status: {ERROR_CODES[self.c.error_status]}"
                self.update_sample_status(slot, "IN")
            else:
                time.sleep(10)
            self.task_status[task_name] = "finished"
            return f"Sample placed in slot {slot}."
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to put sample in slot {slot}: {e}")
            return f"Failed to put sample in slot {slot}: {e}"
    
    @schema_function(skip_self=True)
    def get_sample_from_slot_to_transfer_station(self, slot: int = Field(5, description="Slot number,range: 1-42")):
        """Release sample from a incubator's slot to it's transfer station."""
        task_name = "get_sample_from_slot_to_transfer_station"
        self.task_status[task_name] = "started"
        try:
            if not self.simulation:
                # Access status directly from JSON file
                with open(self.samples_file, 'r') as file:
                    samples = json.load(file)
                current_status = next((sample["status"] for sample in samples if sample["incubator_slot"] == slot), None)
                assert current_status != "OUT", "Plate is already outside the incubator"
                c = self.c
                c.plate_handler.move_plate_from_slot_to_transfer_station(slot)
                c.wait_until_not_busy(timeout=50)
                assert c.error_status == 0, f"Error status: {ERROR_CODES[self.c.error_status]}"
                self.update_sample_status(slot, "OUT")
            else:
                time.sleep(10)
            self.task_status[task_name] = "finished"
            return f"Sample removed from slot {slot}."
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get sample from slot {slot}: {e}")
            return f"Failed to get sample from slot {slot}: {e}"

    @schema_function(skip_self=True)
    def get_sample_status(self, slot: Optional[int] = Field(None, description="Slot number, range: 1-42, or None for all slots")):
        """Return the status of sample plates from samples.json"""
        task_name = "get_sample_status"
        self.task_status[task_name] = "started"
        try:
            if not self.simulation:
                with open(self.samples_file, 'r') as file:
                    samples = json.load(file)
            
                if slot is None:
                    status = {sample["incubator_slot"]: sample["status"] for sample in samples}
                else:
                    status = next((sample["status"] for sample in samples if sample["incubator_slot"] == slot), "Unknown")
            else:
                time.sleep(10)
                if slot is None:
                    status = {i: "Simulated" for i in range(1, 43)}
                else:
                    status = "Simulated"
            self.task_status[task_name] = "finished"
            return status
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get sample status: {e}")
            return {}

    def is_busy(self):
        task_name = "is_busy"
        self.task_status[task_name] = "started"
        try:
            if not self.simulation:
                busy = self.c.overview_status.busy
            else:
                time.sleep(10)
                busy = False
            self.task_status[task_name] = "finished"
            return busy
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to check if incubator is busy: {e}")
            return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the Hypha service for the incubator.")
    parser.add_argument('--local', action='store_true', help="Use localhost as server URL")
    parser.add_argument('--simulation', action='store_true', help="Run in simulation mode")
    args = parser.parse_args()

    incubator_service = IncubatorService(local=args.local, simulation=args.simulation)

    loop = asyncio.get_event_loop()

    async def main():
        try:
            incubator_service.setup_task = asyncio.create_task(incubator_service.setup())
            await incubator_service.setup_task
        except Exception as e:
            logger.error(f"Error setting up incubator service: {e}")
            raise e

    loop.create_task(main())
    loop.run_forever()