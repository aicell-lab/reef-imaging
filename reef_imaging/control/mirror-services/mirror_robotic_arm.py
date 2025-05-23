import os
import logging
import logging.handlers
import time
import argparse
import asyncio
import traceback
import dotenv
from hypha_rpc import login, connect_to_server

dotenv.load_dotenv()  
ENV_FILE = dotenv.find_dotenv()  
if ENV_FILE:  
    dotenv.load_dotenv(ENV_FILE)  

# Set up logging
def setup_logging(log_file="mirror_robotic_arm_service.log", max_bytes=100000, backup_count=3):
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

class MirrorRoboticArmService:
    def __init__(self):
        # Connection to cloud service
        self.cloud_server_url = "https://hypha.aicell.io"
        self.cloud_workspace = "reef-imaging"
        self.cloud_token = os.environ.get("REEF_WORKSPACE_TOKEN")
        self.cloud_service_id = "mirror-robotic-arm-control"
        self.cloud_server = None
        
        # Connection to local service
        self.local_server_url = "http://reef.dyn.scilifelab.se:9527"
        self.local_token = os.environ.get("REEF_LOCAL_TOKEN")
        self.local_service_id = "robotic-arm-control"
        self.local_server = None
        self.local_service = None

        # Task tracking
        self.setup_task = None
        self.task_status = {
            "connect_to_local_service": "not_started",
            "move_sample_from_microscope1_to_incubator": "not_started",
            "move_sample_from_incubator_to_microscope1": "not_started",
            "grab_sample_from_microscope1": "not_started",
            "grab_sample_from_incubator": "not_started",
            "put_sample_on_microscope1": "not_started",
            "put_sample_on_incubator": "not_started",
            "transport_from_incubator_to_microscope1": "not_started",
            "transport_from_microscope1_to_incubator": "not_started",
            "connect": "not_started",
            "disconnect": "not_started",
            "halt": "not_started",
            "set_alarm": "not_started",
            "get_all_joints": "not_started",
            "get_all_positions": "not_started",
            "light_on": "not_started",
            "light_off": "not_started",
            "incubator_to_microscope": "not_started",
            "microscope_to_incubator": "not_started"
        }

    async def connect_to_local_service(self):
        """Connect to the local robotic arm service"""
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
                    
                    #logger.info("Checking local service health with timeout, if timeout, local service is not healthy...")
                    try:
                        local_hello_world_result = await asyncio.wait_for(self.local_service.hello_world(), timeout=10)
                    except asyncio.TimeoutError:
                        logger.error("Local service health check timed out, assuming it's not healthy")
                        raise Exception("Local service not healthy")
                    
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
            
            await asyncio.sleep(10)  # Check every half minute

    async def start_hypha_service(self, server):
        self.cloud_server = server
        svc = await server.register_service({
            "name": "Mirror Robotic Arm Control",
            "id": self.cloud_service_id,
            "config": {
                "visibility": "public",
                "run_in_executor": True
            },
            "hello_world": self.hello_world,
            "move_sample_from_microscope1_to_incubator": self.move_sample_from_microscope1_to_incubator,
            "move_sample_from_incubator_to_microscope1": self.move_sample_from_incubator_to_microscope1,
            "grab_sample_from_microscope1": self.grab_sample_from_microscope1,
            "grab_sample_from_incubator": self.grab_sample_from_incubator,
            "put_sample_on_microscope1": self.put_sample_on_microscope1,
            "put_sample_on_incubator": self.put_sample_on_incubator,
            "transport_from_incubator_to_microscope1": self.transport_from_incubator_to_microscope1,
            "transport_from_microscope1_to_incubator": self.transport_from_microscope1_to_incubator,
            "connect": self.connect,
            "disconnect": self.disconnect,
            "halt": self.halt,
            "get_all_joints": self.get_all_joints,
            "get_all_positions": self.get_all_positions,
            # Add status functions
            "get_task_status": self.get_task_status,
            "get_all_task_status": self.get_all_task_status,
            "reset_task_status": self.reset_task_status,
            "reset_all_task_status": self.reset_all_task_status,
            "set_alarm": self.set_alarm,
            "light_on": self.light_on,
            "light_off": self.light_off,
            "get_actions": self.get_actions,
            "execute_action": self.execute_action,
            # Add microscope ID functions
            "incubator_to_microscope": self.incubator_to_microscope,
            "microscope_to_incubator": self.microscope_to_incubator
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
    async def move_sample_from_microscope1_to_incubator(self):
        """Mirror function for move_sample_from_microscope1_to_incubator"""
        task_name = "move_sample_from_microscope1_to_incubator"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.move_sample_from_microscope1_to_incubator()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to move sample from microscope1 to incubator: {e}")
            raise e

    async def move_sample_from_incubator_to_microscope1(self):
        """Mirror function for move_sample_from_incubator_to_microscope1"""
        task_name = "move_sample_from_incubator_to_microscope1"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.move_sample_from_incubator_to_microscope1()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to move sample from incubator to microscope1: {e}")
            raise e

    async def grab_sample_from_microscope1(self):
        """Mirror function for grab_sample_from_microscope1"""
        task_name = "grab_sample_from_microscope1"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.grab_sample_from_microscope1()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to grab sample from microscope1: {e}")
            raise e

    async def grab_sample_from_incubator(self):
        """Mirror function for grab_sample_from_incubator"""
        task_name = "grab_sample_from_incubator"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.grab_sample_from_incubator()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to grab sample from incubator: {e}")
            raise e

    async def put_sample_on_microscope1(self):
        """Mirror function for put_sample_on_microscope1"""
        task_name = "put_sample_on_microscope1"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.put_sample_on_microscope1()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to put sample on microscope1: {e}")
            raise e

    async def put_sample_on_incubator(self):
        """Mirror function for put_sample_on_incubator"""
        task_name = "put_sample_on_incubator"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.put_sample_on_incubator()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to put sample on incubator: {e}")
            raise e

    async def transport_from_incubator_to_microscope1(self):
        """Mirror function for transport_from_incubator_to_microscope1"""
        task_name = "transport_from_incubator_to_microscope1"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.transport_from_incubator_to_microscope1()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to transport from incubator to microscope1: {e}")
            raise e

    async def transport_from_microscope1_to_incubator(self):
        """Mirror function for transport_from_microscope1_to_incubator"""
        task_name = "transport_from_microscope1_to_incubator"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.transport_from_microscope1_to_incubator()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to transport from microscope1 to incubator: {e}")
            raise e
    
    async def incubator_to_microscope(self, microscope_id=1):
        """
        Move a sample from the incubator to microscopes
        Returns: bool
        """
        task_name = "incubator_to_microscope"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
                                
            result = await self.local_service.incubator_to_microscope(microscope_id)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to move sample from incubator to microscope: {e}")
            raise e

    async def microscope_to_incubator(self, microscope_id=1):
        """
        Move a sample from microscopes to the incubator
        Returns: bool
        """
        task_name = "microscope_to_incubator"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()

            result = await self.local_service.microscope_to_incubator(microscope_id)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to move sample from microscope to incubator: {e}")
            raise e
                
    async def connect(self):
        """Mirror function for connect"""
        task_name = "connect"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.connect()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to connect: {e}")
            raise e

    async def disconnect(self):
        """Mirror function for disconnect"""
        task_name = "disconnect"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.disconnect()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to disconnect: {e}")
            raise e

    async def halt(self):
        """Mirror function for halt"""
        task_name = "halt"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.halt()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to halt: {e}")
            raise e

    async def get_all_joints(self):
        """Mirror function for get_all_joints"""
        task_name = "get_all_joints"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.get_all_joints()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get all joints: {e}")
            raise e

    async def get_all_positions(self):
        """Mirror function for get_all_positions"""
        task_name = "get_all_positions"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.get_all_positions()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get all positions: {e}")
            raise e

    async def set_alarm(self, state=1):
        """Mirror function for set_alarm"""
        task_name = "set_alarm"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.set_alarm(state)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to set alarm: {e}")
            raise e

    async def light_on(self):
        """Mirror function for light_on"""
        task_name = "light_on"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.light_on()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to turn on light: {e}")
            raise e

    async def light_off(self):
        """Mirror function for light_off"""
        task_name = "light_off"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.light_off()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to turn off light: {e}")
            raise e

    async def get_actions(self):
        """Mirror function for get_actions"""
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.get_actions()
            return result
        except Exception as e:
            logger.error(f"Failed to get actions: {e}")
            raise e

    async def execute_action(self, action_id):
        """Mirror function for execute_action"""
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.execute_action(action_id)
            return result
        except Exception as e:
            logger.error(f"Failed to execute action: {e}")
            raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Mirror service for Robotic Arm control."
    )
    args = parser.parse_args()

    mirror_service = MirrorRoboticArmService()

    loop = asyncio.get_event_loop()

    async def main():
        try:
            mirror_service.setup_task = asyncio.create_task(mirror_service.setup())
            await mirror_service.setup_task
            # Start the health check task after setup is complete
            asyncio.create_task(mirror_service.check_service_health())
        except Exception:
            traceback.print_exc()

    loop.create_task(main())
    loop.run_forever() 