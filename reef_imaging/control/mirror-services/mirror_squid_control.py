import os
import logging
import logging.handlers
import time
import argparse
import asyncio
import traceback
import dotenv
from hypha_rpc import login, connect_to_server, register_rtc_service

dotenv.load_dotenv()  
ENV_FILE = dotenv.find_dotenv()  
if ENV_FILE:  
    dotenv.load_dotenv(ENV_FILE)  

# Set up logging
def setup_logging(log_file="mirror_squid_control_service.log", max_bytes=100000, backup_count=3):
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

class MirrorMicroscopeService:
    def __init__(self):
        self.login_required = True
        # Connection to cloud service
        self.cloud_server_url = "https://hypha.aicell.io"
        self.cloud_workspace = "reef-imaging"
        self.cloud_token = os.environ.get("REEF_WORKSPACE_TOKEN")
        self.cloud_service_id = "mirror-microscope-control-squid-1"
        self.cloud_server = None
        
        # Connection to local service
        self.local_server_url = "http://reef.dyn.scilifelab.se:9527"
        self.local_token = os.environ.get("REEF_LOCAL_TOKEN")
        self.local_service_id = "microscope-control-squid-1"
        self.local_server = None
        self.local_service = None

        # Task tracking
        self.setup_task = None
        self.task_status = {
            "connect_to_local_service": "not_started",
            "move_by_distance": "not_started",
            "move_to_position": "not_started",
            "get_status": "not_started",
            "update_parameters_from_client": "not_started",
            "one_new_frame": "not_started",
            "snap": "not_started",
            "open_illumination": "not_started",
            "close_illumination": "not_started",
            "scan_well_plate": "not_started",
            "scan_well_plate_simulated": "not_started",
            "set_illumination": "not_started",
            "set_camera_exposure": "not_started",
            "stop_scan": "not_started",
            "home_stage": "not_started",
            "return_stage": "not_started",
            "move_to_loading_position": "not_started",
            "auto_focus": "not_started",
            "do_laser_autofocus": "not_started",
            "navigate_to_well": "not_started",
            "get_chatbot_url": "not_started"
        }

    async def connect_to_local_service(self):
        """Connect to the local microscope service"""
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
            
            await asyncio.sleep(10)  # Check more frequently (was 30)

    async def start_hypha_service(self, server):
        self.cloud_server = server
        svc = await server.register_service(
            {
                "name": "Mirror Microscope Control Service",
                "id": self.cloud_service_id,
                "config": {
                    "visibility": "public",
                    "run_in_executor": True
                },
                "type": "echo",
                "hello_world": self.hello_world,
                "move_by_distance": self.move_by_distance,
                "snap": self.snap,
                "one_new_frame": self.one_new_frame,
                "off_illumination": self.close_illumination,
                "on_illumination": self.open_illumination,
                "set_illumination": self.set_illumination,
                "set_camera_exposure": self.set_camera_exposure,
                "scan_well_plate": self.scan_well_plate,
                "scan_well_plate_simulated": self.scan_well_plate_simulated,
                "stop_scan": self.stop_scan,
                "home_stage": self.home_stage,
                "return_stage": self.return_stage,
                "navigate_to_well": self.navigate_to_well,
                "move_to_position": self.move_to_position,
                "move_to_loading_position": self.move_to_loading_position,
                "auto_focus": self.auto_focus,
                "do_laser_autofocus": self.do_laser_autofocus,
                "get_status": self.get_status,
                "update_parameters_from_client": self.update_parameters_from_client,
                "get_chatbot_url": self.get_chatbot_url,
                # Add status functions
                "get_task_status": self.get_task_status,
                "get_all_task_status": self.get_all_task_status,
                "reset_task_status": self.reset_task_status,
                "reset_all_task_status": self.reset_all_task_status
            },
        )

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
    async def move_by_distance(self, x=0.0, y=0.0, z=0.0, context=None):
        """Mirror function to move_by_distance on local service"""
        task_name = "move_by_distance"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.move_by_distance(x, y, z)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to move by distance: {e}")
            raise e

    async def move_to_position(self, x=None, y=None, z=None, context=None):
        """Mirror function to move_to_position on local service"""
        task_name = "move_to_position"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.move_to_position(x, y, z)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to move to position: {e}")
            raise e

    async def get_status(self, context=None):
        """Mirror function to get_status on local service"""
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

    async def update_parameters_from_client(self, new_parameters=None, context=None):
        """Mirror function to update_parameters_from_client on local service"""
        task_name = "update_parameters_from_client"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.update_parameters_from_client(new_parameters)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to update parameters: {e}")
            raise e

    async def one_new_frame(self, exposure_time=100, channel=0, intensity=50, context=None):
        """Mirror function to one_new_frame on local service"""
        task_name = "one_new_frame"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.one_new_frame(exposure_time, channel, intensity)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get new frame: {e}")
            raise e

    async def snap(self, exposure_time=100, channel=0, intensity=50, context=None):
        """Mirror function to snap on local service"""
        task_name = "snap"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.snap(exposure_time, channel, intensity)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to snap image: {e}")
            raise e

    async def open_illumination(self, context=None):
        """Mirror function to open_illumination on local service"""
        task_name = "open_illumination"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.on_illumination()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to open illumination: {e}")
            raise e

    async def close_illumination(self, context=None):
        """Mirror function to close_illumination on local service"""
        task_name = "close_illumination"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.off_illumination()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to close illumination: {e}")
            raise e

    async def scan_well_plate(self, well_plate_type="96", illuminate_channels=None, do_contrast_autofocus=False, do_reflection_af=True, scanning_zone=None, Nx=3, Ny=3, action_ID='testPlateScan', context=None):
        """Mirror function to scan_well_plate on local service"""
        task_name = "scan_well_plate"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            if illuminate_channels is None:
                illuminate_channels = ['BF LED matrix full','Fluorescence 488 nm Ex','Fluorescence 561 nm Ex']
            
            if scanning_zone is None:
                scanning_zone = [(0,0),(0,0)]
                
            result = await self.local_service.scan_well_plate(
                well_plate_type, illuminate_channels, do_contrast_autofocus, 
                do_reflection_af, scanning_zone, Nx, Ny, action_ID
            )
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to scan well plate: {e}")
            raise e

    async def scan_well_plate_simulated(self, context=None):
        """Mirror function to scan_well_plate_simulated on local service"""
        task_name = "scan_well_plate_simulated"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.scan_well_plate_simulated()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to scan well plate: {e}")
            raise e

    async def set_illumination(self, channel=0, intensity=50, context=None):
        """Mirror function to set_illumination on local service"""
        task_name = "set_illumination"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.set_illumination(channel, intensity)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to set illumination: {e}")
            raise e

    async def set_camera_exposure(self, exposure_time=100, context=None):
        """Mirror function to set_camera_exposure on local service"""
        task_name = "set_camera_exposure"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.set_camera_exposure(exposure_time)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to set camera exposure: {e}")
            raise e

    async def stop_scan(self, context=None):
        """Mirror function to stop_scan on local service"""
        task_name = "stop_scan"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.stop_scan()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to stop scan: {e}")
            raise e

    async def home_stage(self, context=None):
        """Mirror function to home_stage on local service"""
        task_name = "home_stage"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.home_stage()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to home stage: {e}")
            raise e

    async def return_stage(self, context=None):
        """Mirror function to return_stage on local service"""
        task_name = "return_stage"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.return_stage()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to return stage: {e}")
            raise e

    async def move_to_loading_position(self, context=None):
        """Mirror function to move_to_loading_position on local service"""
        task_name = "move_to_loading_position"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.move_to_loading_position()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to move to loading position: {e}")
            raise e

    async def auto_focus(self, context=None):
        """Mirror function to auto_focus on local service"""
        task_name = "auto_focus"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.auto_focus()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to auto focus: {e}")
            raise e

    async def do_laser_autofocus(self, context=None):
        """Mirror function to do_laser_autofocus on local service"""
        task_name = "do_laser_autofocus"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.do_laser_autofocus()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to do laser autofocus: {e}")
            raise e

    async def navigate_to_well(self, row='A', col=1, wellplate_type='96', context=None):
        """Mirror function to navigate_to_well on local service"""
        task_name = "navigate_to_well"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.navigate_to_well(row, col, wellplate_type)
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to navigate to well: {e}")
            raise e

    async def get_chatbot_url(self, context=None):
        """Mirror function to get_chatbot_url on local service"""
        task_name = "get_chatbot_url"
        self.task_status[task_name] = "started"
        try:
            if self.local_service is None:
                await self.connect_to_local_service()
            
            result = await self.local_service.get_chatbot_url()
            self.task_status[task_name] = "finished"
            return result
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get chatbot URL: {e}")
            raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Mirror service for Squid microscope control."
    )
    parser.add_argument(
        "--cloud-service-id",
        default="mirror-microscope-control-squid-1",
        help="ID for the cloud service (default: mirror-microscope-control-squid-1)"
    )
    parser.add_argument(
        "--local-service-id",
        default="microscope-control-squid-1",
        help="ID for the local service (default: microscope-control-squid-1)"
    )
    args = parser.parse_args()

    mirror_service = MirrorMicroscopeService()
    # Override the service IDs with command line arguments
    mirror_service.cloud_service_id = args.cloud_service_id
    mirror_service.local_service_id = args.local_service_id

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