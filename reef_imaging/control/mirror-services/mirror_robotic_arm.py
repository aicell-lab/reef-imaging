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
        self.cloud_service = None  # Add reference to registered cloud service
        
        # Connection to local service
        self.local_server_url = "http://reef.dyn.scilifelab.se:9527"
        self.local_token = os.environ.get("REEF_LOCAL_TOKEN")
        self.local_service_id = "robotic-arm-control"
        self.local_server = None
        self.local_service = None

        # Setup task tracking
        self.setup_task = None
        
        # Store dynamically created mirror methods
        self.mirrored_methods = {}

    async def connect_to_local_service(self):
        """Connect to the local robotic arm service"""
        try:
            logger.info(f"Connecting to local service at {self.local_server_url}")
            self.local_server = await connect_to_server({
                "server_url": self.local_server_url, 
                "token": self.local_token,
                "ping_interval": 30
            })
            
            # Connect to the local service
            self.local_service = await self.local_server.get_service(self.local_service_id)
            logger.info(f"Successfully connected to local service {self.local_service_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to local service: {e}")
            self.local_service = None
            self.local_server = None
            return False

    async def cleanup_cloud_service(self):
        """Clean up the cloud service registration"""
        try:
            if self.cloud_service:
                logger.info(f"Unregistering cloud service {self.cloud_service_id}")
                # Try to unregister the service
                try:
                    await self.cloud_server.unregister_service(self.cloud_service_id)
                    logger.info(f"Successfully unregistered cloud service {self.cloud_service_id}")
                except Exception as e:
                    logger.warning(f"Failed to unregister cloud service {self.cloud_service_id}: {e}")
                
                self.cloud_service = None
            
            # Clear mirrored methods
            self.mirrored_methods.clear()
            logger.info("Cleared mirrored methods")
            
        except Exception as e:
            logger.error(f"Error during cloud service cleanup: {e}")

    def _create_mirror_method(self, method_name, local_method):
        """Create a mirror method that forwards calls to the local service"""
        async def mirror_method(*args, **kwargs):
            try:
                if self.local_service is None:
                    logger.warning(f"Local service is None when calling {method_name}, attempting to reconnect")
                    success = await self.connect_to_local_service()
                    if not success or self.local_service is None:
                        raise Exception("Failed to connect to local service")
                
                # Forward the call to the local service
                result = await local_method(*args, **kwargs)
                return result
            except Exception as e:
                logger.error(f"Failed to call {method_name}: {e}")
                raise e

        # Check if the original method has schema information
        if hasattr(local_method, '__schema__'):
            # Preserve the schema information from the original method
            original_schema = getattr(local_method, '__schema__')
            
            # Handle case where schema might be None
            if original_schema is not None:
                logger.info(f"Preserving schema for method {method_name}: {original_schema}")
                
                # Create a new function with the same signature and schema
                # We need to manually copy the schema information since we can't use the decorator directly
                mirror_method.__schema__ = original_schema
                mirror_method.__doc__ = original_schema.get('description', f"Mirror of {method_name}")
            else:
                logger.debug(f"Schema is None for method {method_name}, using basic mirror")
        else:
            # No schema information available, return the basic mirror method
            logger.debug(f"No schema information found for method {method_name}, using basic mirror")
        
        return mirror_method

    def _get_mirrored_methods(self):
        """Dynamically create mirror methods for all callable methods in local_service"""
        if self.local_service is None:
            logger.warning("Cannot create mirror methods: local_service is None")
            return {}
        
        logger.info(f"Creating mirror methods for local service {self.local_service_id}")
        mirrored_methods = {}
        
        # Methods to exclude from mirroring (these are handled specially)
        excluded_methods = {
            'name', 'id', 'config', 'type',  # Service metadata
            '__class__', '__doc__', '__dict__', '__module__',  # Python internals
        }
        
        # Get all attributes from the local service
        for attr_name in dir(self.local_service):
            if attr_name.startswith('_') or attr_name in excluded_methods:
                continue
                
            attr = getattr(self.local_service, attr_name)
            
            # Check if it's callable (a method)
            if callable(attr):
                logger.info(f"Creating robotic arm mirror method for: {attr_name}")
                mirrored_methods[attr_name] = self._create_mirror_method(attr_name, attr)
        
        logger.info(f"Robotic arm service: Created {len(mirrored_methods)} mirror methods: {list(mirrored_methods.keys())}")
        return mirrored_methods

    async def check_service_health(self):
        """Check if the service is healthy and rerun setup if needed"""
        logger.info("Starting service health check task")
        while True:
            try:
                # Try to get the service status
                if self.cloud_service_id and self.cloud_server:
                    try:
                        service = await self.cloud_server.get_service(self.cloud_service_id)
                        # Try a simple operation to verify service is working
                        ping_result = await asyncio.wait_for(service.ping(), timeout=10)
                        if ping_result != "pong":
                            logger.error(f"Cloud service health check failed: {ping_result}")
                            raise Exception("Cloud service not healthy")
                    except Exception as e:
                        logger.error(f"Cloud service health check failed: {e}")
                        raise Exception(f"Cloud service not healthy: {e}")
                else:
                    logger.info("Cloud service ID or server not set, waiting for service registration")
                
                # Always check local service regardless of whether it's None
                try:
                    if self.local_service is None:
                        logger.info("Local service connection lost, attempting to reconnect")
                        success = await self.connect_to_local_service()
                        if not success or self.local_service is None:
                            raise Exception("Failed to connect to local service")
                    
                    #logger.info("Checking local service health with timeout, if timeout, local service is not healthy...")
                    try:
                        local_ping_result = await asyncio.wait_for(self.local_service.ping(), timeout=10)
                    except asyncio.TimeoutError:
                        logger.error("Local service health check timed out, assuming it's not healthy")
                        raise Exception("Local service not healthy")
                    
                    #logger.info(f"Local service response: {local_ping_result}")
                    
                    if local_ping_result != "pong":
                        logger.error(f"Local service health check failed: {local_ping_result}")
                        raise Exception("Local service not healthy")
                    
                    #logger.info("Local service health check passed")
                except Exception as e:
                    logger.error(f"Local service health check failed: {e}")
                    self.local_service = None  # Reset connection so it will reconnect next time
                    raise Exception(f"Local service not healthy: {e}")
            except Exception as e:
                logger.error(f"Service health check failed: {e}")
                logger.info("Attempting to clean up and rerun setup...")
                
                # Clean up everything properly
                try:
                    # First, clean up the cloud service
                    await self.cleanup_cloud_service()
                    
                    # Then disconnect from servers
                    if self.cloud_server:
                        await self.cloud_server.disconnect()
                    if self.local_server:
                        await self.local_server.disconnect()
                    if self.setup_task:
                        self.setup_task.cancel()  # Cancel the previous setup task
                except Exception as disconnect_error:
                    logger.error(f"Error during cleanup: {disconnect_error}")
                finally:
                    self.cloud_server = None
                    self.cloud_service = None
                    self.local_server = None
                    self.local_service = None
                    self.mirrored_methods.clear()

                # Retry setup with exponential backoff
                retry_count = 0
                max_retries = 50
                base_delay = 10
                
                while retry_count < max_retries:
                    try:
                        delay = base_delay * (2 ** min(retry_count, 5))  # Cap at 32 * base_delay
                        logger.info(f"Retrying setup in {delay} seconds (attempt {retry_count + 1}/{max_retries})")
                        await asyncio.sleep(delay)
                        
                        # Rerun the setup method
                        self.setup_task = asyncio.create_task(self.setup())
                        await self.setup_task
                        logger.info("Setup successful after reconnection")
                        break  # Exit the loop if setup is successful
                    except Exception as setup_error:
                        retry_count += 1
                        logger.error(f"Failed to rerun setup (attempt {retry_count}/{max_retries}): {setup_error}")
                        if retry_count >= max_retries:
                            logger.error("Max retries reached, giving up on setup")
                            await asyncio.sleep(60)  # Wait longer before next health check cycle
                            break
            
            await asyncio.sleep(10)  # Check every half minute

    async def start_hypha_service(self, server):
        """Start the Hypha service with dynamically mirrored methods"""
        self.cloud_server = server
        
        # Ensure we have a connection to the local service
        if self.local_service is None:
            logger.info("Local service not connected, attempting to connect before creating mirror methods")
            success = await self.connect_to_local_service()
            if not success:
                raise Exception("Cannot start Hypha service without local service connection")
        
        # Get the mirrored methods from the current local service
        self.mirrored_methods = self._get_mirrored_methods()
        
        # Base service configuration with core methods
        service_config = {
            "name": "Mirror Robotic Arm Control",
            "id": self.cloud_service_id,
            "config": {
                "visibility": "protected",
                "run_in_executor": True
            },
            "type": "service",
        }
        
        # Add all mirrored methods to the service configuration
        service_config.update(self.mirrored_methods)
        
        # Register the service
        self.cloud_service = await server.register_service(service_config,overwrite=True)

        logger.info(
            f"Mirror robotic arm service (service_id={self.cloud_service_id}) started successfully with {len(self.mirrored_methods)} mirrored methods, available at {self.cloud_server_url}/services"
        )

        logger.info(f'You can use this service using the service id: {self.cloud_service.id}')
        id = self.cloud_service.id.split(":")[1]

        logger.info(f"You can also test the service via the HTTP proxy: {self.cloud_server_url}/{server.config.workspace}/services/{id}")

    async def setup(self):
        # Connect to cloud workspace
        logger.info(f"Connecting to cloud workspace {self.cloud_workspace} at {self.cloud_server_url}")
        server = await connect_to_server({
            "server_url": self.cloud_server_url, 
            "token": self.cloud_token, 
            "workspace": self.cloud_workspace,
            "ping_interval": 30
        })
        
        # Connect to local service first (needed to get available methods)
        logger.info("Connecting to local service before setting up mirror service")
        success = await self.connect_to_local_service()
        if not success or self.local_service is None:
            raise Exception("Failed to connect to local service during setup")
        
        # Verify local service is working
        try:
            ping_result = await asyncio.wait_for(self.local_service.ping(), timeout=10)
            if ping_result != "pong":
                raise Exception(f"Local service verification failed: {ping_result}")
            logger.info("Local service connection verified successfully")
        except Exception as e:
            logger.error(f"Local service verification failed: {e}")
            raise Exception(f"Local service not responding properly: {e}")
        
        # Small delay to ensure local service is fully ready
        await asyncio.sleep(1)
        
        # Start the cloud service with mirrored methods
        logger.info("Starting cloud service with mirrored methods")
        await self.start_hypha_service(server)
        
        logger.info("Setup completed successfully")

    def ping(self):
        """Ping function for health checks"""
        return "pong"

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