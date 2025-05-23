import os
import logging
import logging.handlers
import time
import asyncio
import traceback
import dotenv
from hypha_rpc import connect_to_server
from typing import List, Dict, Any, Callable

dotenv.load_dotenv()
ENV_FILE = dotenv.find_dotenv()
if ENV_FILE:
    dotenv.load_dotenv(ENV_FILE)

def setup_logging_generic(log_file_name: str, logger_name: str, max_bytes=100000, backup_count=3) -> logging.Logger:
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.handlers.RotatingFileHandler(log_file_name, maxBytes=max_bytes, backupCount=backup_count)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    return logger

class GenericMirrorService:
    def __init__(self,
                 local_service_id: str,
                 local_server_url: str,
                 local_token_env_var: str,
                 cloud_service_id: str,
                 cloud_service_name: str,
                 cloud_server_url: str,
                 cloud_token_env_var: str,
                 cloud_workspace: str,
                 methods_to_mirror: List[str],
                 logger_instance: logging.Logger
                ):
        self.local_service_id = local_service_id
        self.local_server_url = local_server_url
        self.local_token = os.environ.get(local_token_env_var)
        
        self.cloud_service_id = cloud_service_id
        self.cloud_service_name = cloud_service_name
        self.cloud_server_url = cloud_server_url
        self.cloud_token = os.environ.get(cloud_token_env_var)
        self.cloud_workspace = cloud_workspace
        
        self.methods_to_mirror = methods_to_mirror
        self.logger = logger_instance

        self.local_server: Any = None
        self.local_service: Any = None
        self.cloud_server: Any = None
        self.setup_task: asyncio.Task | None = None

        self.task_status: Dict[str, str] = {
            "connect_to_local_service": "not_started",
        }
        for method_name in self.methods_to_mirror:
            self.task_status[method_name] = "not_started"
            wrapper = self._create_mirrored_method(method_name)
            setattr(self, method_name, wrapper)
        
        # Add task status entries for the status functions themselves, if they were to be tracked.
        # For now, they are not tracked in the original code.
        self.task_status["get_task_status"] = "not_started" # Example, not strictly necessary for these
        self.task_status["get_all_task_status"] = "not_started"
        self.task_status["reset_task_status"] = "not_started"
        self.task_status["reset_all_task_status"] = "not_started"


    def _create_mirrored_method(self, method_name: str) -> Callable:
        async def mirrored_method_wrapper(*args: Any, **kwargs: Any) -> Any:
            kwargs_for_local_service = {k: v for k, v in kwargs.items() if k != 'context'}

            self.task_status[method_name] = "started"
            self.logger.info(f"Task '{method_name}' started with args: {args}, original kwargs: {kwargs} (context stripped for local call if present)")
            try:
                if self.local_service is None:
                    self.logger.info(f"Local service not connected for task '{method_name}', attempting to connect.")
                    await self.connect_to_local_service()
                    if self.local_service is None:
                        self.logger.error(f"Failed to connect to local service before calling method '{method_name}'.")
                        raise Exception(f"Local service connection failed for {method_name}")
                
                local_method_callable = getattr(self.local_service, method_name)
                result = await local_method_callable(*args, **kwargs_for_local_service)
                self.task_status[method_name] = "finished"
                self.logger.info(f"Task '{method_name}' finished successfully.")
                return result
            except Exception as e:
                self.task_status[method_name] = "failed"
                self.logger.error(f"Task '{method_name}' failed: {e}\\n{traceback.format_exc()}")
                raise

        mirrored_method_wrapper.__name__ = method_name
        mirrored_method_wrapper.__doc__ = f"Mirrored function for {method_name}. Calls the method on the local service {self.local_service_id}."
        return mirrored_method_wrapper

    async def connect_to_local_service(self) -> bool:
        task_name = "connect_to_local_service"
        # Optimistically set to started, will be failed on error
        self.task_status[task_name] = "started" 
        self.logger.info(f"Attempting to connect to local service: {self.local_service_id} at {self.local_server_url}")
        try:
            if not self.local_token:
                self.logger.error(f"Local token (env var for {self.local_token_env_var if hasattr(self, 'local_token_env_var') else 'LOCAL_TOKEN'}) not found. Cannot connect to local service.")
                self.task_status[task_name] = "failed"
                return False

            self.local_server = await connect_to_server({
                "server_url": self.local_server_url,
                "token": self.local_token,
                "ping_interval": None
            })
            self.local_service = await self.local_server.get_service(self.local_service_id)
            self.logger.info(f"Successfully connected to local service: {self.local_service_id}")
            self.task_status[task_name] = "finished"
            return True
        except Exception as e:
            self.task_status[task_name] = "failed"
            self.logger.error(f"Failed to connect to local service '{self.local_service_id}': {e}\\n{traceback.format_exc()}")
            if self.local_server: # Attempt to disconnect if server object exists
                try:
                    await self.local_server.disconnect()
                except Exception as disconnect_e:
                    self.logger.error(f"Error disconnecting local server during connect failure: {disconnect_e}")
            self.local_server = None
            self.local_service = None
            return False

    async def start_hypha_service(self, server: Any):
        self.cloud_server = server
        
        functions_to_register: Dict[str, Callable] = {
            "hello_world": self.hello_world,
            "get_task_status": self.get_task_status,
            "get_all_task_status": self.get_all_task_status,
            "reset_task_status": self.reset_task_status,
            "reset_all_task_status": self.reset_all_task_status,
        }

        for method_name in self.methods_to_mirror:
            functions_to_register[method_name] = getattr(self, method_name)

        svc_config = {
            "name": self.cloud_service_name,
            "id": self.cloud_service_id,
            "config": {
                "visibility": "public",
                "run_in_executor": True
            },
        }
        svc_config.update(functions_to_register) # Add functions directly to the service config dict
        
        registered_svc_proxy = await server.register_service(svc_config)

        self.logger.info(
            f"Mirror service '{self.cloud_service_name}' (id={self.cloud_service_id}) started successfully. "
            f"Available at workspace '{self.cloud_workspace}' on {self.cloud_server_url}"
        )
        self.logger.info(f"Registered service full ID from server: {registered_svc_proxy.id}")
        
        # Construct proxy URL (handle potential : in workspace name if it occurs)
        workspace_part = server.config.workspace
        service_id_part = registered_svc_proxy.id.split(":")[-1] # Get the part after the last colon
        
        proxy_url = f"{self.cloud_server_url}/{workspace_part}/services/{service_id_part}"
        self.logger.info(f"Test via HTTP proxy (example with hello_world): {proxy_url}/hello_world")


    async def setup(self):
        self.logger.info(f"Connecting to cloud workspace '{self.cloud_workspace}' at {self.cloud_server_url}")
        if not self.cloud_token:
            self.logger.error(f"Cloud token (env var for {self.cloud_token_env_var if hasattr(self, 'cloud_token_env_var') else 'CLOUD_TOKEN'}) not found. Cannot connect to cloud server.")
            raise Exception("Cloud token not configured.")

        cloud_connection = await connect_to_server({
            "server_url": self.cloud_server_url,
            "token": self.cloud_token,
            "workspace": self.cloud_workspace,
            "ping_interval": None
        })
        
        await self.start_hypha_service(cloud_connection)
        # It's important that local service is connected for methods to work,
        # but start_hypha_service registers them. connect_to_local_service will be called
        # by individual methods if needed, or here to ensure it's ready.
        # Original called it after start_hypha_service.
        await self.connect_to_local_service()

    def hello_world(self) -> str:
        self.logger.info(f"hello_world called on mirror service '{self.cloud_service_name}' itself.")
        return "Hello world"

    def get_task_status(self, task_name: str) -> str:
        status = self.task_status.get(task_name, "unknown_task")
        # self.logger.debug(f"Reporting status for task '{task_name}': {status}")
        return status

    def get_all_task_status(self) -> Dict[str, str]:
        # self.logger.debug("Reporting all task statuses.")
        return self.task_status.copy() # Return a copy

    def reset_task_status(self, task_name: str):
        if task_name in self.task_status:
            # Allow resetting 'connect_to_local_service' as well
            self.task_status[task_name] = "not_started"
            self.logger.info(f"Task status for '{task_name}' reset to 'not_started'.")
        else:
            self.logger.warning(f"Attempted to reset status for unknown or non-resettable task: {task_name}")

    def reset_all_task_status(self):
        self.logger.info("Resetting all task statuses to 'not_started'.")
        for task_name in list(self.task_status.keys()): # Iterate over keys for safe modification
             self.task_status[task_name] = "not_started"
        # Ensure 'connect_to_local_service' starts as 'not_started' if it's reset.
        # This blanket reset is fine.

    async def check_service_health(self):
        self.logger.info(f"Starting service health check task for '{self.cloud_service_name}'.")
        await asyncio.sleep(5) # Initial delay before first check

        while True:
            health_check_passed = True
            try:
                # Check mirror service (cloud)
                if self.cloud_server and self.cloud_service_id:
                    # self.logger.debug(f"Checking health of cloud service '{self.cloud_service_id}'.")
                    service_proxy = await self.cloud_server.get_service(self.cloud_service_id)
                    hw_result = await asyncio.wait_for(service_proxy.hello_world(), timeout=10)
                    if hw_result != "Hello world":
                        self.logger.error(f"Cloud service health check failed (hello_world response: {hw_result}) for '{self.cloud_service_id}'")
                        health_check_passed = False
                else:
                    self.logger.warning("Cloud server/service_id not fully available, cannot check cloud service health.")
                    health_check_passed = False # Treat as failure to trigger recovery if needed

                # Check local service
                # self.logger.debug(f"Checking health of local service '{self.local_service_id}'.")
                if self.local_service is None:
                    self.logger.info(f"Local service '{self.local_service_id}' connection lost or not established, attempting to reconnect.")
                    if not await self.connect_to_local_service(): # connect_to_local_service returns bool
                        self.logger.error(f"Failed to reconnect to local service '{self.local_service_id}' during health check.")
                        health_check_passed = False
                    elif self.local_service is None: # Double check after connect attempt
                        self.logger.error(f"Local service '{self.local_service_id}' still None after reconnect attempt.")
                        health_check_passed = False
                
                if self.local_service: # If connected or reconnected
                    # Assumes local service also has a 'hello_world' method
                    # Also assumes the local service has a hello_world method. If not, this check needs adjustment.
                    local_hw_result = await asyncio.wait_for(self.local_service.hello_world(), timeout=10)
                    if local_hw_result != "Hello world":
                        self.logger.error(f"Local service '{self.local_service_id}' health check failed (hello_world response: {local_hw_result})")
                        health_check_passed = False
                        self.local_service = None # Assume connection is compromised
                elif health_check_passed: # If it was true but local_service is now None (e.g. failed reconnect)
                     health_check_passed = False


                if not health_check_passed:
                    raise Exception("One or more service health checks failed.")

                # self.logger.debug("Service health check passed for both cloud and local components.")

            except asyncio.TimeoutError as te:
                self.logger.error(f"Service health check timed out: {te}")
                health_check_passed = False
                # Try to identify which part timed out if possible, otherwise general failure
                self.local_service = None # Assume local connection might be the issue
                # No need to raise here, will fall into generic Exception and trigger recovery
            except Exception as e:
                self.logger.error(f"Service health check failed for '{self.cloud_service_name}': {e}. Attempting to rerun setup...")
                health_check_passed = False # Ensure it's marked as failed

            if not health_check_passed:
                self.logger.info("Attempting to recover by rerunning setup...")
                try:
                    if self.cloud_server and hasattr(self.cloud_server, 'disconnect'):
                        await self.cloud_server.disconnect()
                    if self.local_server and hasattr(self.local_server, 'disconnect'):
                        await self.local_server.disconnect()
                    if self.setup_task and not self.setup_task.done():
                        self.setup_task.cancel()
                        await asyncio.sleep(1) # Allow cancellation to propagate
                except Exception as disconnect_error:
                    self.logger.error(f"Error during disconnect phase of health check recovery: {disconnect_error}")
                finally:
                    self.cloud_server = None
                    self.local_server = None
                    self.local_service = None

                retry_count = 0
                max_retries = 30 
                while retry_count < max_retries:
                    retry_count += 1
                    self.logger.info(f"Attempting setup for '{self.cloud_service_name}', try {retry_count}/{max_retries}...")
                    try:
                        # Create new setup task
                        current_setup_task = asyncio.create_task(self.setup())
                        self.setup_task = current_setup_task # Store current task
                        await current_setup_task
                        self.logger.info(f"Setup successful for '{self.cloud_service_name}' after health check failure.")
                        break 
                    except Exception as setup_error:
                        self.logger.error(f"Failed to rerun setup for '{self.cloud_service_name}' (attempt {retry_count}): {setup_error}\\\\{traceback.format_exc()}")
                        if retry_count >= max_retries:
                            self.logger.error(f"Max retries for setup reached for '{self.cloud_service_name}'. Service might remain unhealthy.")
                            break
                        await asyncio.sleep(30)
            
            await asyncio.sleep(10) # Health check interval

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generic Hypha Mirror Service Runner")
    
    # Local service arguments
    parser.add_argument("--local-service-id", type=str, required=True, help="ID of the local service to mirror.")
    parser.add_argument("--local-server-url", type=str, default="http://reef.dyn.scilifelab.se:9527", help="URL of the local Hypha server.")
    parser.add_argument("--local-token-env-var", type=str, default="REEF_LOCAL_TOKEN", help="Environment variable name for the local server token.")

    # Cloud service arguments
    parser.add_argument("--cloud-service-id", type=str, required=True, help="Desired ID for the mirror service on the cloud.")
    parser.add_argument("--cloud-service-name", type=str, required=True, help="Display name for the mirror service on the cloud.")
    parser.add_argument("--cloud-server-url", type=str, default="https://hypha.aicell.io", help="URL of the cloud Hypha server.")
    parser.add_argument("--cloud-token-env-var", type=str, default="REEF_WORKSPACE_TOKEN", help="Environment variable name for the cloud server token.")
    parser.add_argument("--cloud-workspace", type=str, default="reef-imaging", help="Cloud Hypha workspace name.")

    # Mirroring configuration
    parser.add_argument("--methods-to-mirror", type=str, required=True, help="Comma-separated list of method names to mirror from the local service (e.g., 'initialize,get_status').")
    
    # Logging configuration
    parser.add_argument("--log-file-name", type=str, default="generic_mirror_service.log", help="Name for the log file.")
    parser.add_argument("--logger-name", type=str, default="GenericMirrorService", help="Name for the logger.")

    args = parser.parse_args()

    # Prepare methods_to_mirror list
    methods_list = [method.strip() for method in args.methods_to_mirror.split(',') if method.strip()]
    if not methods_list:
        print("Error: --methods-to-mirror cannot be empty or only whitespace.")
        exit(1)
    
    # Setup logging
    logger = setup_logging_generic(
        log_file_name=args.log_file_name,
        logger_name=args.logger_name
    )

    # Instantiate the service
    mirror_service = GenericMirrorService(
        local_service_id=args.local_service_id,
        local_server_url=args.local_server_url,
        local_token_env_var=args.local_token_env_var,
        cloud_service_id=args.cloud_service_id,
        cloud_service_name=args.cloud_service_name,
        cloud_server_url=args.cloud_server_url,
        cloud_token_env_var=args.cloud_token_env_var,
        cloud_workspace=args.cloud_workspace,
        methods_to_mirror=methods_list,
        logger_instance=logger
    )

    loop = asyncio.get_event_loop()

    async def main_runner():
        try:
            logger.info(f"Starting generic mirror service for local ID: {args.local_service_id} -> cloud ID: {args.cloud_service_id}")
            # Create setup task and store it on the instance
            mirror_service.setup_task = asyncio.create_task(mirror_service.setup())
            await mirror_service.setup_task 
            
            # Start the health check task after setup is presumably complete
            # The health check will also try to run setup if it fails initially or services become unhealthy
            asyncio.create_task(mirror_service.check_service_health())
            logger.info("Generic mirror service setup complete, health check running.")
        except Exception as e:
            logger.error(f"Critical error during initial setup or main_runner: {e}\\\\{traceback.format_exc()}")
            # Depending on desired behavior, could try to exit or let run_forever handle it.
            # If setup fails catastrophically (e.g. token missing), it might not recover.

    try:
        loop.create_task(main_runner())
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Service interrupted by user (KeyboardInterrupt). Shutting down.")
    except Exception as e:
        logger.critical(f"Unhandled exception in main event loop: {e}\\\\{traceback.format_exc()}")
    finally:
        logger.info("Cleaning up and closing event loop.")
        # Consider adding cleanup for asyncio tasks if necessary, e.g. cancelling them.
        # loop.close() # This might be problematic if other tasks are still scheduled by hypha
        pass 