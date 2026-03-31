import asyncio
import argparse
import os
from hypha_rpc import connect_to_server, login
from dorna2 import Dorna
import dotenv
from pydantic import Field
from hypha_rpc.utils.schema import schema_function
import logging
import logging.handlers
import time

dotenv.load_dotenv()  
ENV_FILE = dotenv.find_dotenv()  
if ENV_FILE:  
    dotenv.load_dotenv(ENV_FILE)  

# Set up logging

def setup_logging(log_file="robotic_arm_service.log", max_bytes=100000, backup_count=3):
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

class RoboticArmService:
    def __init__(self, local, simulation=False):
        self.local = local
        self.simulation = simulation
        self.server_url = "http://localhost:9527" if local else "https://hypha.aicell.io"
        self.robot = Dorna() if not simulation else None
        self.ip = "192.168.2.20"
        self.connected = False
        self.server = None
        self.service_id = "robotic-arm-control" + ("-simulation" if simulation else "")
        self.setup_task = None


    async def check_service_health(self):
        """Check if the service is healthy and rerun setup if needed"""
        while True:
            try:
                # Try to get the service status
                if self.service_id:
                    service = await self.server.get_service(self.service_id)
                    # Try a simple operation to verify service is working
                    ping_result = await service.ping()
                    if ping_result != "pong":
                        logger.error(f"Service health check failed: {ping_result}")
                        raise Exception("Service not healthy")
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
                        self.server = None  # Ensure server is set to None after disconnecting
                    if self.setup_task:
                        self.setup_task.cancel()  # Cancel the previous setup task
                        self.setup_task = None
                except Exception as disconnect_error:
                    logger.error(f"Error during disconnect: {disconnect_error}")
                finally:
                    self.server = None

                while True:
                    try:
                        # Rerun the setup method to reset Hypha service
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
            "name": "Robotic Arm Control",
            "id": self.service_id,  # Use the defined service ID
            "config": {
                "visibility": "protected",
                "run_in_executor": True
            },
            "ping": self.ping,
            # Unified transport API (recommended for new code)
            "transport_plate": self.transport_plate,

            # Device control
            "connect": self.connect,
            "disconnect": self.disconnect,
            "halt": self.halt,
            "get_all_joints": self.get_all_joints,
            "get_all_positions": self.get_all_positions,
            "set_alarm": self.set_alarm,
            "light_on": self.light_on,
            "light_off": self.light_off,
            # Action management
            "get_actions": self.get_actions,
            "execute_action": self.execute_action,
        })

        logger.info(f"Robotic arm control service registered at workspace: {server.config.workspace}, id: {svc.id}")
        logger.info(f'You can use this service using the service id: {svc.id}')
        id = svc.id.split(":")[1]
        logger.info(f"You can also test the service via the HTTP proxy: {self.server_url}/{server.config.workspace}/services/{id}/ping")

        # Health check will be started after setup is complete

    async def setup(self):
        if self.local:
            token = os.environ.get("REEF_LOCAL_TOKEN")
            server = await connect_to_server({"server_url": self.server_url, "token": token, "ping_interval": 30})
        else:
            try:
                token = os.environ.get("REEF_WORKSPACE_TOKEN")
            except:
                token = await login({"server_url": self.server_url})
            server = await connect_to_server({"server_url": self.server_url, "token": token, "workspace": "reef-imaging", "ping_interval": 30})

        self.server = server
        await self.start_hypha_service(server)

    def ping(self):
        """Ping function for health checks"""
        return "pong"

    # Maps service IDs to script file name prefixes
    _DEVICE_SCRIPT_NAMES = {
        "incubator": "incubator",
        "hamilton": "hamilton",
        "microscope-squid-1": "squid-1",
        "microscope-squid-2": "squid-2",
        "microscope-squid-plus-3": "squid-plus-3",
    }

    def _get_device_script_name(self, device: str) -> str:
        """
        Convert device service ID to script name.
        
        Supported device IDs:
        - 'incubator' - The Cytomat incubator
        - 'hamilton' - The Hamilton liquid handler  
        - 'microscope-squid-1' - Microscope 1
        - 'microscope-squid-2' - Microscope 2
        - 'microscope-squid-plus-3' - Microscope 3
        
        Returns the script name used in path files.
        """
        device = str(device).lower().strip()
        
        # Direct lookup for known devices
        if device in self._DEVICE_SCRIPT_NAMES:
            return self._DEVICE_SCRIPT_NAMES[device]
        
        raise Exception(f"Invalid device identifier: '{device}'. Supported: {list(self._DEVICE_SCRIPT_NAMES.keys())}")

    def _get_microscope_script_paths(self, microscope_id):
        """Legacy method - now uses unified device system."""
        script_name = self._get_device_script_name(f"microscope{microscope_id}")
        return {
            "label": f"microscope {microscope_id}",
            "grab": f"paths/grab_from_{script_name}.txt",
            "put": f"paths/put_on_{script_name}.txt",
        }

    def _play_script_sequence(self, script_paths):
        for script_path in script_paths:
            self.play_script(script_path)

    def _get_action_definitions(self):
        """Define predefined grab/put actions for manual operations."""
        return {
            "grab_from_incubator": {
                "name": "Grab from Incubator",
                "description": "Grab a sample from the incubator",
                "scripts": ["paths/grab_from_incubator.txt"],
            },
            "put_on_incubator": {
                "name": "Put on Incubator",
                "description": "Place a sample on the incubator",
                "scripts": ["paths/put_on_incubator.txt"],
            },
            "grab_from_hamilton": {
                "name": "Grab from Hamilton",
                "description": "Grab a sample from the Hamilton",
                "scripts": ["paths/grab_from_hamilton.txt"],
            },
            "put_on_hamilton": {
                "name": "Put on Hamilton",
                "description": "Place a sample on the Hamilton",
                "scripts": ["paths/put_on_hamilton.txt"],
            },
            "grab_from_squid-1": {
                "name": "Grab from Squid-1",
                "description": "Grab a sample from microscope 1",
                "scripts": ["paths/grab_from_squid-1.txt"],
            },
            "put_on_squid-1": {
                "name": "Put on Squid-1",
                "description": "Place a sample on microscope 1",
                "scripts": ["paths/put_on_squid-1.txt"],
            },
            "grab_from_squid-2": {
                "name": "Grab from Squid-2",
                "description": "Grab a sample from microscope 2",
                "scripts": ["paths/grab_from_squid-2.txt"],
            },
            "put_on_squid-2": {
                "name": "Put on Squid-2",
                "description": "Place a sample on microscope 2",
                "scripts": ["paths/put_on_squid-2.txt"],
            },
            "grab_from_squid-plus-3": {
                "name": "Grab from Squid-plus-3",
                "description": "Grab a sample from microscope 3",
                "scripts": ["paths/grab_from_squid-plus-3.txt"],
            },
            "put_on_squid-plus-3": {
                "name": "Put on Squid-plus-3",
                "description": "Place a sample on microscope 3",
                "scripts": ["paths/put_on_squid-plus-3.txt"],
            },
        }

    @schema_function(skip_self=True)
    def connect(self):
        """
        Connect and occupy the robot, so that it can be controlled.
        Automatically disables alarm if active (hardware safety reset).
        Returns: bool
        """
        try:
            if not self.simulation:
                self.robot.connect(self.ip)
            self.connected = True
            logger.info("Connected to robot")
            
            # Disable alarm if active (hardware safety reset on service restart)
            if not self.simulation:
                try:
                    alarm_status = self.robot.get_alarm()
                    if alarm_status:
                        logger.info("Alarm is active, disabling alarm...")
                        self.robot.set_alarm(0)
                        logger.info("Alarm disabled successfully")
                    else:
                        logger.info("Alarm is already disabled")
                except Exception as alarm_e:
                    logger.warning(f"Could not check/disable alarm: {alarm_e}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise e

    @schema_function(skip_self=True)
    def disconnect(self):
        """
        Disconnect the robot, so that it can be used by other clients.
        Returns: bool
        """
        try:
            if not self.simulation:
                self.robot.close()
            self.connected = False
            logger.info("Disconnected from robot")
            return True
        except Exception as e:
            logger.error(f"Failed to disconnect: {e}")
            raise e

    @schema_function(skip_self=True)
    def set_motor(self, state: int=Field(1, description="Enable or disable the motor, 1 for enable, 0 for disable")):
        if not self.connected:
            self.connect()
        if not self.simulation:
            self.robot.set_motor(state)
        else:
            time.sleep(10)
        return f"Motor set to {state}"

    @schema_function(skip_self=True)
    def play_script(self, script_path):
        if not self.connected:
            self.connect()
        if not self.simulation:
            result = self.robot.play_script(script_path)
            if result != 2:
                # Get additional diagnostic info
                try:
                    alarm_status = self.robot.get_alarm()
                    motor_status = self.robot.get_motor()
                    stat = self.robot.stat()
                    diag_info = f"alarm={alarm_status}, motor={motor_status}, stat={stat}"
                except:
                    diag_info = "could not retrieve diagnostic info"
                raise Exception(f"Error playing script '{script_path}': result={result} (expected 2), {diag_info}")
            else:
                return "Script played"
        else:
            time.sleep(10)
            return "Script played in simulation"
    
    @schema_function(skip_self=True)
    def get_all_joints(self):
        """
        Get the current position of all joints
        Returns: dict
        """
        try:
            if not self.connected:
                self.connect()
            if not self.simulation:
                result = self.robot.get_all_joint()
            else:
                time.sleep(10)
                result = {"joints": "Simulated"}
            return result
        except Exception as e:
            logger.error(f"Failed to get all joints: {e}")
            raise e

    @schema_function(skip_self=True)
    def get_all_positions(self):
        """
        Get the current position of all joints
        Returns: dict
        """
        try:
            if not self.connected:
                self.connect()
            if not self.simulation:
                result = self.robot.get_all_pose()
            else:
                time.sleep(10)
                result = {"positions": "Simulated"}
            return result
        except Exception as e:
            logger.error(f"Failed to get all positions: {e}")
            raise e



    @schema_function(skip_self=True)
    def transport_plate(self, from_device: str = Field(..., description="Source device: 'incubator', 'hamilton', 'microscope-squid-1', 'microscope-squid-2', 'microscope-squid-plus-3'"), 
                              to_device: str = Field(..., description="Target device: 'incubator', 'hamilton', 'microscope-squid-1', 'microscope-squid-2', 'microscope-squid-plus-3'")):
        """
        Unified transport API: Move a plate from any device to any other device.
        
        Supported device IDs:
        - 'incubator' - The Cytomat incubator
        - 'hamilton' - The Hamilton liquid handler
        - 'microscope-squid-1' - Microscope 1
        - 'microscope-squid-2' - Microscope 2
        - 'microscope-squid-plus-3' - Microscope 3
        
        Returns: bool
        """
        try:
            from_script = self._get_device_script_name(from_device)
            to_script = self._get_device_script_name(to_device)
        except Exception as e:
            logger.error(f"Invalid device identifier: {e}")
            raise e
        
        if from_script == to_script:
            raise Exception(f"Cannot transport from '{from_device}' to '{to_device}': same device")
        
        if not self.connected:
            self.connect()
        self.set_motor(1)
        
        try:
            grab_path = f"paths/grab_from_{from_script}.txt"
            put_path = f"paths/put_on_{to_script}.txt"
            
            self._play_script_sequence((grab_path, put_path))
            logger.info(f"Sample transported from '{from_device}' to '{to_device}'")
            return True
        except Exception as e:
            logger.error(f"Failed to transport sample from '{from_device}' to '{to_device}': {e}")
            raise e



    @schema_function(skip_self=True)
    def halt(self):
        """
        Halt/stop the robot, stop all the movements
        Returns: bool
        """
        try:
            if not self.connected:
                self.connect()
            self.robot.halt()
            logger.info("Robot halted")
            return True
        except Exception as e:
            logger.error(f"Failed to halt robot: {e}")
            raise e
    
    @schema_function(skip_self=True)
    def set_alarm(self, state: int=Field(1, description="Enable or disable the alarm, 1 for enable, 0 for disable")):
        """
        Set the alarm state
        """
        try:
            if not self.connected:
                self.connect()
            self.robot.set_alarm(state)
            return True
        except Exception as e:
            logger.error(f"Failed to set alarm: {e}")
            raise e

    @schema_function(skip_self=True)
    def light_on(self):
        """
        Turn on the light
        """
        try:
            if not self.connected:
                self.connect()
            self.robot.set_output(7, 0)
            return True
        except Exception as e:
            logger.error(f"Failed to turn on light: {e}")
            raise e

    @schema_function(skip_self=True)
    def light_off(self):    
        """
        Turn off the light
        """
        try:
            if not self.connected:
                self.connect()
            self.robot.set_output(7, 1)
            return True
        except Exception as e:
            logger.error(f"Failed to turn off light: {e}")
            raise e

    @schema_function(skip_self=True)
    def get_actions(self):
        """
        Get a list of predefined actions that can be executed by the robot.
        Each action has a name, description, and a list of positions.
        Returns: dict
        """
        import os
        import json
        
        actions = []
        base_dir = os.path.dirname(os.path.abspath(__file__))

        for action_id, action_info in self._get_action_definitions().items():
            positions = []
            speeds = []
            missing_script = False

            for script_path in action_info["scripts"]:
                file_path = os.path.join(base_dir, script_path)
                if not os.path.exists(file_path):
                    logger.warning(f"Skipping action {action_id}; missing script {script_path}")
                    missing_script = True
                    break

                with open(file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            try:
                                cmd = json.loads(line)
                                if cmd.get('cmd') == 'jmove':
                                    pos = [
                                        cmd.get('j0', 0),
                                        cmd.get('j1', 0),
                                        cmd.get('j2', 0),
                                        cmd.get('j3', 0),
                                        cmd.get('j4', 0),
                                        cmd.get('j5', 0)
                                    ]
                                    positions.append(pos)
                                    speeds.append(cmd.get('vel', 20))
                            except json.JSONDecodeError:
                                continue

            if missing_script:
                continue

            action = {
                "name": action_info["name"],
                "description": action_info["description"],
                "id": action_id,
                "positions": positions,
                "speeds": speeds
            }

            actions.append(action)
        
        return {"actions": actions}

    @schema_function(skip_self=True)
    def execute_action(self, action_id):
        """
        Execute a predefined action by its ID
        Returns: bool
        """
        if not self.connected:
            self.connect()

        action_definitions = self._get_action_definitions()
        if action_id not in action_definitions:
            logger.error(f"Unknown action ID: {action_id}")
            raise Exception("Unknown action ID")

        try:
            self.set_motor(1)
            self._play_script_sequence(action_definitions[action_id]["scripts"])
            logger.info(f"Action {action_id} executed successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to execute action {action_id}: {e}")
            raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the Hypha service for the robotic arm.")
    parser.add_argument('--local', action='store_true', help="Use localhost as server URL")
    parser.add_argument('--simulation', action='store_true', help="Run in simulation mode")
    args = parser.parse_args()

    robotic_arm_service = RoboticArmService(local=args.local, simulation=args.simulation)

    loop = asyncio.get_event_loop()

    async def main():
        try:
            robotic_arm_service.setup_task = asyncio.create_task(robotic_arm_service.setup())
            await robotic_arm_service.setup_task
            
            # Start the health check task after setup is complete
            asyncio.create_task(robotic_arm_service.check_service_health())
        except Exception as e:
            logger.error(f"Error setting up robotic arm service: {e}")
            raise e

    loop.create_task(main())
    loop.run_forever()
