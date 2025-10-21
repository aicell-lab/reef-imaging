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
            "move_sample_from_microscope1_to_incubator": self.move_sample_from_microscope1_to_incubator,
            "move_sample_from_incubator_to_microscope1": self.move_sample_from_incubator_to_microscope1,
            "grab_sample_from_microscope1": self.grab_sample_from_microscope1,
            "grab_sample_from_incubator": self.grab_sample_from_incubator,
            "put_sample_on_microscope1": self.put_sample_on_microscope1,
            "put_sample_on_incubator": self.put_sample_on_incubator,
            "transport_from_incubator_to_microscope1": self.transport_from_incubator_to_microscope1,
            "transport_to_incubator": self.transport_to_incubator,
            "connect": self.connect,
            "disconnect": self.disconnect,
            "halt": self.halt,
            "get_all_joints": self.get_all_joints,
            "get_all_positions": self.get_all_positions,

            "set_alarm": self.set_alarm,
            "light_on": self.light_on,
            "light_off": self.light_off,
            "get_actions": self.get_actions,
            "execute_action": self.execute_action,
            # Add microscope ID functions
            "incubator_to_microscope": self.incubator_to_microscope,
            "microscope_to_incubator": self.microscope_to_incubator
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

    @schema_function(skip_self=True)
    def connect(self):
        """
        Connect and occupy the robot, so that it can be controlled.
        Returns: bool
        """
        try:
            if not self.simulation:
                self.robot.connect(self.ip)
            self.connected = True
            logger.info("Connected to robot")
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
                raise Exception("Error playing script")
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
    def move_sample_from_microscope1_to_incubator(self):
        """
        Move sample from microscope1 to incubator, the microscope need to be homed before
        Returns: bool
        """
        self.set_motor(1)
        try:
            if not self.simulation:
                self.play_script("paths/microscope1_to_incubator.txt")
            self.play_script("paths/microscope1_to_incubator.txt")
            logger.info("Sample moved from microscope1 to incubator")
            return True
        except Exception as e:
            logger.error(f"Failed to move sample from microscope1 to incubator: {e}")
            raise e

    @schema_function(skip_self=True)
    def move_sample_from_incubator_to_microscope1(self):
        """
        Move sample from incubator to microscope1, microscope need to be homed before
        Returns: bool
        """
        self.set_motor(1)
        try:
            self.play_script("paths/incubator_to_microscope1.txt")
            logger.info("Sample moved from incubator to microscope1")
            return True
        except Exception as e:
            logger.error(f"Failed to move sample from incubator to microscope1: {e}")
            raise e

    @schema_function(skip_self=True)
    def grab_sample_from_microscope1(self):
        """
        Transport a sample from microscope1 to the incubator
        Returns: bool
        """
        self.set_motor(1)
        try:
            self.play_script("paths/grab_from_microscope1.txt")
            logger.info("Sample grabbed from microscope1")
            return True
        except Exception as e:
            logger.error(f"Failed to grab sample from microscope1: {e}")
            raise e

    @schema_function(skip_self=True)
    def grab_sample_from_incubator(self):
        """
        Grab a sample from the incubator
        Returns: bool
        """
        self.set_motor(1)
        try:
            self.play_script("paths/grab_from_incubator.txt")
            logger.info("Sample grabbed from incubator")
            return True
        except Exception as e:
            logger.error(f"Failed to grab sample from incubator: {e}")
            raise e

    @schema_function(skip_self=True)
    def put_sample_on_microscope1(self):
        """
        Place a sample on microscope1
        Returns: bool
        """
        self.set_motor(1)
        try:
            self.play_script("paths/put_on_microscope1.txt")
            logger.info("Sample placed on microscope1")
            return True
        except Exception as e:
            logger.error(f"Failed to put sample on microscope1: {e}")
            raise e

    @schema_function(skip_self=True)
    def put_sample_on_incubator(self):
        """
        Place a sample on the incubator.
        Returns: bool
        """
        self.set_motor(1)
        try:
            self.play_script("paths/put_on_incubator.txt")
            logger.info("Sample placed on incubator")
            return True
        except Exception as e:
            logger.error(f"Failed to put sample on incubator: {e}")
            raise e

    @schema_function(skip_self=True)
    def transport_from_incubator_to_microscope1(self):
        """
        Transport a sample from the incubator to microscope1
        Returns: bool
        """
        if not self.connected:
            self.connect()
        self.set_motor(1)
        try:
            self.play_script("paths/transport_from_incubator_to_microscope1.txt")
            logger.info("Sample moved from incubator to microscope1")
            return True
        except Exception as e:
            logger.error(f"Failed to transport sample from incubator to microscope1: {e}")
            raise e

    @schema_function(skip_self=True)
    def transport_to_incubator(self):
        """
        Transport a sample to the incubator (unified for all microscopes)
        Returns: bool
        """
        if not self.connected:
            self.connect()
        self.set_motor(1)
        try:
            self.play_script("paths/transport_to_incubator.txt")
            logger.info("Sample moved to incubator")
            return True
        except Exception as e:
            logger.error(f"Failed to transport sample to incubator: {e}")
            raise e
    
    @schema_function(skip_self=True)
    def incubator_to_microscope(self, microscope_id=1):
        """
        Move a sample from the incubator to microscopes
        Returns: bool
        """
        if not self.connected:
            self.connect()
        self.set_motor(1)
        try:
            if microscope_id == 1:
                self.play_script("paths/grab_from_incubator.txt")
                self.play_script("paths/transport_from_incubator_to_microscope1.txt")
                self.play_script("paths/put_on_microscope1.txt")
                logger.info(f"Sample moved from incubator to microscope 1")
            elif microscope_id == 2:
                self.play_script("paths/grab_from_incubator.txt")
                self.play_script("paths/transport_from_incubator_to_microscope2.txt")
                self.play_script("paths/put_on_microscope2.txt")
                logger.info(f"Sample moved from incubator to microscope 2")
            elif microscope_id == 3:  # squid+1 microscope
                self.play_script("paths/grab_from_incubator.txt")
                self.play_script("paths/transport_from_incubator_to_squid+1.txt")
                self.play_script("paths/put_on_squid+1.txt")
                logger.info(f"Sample moved from incubator to squid+1 microscope")
            else:
                logger.error(f"Invalid microscope ID: {microscope_id}")
                raise Exception(f"Invalid microscope ID: {microscope_id}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to move sample from incubator to microscope {microscope_id}: {e}")
            raise e
    
    @schema_function(skip_self=True)
    def microscope_to_incubator(self, microscope_id=1):
        """
        Move a sample from microscopes to the incubator
        Returns: bool
        """
        if not self.connected:
            self.connect()
        self.set_motor(1)
        try:
            if microscope_id == 1:
                self.play_script("paths/grab_from_microscope1.txt")
                self.play_script("paths/transport_to_incubator.txt")
                self.play_script("paths/put_on_incubator.txt")
                logger.info(f"Sample moved from microscope 1 to incubator")
            elif microscope_id == 2:
                self.play_script("paths/grab_from_microscope2.txt")
                self.play_script("paths/transport_to_incubator.txt")
                self.play_script("paths/put_on_incubator.txt")
                logger.info(f"Sample moved from microscope 2 to incubator")
            elif microscope_id == 3:  # squid+1 microscope
                self.play_script("paths/grab_from_squid+1.txt")
                self.play_script("paths/transport_to_incubator.txt")
                self.play_script("paths/put_on_incubator.txt")
                logger.info(f"Sample moved from squid+1 microscope to incubator")
            else:
                logger.error(f"Invalid microscope ID: {microscope_id}")
                raise Exception(f"Invalid microscope ID: {microscope_id}")
                
            return True
        except Exception as e:
            logger.error(f"Failed to move sample from microscope {microscope_id} to incubator: {e}")
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
        paths_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "paths")
        
        # Define action mappings
        action_mappings = {
            "grab_from_incubator.txt": {
                "name": "Grab from Incubator",
                "description": "Grab a sample from the incubator",
                "id": "grab_from_incubator"
            },
            "put_on_incubator.txt": {
                "name": "Put on Incubator",
                "description": "Place a sample on the incubator",
                "id": "put_on_incubator"
            },
            "put_on_microscope1.txt": {
                "name": "Put on Microscope 1",
                "description": "Place a sample on microscope 1",
                "id": "put_on_microscope1"
            },
            "grab_from_microscope1.txt": {
                "name": "Grab from Microscope 1",
                "description": "Grab a sample from microscope 1",
                "id": "grab_from_microscope1"
            },
            "transport_from_incubator_to_microscope1.txt": {
                "name": "Transport from Incubator to Microscope 1",
                "description": "Transport a sample from the incubator to microscope 1",
                "id": "transport_from_incubator_to_microscope1"
            },
            "transport_to_incubator.txt": {
                "name": "Transport to Incubator",
                "description": "Transport a sample to the incubator (unified for all microscopes)",
                "id": "transport_to_incubator"
            },
            "incubator_to_microscope1.txt": {
                "name": "Move from Incubator to Microscope 1",
                "description": "Move a sample from the incubator to microscope 1 (complete sequence)",
                "id": "incubator_to_microscope1"
            },
            "microscope1_to_incubator.txt": {
                "name": "Move from Microscope 1 to Incubator",
                "description": "Move a sample from microscope 1 to the incubator (complete sequence)",
                "id": "microscope1_to_incubator"
            }
        }
        
        # Process each path file
        for filename, action_info in action_mappings.items():
            file_path = os.path.join(paths_dir, filename)
            if os.path.exists(file_path):
                positions = []
                speeds = []
                
                with open(file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            try:
                                cmd = json.loads(line)
                                if cmd.get('cmd') == 'jmove':
                                    # Extract position data
                                    pos = [
                                        cmd.get('j0', 0),
                                        cmd.get('j1', 0),
                                        cmd.get('j2', 0),
                                        cmd.get('j3', 0),
                                        cmd.get('j4', 0),
                                        cmd.get('j5', 0)
                                    ]
                                    positions.append(pos)
                                    
                                    # Extract speed if available, otherwise use default of 20
                                    speed = cmd.get('vel', 20)
                                    speeds.append(speed)
                            except json.JSONDecodeError:
                                continue
                
                # Create action object
                action = {
                    "name": action_info["name"],
                    "description": action_info["description"],
                    "id": action_info["id"],
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
            
        # Map action IDs to script paths
        action_to_script = {
            "grab_from_incubator": "paths/grab_from_incubator.txt",
            "put_on_incubator": "paths/put_on_incubator.txt",
            "put_on_microscope1": "paths/put_on_microscope1.txt",
            "grab_from_microscope1": "paths/grab_from_microscope1.txt",
            "transport_from_incubator_to_microscope1": "paths/transport_from_incubator_to_microscope1.txt",
            "transport_to_incubator": "paths/transport_to_incubator.txt",
            "incubator_to_microscope1": "paths/incubator_to_microscope1.txt",
            "microscope1_to_incubator": "paths/microscope1_to_incubator.txt",
            "put_on_squid+1": "paths/put_on_squid+1.txt",
            "grab_from_squid+1": "paths/grab_from_squid+1.txt",
            "transport_from_incubator_to_squid+1": "paths/transport_from_incubator_to_squid+1.txt",
        }
        
        if action_id not in action_to_script:
            logger.error(f"Unknown action ID: {action_id}")
            raise Exception("Unknown action ID")
            
        script_path = action_to_script[action_id]
        try:
            self.set_motor(1)
            result = self.robot.play_script(script_path)
            if result != 2:
                raise Exception("Error playing script")
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
