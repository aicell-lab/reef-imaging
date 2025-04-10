import os
import logging
import logging.handlers
import time
import argparse
import asyncio
import fractions
from functools import partial
import traceback
import numpy as np
from hypha_rpc import login, connect_to_server, register_rtc_service
import json
import cv2
import dotenv
import sys
import io
from PIL import Image  
# Now you can import squid_control
from squid_control.squid_controller import SquidController
from pydantic import Field, BaseModel
from typing import List, Optional
from hypha_tools.hypha_storage import HyphaDataStore
from hypha_tools.chatbot.aask import aask
import base64
from pydantic import Field
from hypha_rpc.utils.schema import schema_function

dotenv.load_dotenv()  
ENV_FILE = dotenv.find_dotenv()  
if ENV_FILE:  
    dotenv.load_dotenv(ENV_FILE)  

# Set up logging

def setup_logging(log_file="squid_control_service.log", max_bytes=100000, backup_count=3):
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

class Microscope:
    def __init__(self, is_simulation, is_local):
        self.login_required = True
        self.current_x = 0
        self.current_y = 0
        self.current_z = 0
        self.current_theta = 0
        self.current_illumination_channel = None
        self.current_intensity = None
        self.is_illumination_on = False
        self.chatbot_service_url = None
        self.is_simulation = is_simulation
        self.is_local = is_local
        self.squidController = SquidController(is_simulation=is_simulation)
        self.dx = 1
        self.dy = 1
        self.dz = 1
        self.BF_intensity_exposure = [50, 100]
        self.F405_intensity_exposure = [50, 100]
        self.F488_intensity_exposure = [50, 100]
        self.F561_intensity_exposure = [50, 100]
        self.F638_intensity_exposure = [50, 100]
        self.F730_intensity_exposure = [50, 100]
        self.parameters = {
            'current_x': self.current_x,
            'current_y': self.current_y,
            'current_z': self.current_z,
            'current_theta': self.current_theta,
            'is_illumination_on': self.is_illumination_on,
            'dx': self.dx,
            'dy': self.dy,
            'dz': self.dz,
            'BF_intensity_exposure': self.BF_intensity_exposure,
            'F405_intensity_exposure': self.F405_intensity_exposure,
            'F488_intensity_exposure': self.F488_intensity_exposure,
            'F561_intensity_exposure': self.F561_intensity_exposure,
            'F638_intensity_exposure': self.F638_intensity_exposure,
            'F730_intensity_exposure': self.F730_intensity_exposure,
        }
        self.authorized_emails = self.load_authorized_emails(self.login_required)
        logger.info(f"Authorized emails: {self.authorized_emails}")
        self.datastore = None
        self.server_url = "http://reef.dyn.scilifelab.se:9527" if is_local else "https://hypha.aicell.io/"
        self.server = None
        self.service_id = os.environ.get("MICROSCOPE_SERVICE_ID")
        self.setup_task = None  # Track the setup task
        # Add task status tracking
        self.task_status = {
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

    def load_authorized_emails(self, login_required=True):
        if login_required:
            authorized_users_path = os.environ.get("BIOIMAGEIO_AUTHORIZED_USERS_PATH")
            if authorized_users_path:
                assert os.path.exists(
                    authorized_users_path
                ), f"The authorized users file is not found at {authorized_users_path}"
                with open(authorized_users_path, "r") as f:
                    authorized_users = json.load(f)["users"]
                authorized_emails = [
                    user["email"] for user in authorized_users if "email" in user
                ]
            else:
                authorized_emails = None
        else:
            authorized_emails = None
        return authorized_emails

    def check_permission(self, user):
        if user['is_anonymous']:
            return False
        if self.authorized_emails is None or user["email"] in self.authorized_emails:
            return True
        else:
            return False

    async def ping(self, context=None):
        if self.login_required and context and context.get("user"):
            assert self.check_permission(
                context.get("user")
            ), "You don't have permission to use the chatbot, please sign up and wait for approval"
        return "pong"

    def get_task_status(self, task_name):
        """Get the status of a specific task"""
        return self.task_status.get(task_name, "unknown")
    
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
    def hello_world(self):
        """Hello world"""
        task_name = "hello_world"
        self.task_status[task_name] = "started"
        self.task_status[task_name] = "finished"
        return "Hello world"
    
    @schema_function(skip_self=True)
    def move_by_distance(self, x: float=Field(1.0, description="disntance through X axis, unit: milimeter"), y: float=Field(1.0, description="disntance through Y axis, unit: milimeter"), z: float=Field(1.0, description="disntance through Z axis, unit: milimeter"), context=None):
        """
        Move the stage by a distances in x, y, z axis
        Returns: Result information
        """
        task_name = "move_by_distance"
        self.task_status[task_name] = "started"
        try:
            is_success, x_pos, y_pos, z_pos, x_des, y_des, z_des = self.squidController.move_by_distance_limited(x, y, z)
            if is_success:
                result = f'The stage moved ({x},{y},{z})mm through x,y,z axis, from ({x_pos},{y_pos},{z_pos})mm to ({x_des},{y_des},{z_des})mm'
                self.task_status[task_name] = "finished"
                return {
                    "success": True,
                    "message": result,
                    "initial_position": {"x": x_pos, "y": y_pos, "z": z_pos},
                    "final_position": {"x": x_des, "y": y_des, "z": z_des}
                }
            else:
                result = f'The stage cannot move ({x},{y},{z})mm through x,y,z axis, from ({x_pos},{y_pos},{z_pos})mm to ({x_des},{y_des},{z_des})mm because out of the range.'
                self.task_status[task_name] = "failed"
                return {
                    "success": False,
                    "message": result,
                    "initial_position": {"x": x_pos, "y": y_pos, "z": z_pos},
                    "attempted_position": {"x": x_des, "y": y_des, "z": z_des}
                }
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to move by distance: {e}")
            return {
                "success": False,
                "message": f"Failed to move by distance: {e}"
            }

    @schema_function(skip_self=True)
    def move_to_position(self, x:float=Field(1.0,description="Unit: milimeter"), y:float=Field(1.0,description="Unit: milimeter"), z:float=Field(1.0,description="Unit: milimeter"), context=None):
        """
        Move the stage to a position in x, y, z axis
        Returns: The result of the movement
        """
        task_name = "move_to_position"
        self.task_status[task_name] = "started"
        try:
            self.get_status()
            initial_x = self.parameters['current_x']
            initial_y = self.parameters['current_y']
            initial_z = self.parameters['current_z']

            if x != 0:
                is_success, x_pos, y_pos, z_pos, x_des = self.squidController.move_x_to_limited(x)
                if not is_success:
                    self.task_status[task_name] = "failed"
                    return {
                        "success": False,
                        "message": f'The stage cannot move to position ({x},{y},{z})mm from ({initial_x},{initial_y},{initial_z})mm because out of the limit of X axis.',
                        "initial_position": {"x": initial_x, "y": initial_y, "z": initial_z},
                        "final_position": {"x": x_pos, "y": y_pos, "z": z_pos}
                    }

            if y != 0:
                is_success, x_pos, y_pos, z_pos, y_des = self.squidController.move_y_to_limited(y)
                if not is_success:
                    self.task_status[task_name] = "failed"
                    return {
                        "success": False,
                        "message": f'X axis moved successfully, the stage is now at ({x_pos},{y_pos},{z_pos})mm. But aimed position is out of the limit of Y axis and the stage cannot move to position ({x},{y},{z})mm.',
                        "initial_position": {"x": initial_x, "y": initial_y, "z": initial_z},
                        "final_position": {"x": x_pos, "y": y_pos, "z": z_pos}
                    }

            if z != 0:
                is_success, x_pos, y_pos, z_pos, z_des = self.squidController.move_z_to_limited(z)
                if not is_success:
                    self.task_status[task_name] = "failed"
                    return {
                        "success": False,
                        "message": f'X and Y axis moved successfully, the stage is now at ({x_pos},{y_pos},{z_pos})mm. But aimed position is out of the limit of Z axis and the stage cannot move to position ({x},{y},{z})mm.',
                        "initial_position": {"x": initial_x, "y": initial_y, "z": initial_z},
                        "final_position": {"x": x_pos, "y": y_pos, "z": z_pos}
                    }

            self.task_status[task_name] = "finished"
            return {
                "success": True,
                "message": f'The stage moved to position ({x},{y},{z})mm from ({initial_x},{initial_y},{initial_z})mm successfully.',
                "initial_position": {"x": initial_x, "y": initial_y, "z": initial_z},
                "final_position": {"x": x_pos, "y": y_pos, "z": z_pos}
            }
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to move to position: {e}")
            return {
                "success": False,
                "message": f"Failed to move to position: {e}"
            }

    @schema_function(skip_self=True)
    def get_status(self, context=None):
        """
        Get the current status of the microscope
        Returns: Status of the microscope
        """
        task_name = "get_status"
        self.task_status[task_name] = "started"
        try:
            current_x, current_y, current_z, current_theta = self.squidController.navigationController.update_pos(microcontroller=self.squidController.microcontroller)
            is_illumination_on = self.squidController.liveController.illumination_on
            scan_channel = self.squidController.multipointController.selected_configurations
            is_busy = self.squidController.is_busy
            self.parameters = {
                'is_busy': is_busy,
                'current_x': current_x,
                'current_y': current_y,
                'current_z': current_z,
                'current_theta': current_theta,
                'is_illumination_on': is_illumination_on,
                'dx': self.dx,
                'dy': self.dy,
                'dz': self.dz,
                'BF_intensity_exposure': self.BF_intensity_exposure,
                'F405_intensity_exposure': self.F405_intensity_exposure,
                'F488_intensity_exposure': self.F488_intensity_exposure,
                'F561_intensity_exposure': self.F561_intensity_exposure,
                'F638_intensity_exposure': self.F638_intensity_exposure,
                'F730_intensity_exposure': self.F730_intensity_exposure,
            }
            self.task_status[task_name] = "finished"
            return self.parameters
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get status: {e}")
            return {}

    @schema_function(skip_self=True)
    def update_parameters_from_client(self, new_parameters: dict=Field(description="the dictionary parameters user want to update"), context=None):
        """
        Update the parameters from the client side
        Returns: Updated parameters in the microscope
        """
        task_name = "update_parameters_from_client"
        self.task_status[task_name] = "started"
        try:
            if self.parameters is None:
                self.parameters = {}

            # Update only the specified keys
            for key, value in new_parameters.items():
                if key in self.parameters:
                    self.parameters[key] = value
                    logger.info(f"Updated {key} to {value}")

                    # Update the corresponding instance variable if it exists
                    if hasattr(self, key):
                        setattr(self, key, value)
                    else:
                        logger.error(f"Attribute {key} does not exist on self, skipping update.")
                else:
                    logger.error(f"Key {key} not found in parameters, skipping update.")

            self.task_status[task_name] = "finished"
            return {"success": True, "message": "Parameters updated successfully.", "updated_parameters": new_parameters}
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to update parameters: {e}")
            return {"success": False, "message": f"Failed to update parameters: {e}"}

    @schema_function(skip_self=True)
    async def one_new_frame(self, exposure_time: int=Field(100, description="Exposure time in milliseconds"), channel: int=Field(0, description="Light source (0 for Bright Field, Fluorescence channels: 11 for 405 nm, 12 for 488 nm, 13 for 638nm, 14 for 561 nm, 15 for 730 nm)"), intensity: int=Field(50, description="Light intensity"), context=None):
        """
        Get an image from the microscope
        Returns: A base64 encoded image
        """
        task_name = "one_new_frame"
        self.task_status[task_name] = "started"
        try:
            gray_img = await self.squidController.snap_image(channel, intensity, exposure_time)

            min_val = np.min(gray_img)
            max_val = np.max(gray_img)
            if max_val > min_val:  # Avoid division by zero if the image is completely uniform
                gray_img = (gray_img - min_val) * (255 / (max_val - min_val))
                gray_img = gray_img.astype(np.uint8)  # Convert to 8-bit image
                #resize to 512x512
                resized_img = cv2.resize(gray_img, (512, 512))
            else:
                gray_img = np.zeros((512, 512), dtype=np.uint8)  # If no variation, return a black image

            gray_img = Image.fromarray(gray_img)
            gray_img = gray_img.convert("L")  # Convert to grayscale  
            # Save the image to a BytesIO object as PNG  
            buffer = io.BytesIO()  
            gray_img.save(buffer, format="PNG")  
            buffer.seek(0) 
            image_base64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
            
            #update the current illumination channel and intensity
            if channel == 0:
                self.BF_intensity_exposure = [intensity, exposure_time]
            elif channel == 11:
                self.F405_intensity_exposure = [intensity, exposure_time]
            elif channel == 12:
                self.F488_intensity_exposure = [intensity, exposure_time]
            elif channel == 13:
                self.F561_intensity_exposure = [intensity, exposure_time]
            elif channel == 14:
                self.F638_intensity_exposure = [intensity, exposure_time]
            elif channel == 15:
                self.F730_intensity_exposure = [intensity, exposure_time]
            self.get_status()
            self.task_status[task_name] = "finished"
            return image_base64  
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get new frame: {e}")
            return None

    @schema_function(skip_self=True)
    async def snap(self, exposure_time: int=Field(100, description="Exposure time, in milliseconds"), channel: int=Field(0, description="Light source (0 for Bright Field, Fluorescence channels: 11 for 405 nm, 12 for 488 nm, 13 for 638nm, 14 for 561 nm, 15 for 730 nm)"), intensity: int=Field(50, description="Intensity of the illumination source"), context=None):
        """
        Get an image from microscope
        Returns: the URL of the image
        """
        task_name = "snap"
        self.task_status[task_name] = "started"
        try:
            gray_img = await self.squidController.snap_image(channel, intensity, exposure_time)
            logger.info('The image is snapped')
            gray_img = gray_img.astype(np.uint8)
            # Resize the image to a standard size
            resized_img = cv2.resize(gray_img, (2048, 2048))

            # Encode the image directly to PNG without converting to BGR
            _, png_image = cv2.imencode('.png', resized_img)

            # Store the PNG image
            file_id = self.datastore.put('file', png_image.tobytes(), 'snapshot.png', "Captured microscope image in PNG format")
            data_url = self.datastore.get_url(file_id)
            logger.info(f'The image is snapped and saved as {data_url}')
            
            #update the current illumination channel and intensity
            if channel == 0:
                self.BF_intensity_exposure = [intensity, exposure_time]
            elif channel == 11:
                self.F405_intensity_exposure = [intensity, exposure_time]
            elif channel == 12:
                self.F488_intensity_exposure = [intensity, exposure_time]
            elif channel == 13:
                self.F561_intensity_exposure = [intensity, exposure_time]
            elif channel == 14:
                self.F638_intensity_exposure = [intensity, exposure_time]
            elif channel == 15:
                self.F730_intensity_exposure = [intensity, exposure_time]
            self.get_status()
            self.task_status[task_name] = "finished"
            return data_url
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to snap image: {e}")
            return None

    @schema_function(skip_self=True)
    def open_illumination(self, context=None):
        """
        Turn on the illumination
        Returns: The message of the action
        """
        task_name = "open_illumination"
        self.task_status[task_name] = "started"
        try:
            self.squidController.liveController.turn_on_illumination()
            logger.info('Bright field illumination turned on.')
            self.task_status[task_name] = "finished"
            return 'Bright field illumination turned on.'
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to open illumination: {e}")
            return f"Failed to open illumination: {e}"

    @schema_function(skip_self=True)
    def close_illumination(self, context=None):
        """
        Turn off the illumination
        Returns: The message of the action
        """
        task_name = "close_illumination"
        self.task_status[task_name] = "started"
        try:
            self.squidController.liveController.turn_off_illumination()
            logger.info('Bright field illumination turned off.')
            self.task_status[task_name] = "finished"
            return 'Bright field illumination turned off.'
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to close illumination: {e}")
            return f"Failed to close illumination: {e}"

    @schema_function(skip_self=True)
    def scan_well_plate(self, well_plate_type: str=Field("96", description="Type of the well plate (e.g., '6', '12', '24', '96', '384')"), illuminate_channels: List[str]=Field(default_factory=lambda: ['BF LED matrix full','Fluorescence 488 nm Ex','Fluorescence 561 nm Ex'], description="Light source to illuminate the well plate"), do_contrast_autofocus: bool=Field(False, description="Whether to do contrast based autofocus"), do_reflection_af: bool=Field(True, description="Whether to do reflection based autofocus"), scanning_zone: List[tuple]=Field(default_factory=lambda: [(0,0),(0,0)], description="The scanning zone of the well plate, for 91 well plate, it should be[(0,0),(7,11)] "), Nx: int=Field(3, description="Number of columns to scan"), Ny: int=Field(3, description="Number of rows to scan"), action_ID: str=Field('testPlateScan', description="The ID of the action"), context=None):
        """
        Scan the well plate according to the pre-defined position list
        Returns: The message of the action
        """
        task_name = "scan_well_plate"
        self.task_status[task_name] = "started"
        try:
            if illuminate_channels is None:
                illuminate_channels = ['BF LED matrix full','Fluorescence 488 nm Ex','Fluorescence 561 nm Ex']
            logger.info("Start scanning well plate")
            self.squidController.plate_scan(well_plate_type, illuminate_channels, do_contrast_autofocus, do_reflection_af, scanning_zone, Nx, Ny, action_ID)
            logger.info("Well plate scanning completed")
            self.task_status[task_name] = "finished"
            return "Well plate scanning completed"
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to scan well plate: {e}")
            return f"Failed to scan well plate: {e}"
    
    @schema_function(skip_self=True)
    def scan_well_plate_simulated(self, context=None):
        """
        Scan the well plate according to the pre-defined position list
        Returns: The message of the action
        """
        task_name = "scan_well_plate_simulated"
        self.task_status[task_name] = "started"
        try:
            time.sleep(600)
            self.task_status[task_name] = "finished"
            return "Well plate scanning completed"
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to scan well plate: {e}")
            return f"Failed to scan well plate: {e}"


    @schema_function(skip_self=True)
    def set_illumination(self, channel: int=Field(0, description="Light source (e.g., 0 for Bright Field, Fluorescence channels: 11 for 405 nm, 12 for 488 nm, 13 for 638nm, 14 for 561 nm, 15 for 730 nm)"), intensity: int=Field(50, description="Intensity of the illumination source"), context=None):
        """
        Set the intensity of light source
        Returns:A string message
        """
        task_name = "set_illumination"
        self.task_status[task_name] = "started"
        try:
            self.squidController.liveController.set_illumination(channel, intensity)
            logger.info(f'The intensity of the channel {channel} illumination is set to {intensity}.')
            self.task_status[task_name] = "finished"
            return f'The intensity of the channel {channel} illumination is set to {intensity}.'
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to set illumination: {e}")
            return f"Failed to set illumination: {e}"
    
    @schema_function(skip_self=True)
    def set_camera_exposure(self, exposure_time: int=Field(100, description="Exposure time in milliseconds"), context=None):
        """
        Set the exposure time of the camera
        Returns: A string message
        """
        task_name = "set_camera_exposure"
        self.task_status[task_name] = "started"
        try:
            self.squidController.camera.set_exposure_time(exposure_time)
            logger.info(f'The exposure time of the camera is set to {exposure_time}.')
            self.task_status[task_name] = "finished"
            return f'The exposure time of the camera is set to {exposure_time}.'
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to set camera exposure: {e}")
            return f"Failed to set camera exposure: {e}"

    @schema_function(skip_self=True)
    def stop_scan(self, context=None):
        """
        Stop the scanning of the well plate.
        Returns: A string message
        """
        task_name = "stop_scan"
        self.task_status[task_name] = "started"
        try:
            self.squidController.liveController.stop_live()
            self.multipointController.abort_acqusition_requested=True
            logger.info("Stop scanning well plate")
            self.task_status[task_name] = "finished"
            return "Stop scanning well plate"
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to stop scan: {e}")
            return f"Failed to stop scan: {e}"

    @schema_function(skip_self=True)
    def home_stage(self, context=None):
        """
        Move the stage to home/zero position
        Returns: A string message
        """
        task_name = "home_stage"
        self.task_status[task_name] = "started"
        try:
            self.squidController.home_stage()
            logger.info('The stage moved to home position in z, y, and x axis')
            self.task_status[task_name] = "finished"
            return 'The stage moved to home position in z, y, and x axis'
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to home stage: {e}")
            return f"Failed to home stage: {e}"
    
    @schema_function(skip_self=True)
    def return_stage(self,context=None):
        """
        Move the stage to the initial position for imaging.
        Returns: A string message
        """
        task_name = "return_stage"
        self.task_status[task_name] = "started"
        try:
            self.squidController.return_stage()
            logger.info('The stage moved to the initial position')
            self.task_status[task_name] = "finished"
            return 'The stage moved to the initial position'
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to return stage: {e}")
            return f"Failed to return stage: {e}"
    
    @schema_function(skip_self=True)
    def move_to_loading_position(self, context=None):
        """
        Move the stage to the loading position.
        Returns: A  string message
        """
        task_name = "move_to_loading_position"
        self.task_status[task_name] = "started"
        try:
            self.squidController.slidePositionController.move_to_slide_loading_position()
            logger.info('The stage moved to loading position')
            self.task_status[task_name] = "finished"
            return 'The stage moved to loading position'
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to move to loading position: {e}")
            return f"Failed to move to loading position: {e}"

    @schema_function(skip_self=True)
    def auto_focus(self, context=None):
        """
        Do contrast-based autofocus
        Returns: A string message
        """
        task_name = "auto_focus"
        self.task_status[task_name] = "started"
        try:
            self.squidController.do_autofocus()
            logger.info('The camera is auto-focused')
            self.task_status[task_name] = "finished"
            return 'The camera is auto-focused'
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to auto focus: {e}")
            return f"Failed to auto focus: {e}"
    
    @schema_function(skip_self=True)
    def do_laser_autofocus(self, context=None):
        """
        Do reflection-based autofocus
        Returns: A string message
        """
        task_name = "do_laser_autofocus"
        self.task_status[task_name] = "started"
        try:
            self.squidController.do_laser_autofocus()
            logger.info('The camera is auto-focused')
            self.task_status[task_name] = "finished"
            return 'The camera is auto-focused'
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to do laser autofocus: {e}")
            return f"Failed to do laser autofocus: {e}"

    @schema_function(skip_self=True)
    def navigate_to_well(self, row: str=Field('A', description="Row number of the well position (e.g., 'A')"), col: int=Field(1, description="Column number of the well position"), wellplate_type: str=Field('96', description="Type of the well plate (e.g., '6', '12', '24', '96', '384')"), context=None):
        """
        Navigate to the specified well position in the well plate.
        Returns: A string message
        """
        task_name = "navigate_to_well"
        self.task_status[task_name] = "started"
        try:
            if wellplate_type is None:
                wellplate_type = '96'
            self.squidController.move_to_well(row, col, wellplate_type)
            logger.info(f'The stage moved to well position ({row},{col})')
            self.task_status[task_name] = "finished"
            return f'The stage moved to well position ({row},{col})'
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to navigate to well: {e}")
            return f"Failed to navigate to well: {e}"

    @schema_function(skip_self=True)
    def get_chatbot_url(self, context=None):
        """
        Get the URL of the chatbot service.
        Returns: A URL string
        """
        task_name = "get_chatbot_url"
        self.task_status[task_name] = "started"
        try:
            logger.info(f"chatbot_service_url: {self.chatbot_service_url}")
            self.task_status[task_name] = "finished"
            return self.chatbot_service_url
        except Exception as e:
            self.task_status[task_name] = "failed"
            logger.error(f"Failed to get chatbot URL: {e}")
            return None
    
    class MoveByDistanceInput(BaseModel):
        """Move the stage by a distance in x, y, z axis."""
        x: float = Field(0, description="Move the stage along X axis")
        y: float = Field(0, description="Move the stage along Y axis")
        z: float = Field(0, description="Move the stage along Z axis")

    class MoveToPositionInput(BaseModel):
        """Move the stage to a position in x, y, z axis."""
        x: Optional[float] = Field(None, description="Move the stage to the X coordinate")
        y: Optional[float] = Field(None, description="Move the stage to the Y coordinate")
        z: float = Field(3.35, description="Move the stage to the Z coordinate")

    class AutoFocusInput(BaseModel):
        """Reflection based autofocus."""
        N: int = Field(10, description="Number of discrete focus positions")
        delta_Z: float = Field(1.524, description="Step size in the Z-axis in micrometers")

    class SnapImageInput(BaseModel):
        """Snap an image from the camera, and display it in the chatbot."""
        exposure: int = Field(..., description="Exposure time in milliseconds")
        channel: int = Field(..., description="Light source (e.g., 0 for Bright Field, Fluorescence channels: 11 for 405 nm, 12 for 488 nm, 13 for 638nm, 14 for 561 nm, 15 for 730 nm)")
        intensity: int = Field(..., description="Intensity of the illumination source")

    class InspectToolInput(BaseModel):
        """Inspect the images with GPT4-o's vision model."""
        images: List[dict] = Field(..., description="A list of images to be inspected, each with a http url and title")
        query: str = Field(..., description="User query about the image")
        context_description: str = Field(..., description="Context for the visual inspection task, inspect images taken from the microscope")

    class NavigateToWellInput(BaseModel):
        """Navigate to a well position in the well plate."""
        row: str = Field(..., description="Row number of the well position (e.g., 'A')")
        col: int = Field(..., description="Column number of the well position")
        wellplate_type: str = Field('24', description="Type of the well plate (e.g., '6', '12', '24', '96', '384')")

    class MoveToLoadingPositionInput(BaseModel):
        """Move the stage to the loading position."""

    class HomeStageInput(BaseModel):
        """Home the stage in z, y, and x axis."""

    class ReturnStageInput(BaseModel):
        """Return the stage to the initial position."""

    class ImageInfo(BaseModel):
        """Image information."""
        url: str = Field(..., description="The URL of the image.")
        title: Optional[str] = Field(None, description="The title of the image.")

    async def inspect_tool(self, images: List[dict], query: str, context_description: str) -> str:
        image_infos = [self.ImageInfo(**image) for image in images]
        for image in image_infos:
            assert image.url.startswith("http"), "Image URL must start with http."
        response = await aask(image_infos, [context_description, query])
        return response

    def move_by_distance_schema(self, config: MoveByDistanceInput, context=None):
        self.get_status()
        x_pos = self.parameters['current_x']
        y_pos = self.parameters['current_y']
        z_pos = self.parameters['current_z']
        result = self.move_by_distance(config.x, config.y, config.z, context)
        return result['message']

    def move_to_position_schema(self, config: MoveToPositionInput, context=None):
        self.get_status()
        x_pos = self.parameters['current_x']
        y_pos = self.parameters['current_y']
        z_pos = self.parameters['current_z']
        x = config.x if config.x is not None else 0
        y = config.y if config.y is not None else 0
        z = config.z if config.z is not None else 0
        result = self.move_to_position(x, y, z, context)
        return result['message']

    def auto_focus_schema(self, config: AutoFocusInput, context=None):
        self.auto_focus(context)
        return "Auto-focus completed."

    async def snap_image_schema(self, config: SnapImageInput, context=None):
        image_url = await self.snap(config.exposure, config.channel, config.intensity, context)
        return f"![Image]({image_url})"

    def navigate_to_well_schema(self, config: NavigateToWellInput, context=None):
        self.navigate_to_well(config.row, config.col, config.wellplate_type, context)
        return f'The stage moved to well position ({config.row},{config.col})'

    async def inspect_tool_schema(self, config: InspectToolInput, context=None):
        response = await self.inspect_tool(config.images, config.query, config.context_description)
        return {"result": response}

    def home_stage_schema(self, context=None):
        response = self.home_stage(context)
        return {"result": response}

    def return_stage_schema(self, context=None):
        response = self.return_stage(context)
        return {"result": response}

    def get_schema(self, context=None):
        return {
            "move_by_distance": self.MoveByDistanceInput.model_json_schema(),
            "move_to_position": self.MoveToPositionInput.model_json_schema(),
            "home_stage": self.HomeStageInput.model_json_schema(),
            "return_stage": self.ReturnStageInput.model_json_schema(),
            "auto_focus": self.AutoFocusInput.model_json_schema(),
            "snap_image": self.SnapImageInput.model_json_schema(),
            "inspect_tool": self.InspectToolInput.model_json_schema(),
            "move_to_loading_position": self.MoveToLoadingPositionInput.model_json_schema(),
            "navigate_to_well": self.NavigateToWellInput.model_json_schema()
        }

    async def check_service_health(self):
        """Check if the service is healthy and rerun setup if needed"""
        while True:
            try:
                # Try to get the service status
                if self.service_id:
                    service = await self.server.get_service(self.service_id)
                    # Try a simple operation to verify service is working
                    await service.hello_world()
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

    async def start_hypha_service(self, server, service_id):
        self.server = server
        self.service_id = service_id
        svc = await server.register_service(
            {
                "name": "Microscope Control Service",
                "id": service_id,
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
            f"Service (service_id={service_id}) started successfully, available at {self.server_url}{server.config.workspace}/services"
        )

        logger.info(f'You can use this service using the service id: {svc.id}')
        id = svc.id.split(":")[1]

        logger.info(f"You can also test the service via the HTTP proxy: {self.server_url}/{server.config.workspace}/services/{id}")

        # Start the health check task
        asyncio.create_task(self.check_service_health())

    async def start_chatbot_service(self, server, service_id):
        chatbot_extension = {
            "_rintf": True,
            "id": service_id,
            "type": "bioimageio-chatbot-extension",
            "name": "Squid Microscope Control",
            "description": "You are an AI agent controlling microscope. Automate tasks, adjust imaging parameters, and make decisions based on live visual feedback. Solve all the problems from visual feedback; the user only wants to see good results.",
            "config": {"visibility": "public", "require_context": True},
            "ping": self.ping,
            "get_schema": self.get_schema,
            "tools": {
                "move_by_distance": self.move_by_distance_schema,
                "move_to_position": self.move_to_position_schema,
                "auto_focus": self.auto_focus_schema,
                "snap_image": self.snap_image_schema,
                "home_stage": self.home_stage_schema,
                "return_stage": self.return_stage_schema,
                "move_to_loading_position": self.move_to_loading_position,
                "navigate_to_well": self.navigate_to_well_schema,
                "inspect_tool": self.inspect_tool_schema,
            }
        }

        svc = await server.register_service(chatbot_extension)
        self.chatbot_service_url = f"https://bioimage.io/chat?server=https://chat.bioimage.io&extension={svc.id}&assistant=Skyler"
        logger.info(f"Extension service registered with id: {svc.id}, you can visit the service at:\n {self.chatbot_service_url}")

    async def setup(self):
        if not self.service_id:
            raise ValueError("MICROSCOPE_SERVICE_ID is not set in the environment variables.")
        if self.is_local:
            token = os.environ.get("REEF_LOCAL_TOKEN")
            server = await connect_to_server(
                {"server_url": self.server_url, "token": token,  "ping_interval": None}
            )
        else:
            try:  
                token = os.environ.get("SQUID_WORKSPACE_TOKEN")  
            except:  
                token = await login({"server_url": self.server_url})
            
            server = await connect_to_server(
                {"server_url": self.server_url, "token": token, "workspace": "squid-control",  "ping_interval": None}
            )
        if self.is_simulation:
            await self.start_hypha_service(server, service_id=self.service_id)
            datastore_id = f'data-store-simulated-{self.service_id}'
            chatbot_id = f"squid-control-chatbot-simulated-{self.service_id}"
        else:
            await self.start_hypha_service(server, service_id=self.service_id)
            datastore_id = f'data-store-real-{self.service_id}'
            chatbot_id = f"squid-control-chatbot-real-{self.service_id}"
        self.datastore = HyphaDataStore()
        try:
            await self.datastore.setup(server, service_id=datastore_id)
        except TypeError as e:
            if "Future" in str(e):
                # If config is a Future, wait for it to resolve
                config = await asyncio.wrap_future(server.config)
                await self.datastore.setup(server, service_id=datastore_id, config=config)
            else:
                raise e
    
        chatbot_server_url = "https://chat.bioimage.io"
        try:
            chatbot_token= os.environ.get("WORKSPACE_TOKEN_CHATBOT")
        except:
            chatbot_token = await login({"server_url": chatbot_server_url})
        chatbot_server = await connect_to_server({"server_url": chatbot_server_url, "token": chatbot_token,  "ping_interval": None})
        await self.start_chatbot_service(chatbot_server, chatbot_id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Squid microscope control services for Hypha."
    )
    parser.add_argument(
        "--simulation",
        dest="simulation",
        action="store_true",
        default=False,
        help="Run in simulation mode (default: True)"
    )
    parser.add_argument(
        "--local",
        dest="local",
        action="store_true",
        default=False,
        help="Run with local server URL (default: False)"
    )
    parser.add_argument("--verbose", "-v", action="count")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    microscope = Microscope(is_simulation=args.simulation, is_local=args.local)

    loop = asyncio.get_event_loop()

    async def main():
        try:
            microscope.setup_task = asyncio.create_task(microscope.setup())
            await microscope.setup_task
        except Exception:
            traceback.print_exc()

    loop.create_task(main())
    loop.run_forever()