import os
import logging
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

import sys
# Get the absolute path to the directory where squid_control is located
workspace_path = os.path.abspath('reef_imaging')
sys.path.append(workspace_path)
squid_control_path = os.path.abspath('reef_imaging/control/squid_microscope/squid-control')
sys.path.append(squid_control_path)
print(f"Added {squid_control_path} to sys.path")
# Now you can import squid_control
from squid_control.squid_controller import SquidController
from pydantic import Field, BaseModel
from typing import List, Optional
from tools.hypha_storage import HyphaDataStore
from tools.chatbot.aask import aask

class Microscope:
    def __init__(self, is_simulation=True):
        self.login_required = True
        self.current_x = 0
        self.current_y = 0
        self.current_z = 0
        self.current_theta = 0
        self.current_illumination_channel = None
        self.current_intensity = None
        self.is_illumination_on = False
        self.chatbot_service_url = None
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
        print(f"Authorized emails: {self.authorized_emails}")
        self.datastore = None
        self.server_url = "https://hypha.aicell.io/"

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

    def move_by_distance(self, x, y, z, context=None):
        is_success, x_pos, y_pos, z_pos, x_des, y_des, z_des = self.squidController.move_by_distance_limited(x, y, z)
        if is_success:
            result = f'The stage moved ({x},{y},{z})mm through x,y,z axis, from ({x_pos},{y_pos},{z_pos})mm to ({x_des},{y_des},{z_des})mm'
            return {
                "success": True,
                "message": result,
                "initial_position": {"x": x_pos, "y": y_pos, "z": z_pos},
                "final_position": {"x": x_des, "y": y_des, "z": z_des}
            }
        else:
            result = f'The stage cannot move ({x},{y},{z})mm through x,y,z axis, from ({x_pos},{y_pos},{z_pos})mm to ({x_des},{y_des},{z_des})mm because out of the range.'
            return {
                "success": False,
                "message": result,
                "initial_position": {"x": x_pos, "y": y_pos, "z": z_pos},
                "attempted_position": {"x": x_des, "y": y_des, "z": z_des}
            }

    def move_to_position(self, x, y, z, context=None):
        self.get_status()
        initial_x = self.parameters['current_x']
        initial_y = self.parameters['current_y']
        initial_z = self.parameters['current_z']

        if x != 0:
            is_success, x_pos, y_pos, z_pos, x_des = self.squidController.move_x_to_limited(x)
            if not is_success:
                return {
                    "success": False,
                    "message": f'The stage cannot move to position ({x},{y},{z})mm from ({initial_x},{initial_y},{initial_z})mm because out of the limit of X axis.',
                    "initial_position": {"x": initial_x, "y": initial_y, "z": initial_z},
                    "final_position": {"x": x_pos, "y": y_pos, "z": z_pos}
                }

        if y != 0:
            is_success, x_pos, y_pos, z_pos, y_des = self.squidController.move_y_to_limited(y)
            if not is_success:
                return {
                    "success": False,
                    "message": f'X axis moved successfully, the stage is now at ({x_pos},{y_pos},{z_pos})mm. But aimed position is out of the limit of Y axis and the stage cannot move to position ({x},{y},{z})mm.',
                    "initial_position": {"x": initial_x, "y": initial_y, "z": initial_z},
                    "final_position": {"x": x_pos, "y": y_pos, "z": z_pos}
                }

        if z != 0:
            is_success, x_pos, y_pos, z_pos, z_des = self.squidController.move_z_to_limited(z)
            if not is_success:
                return {
                    "success": False,
                    "message": f'X and Y axis moved successfully, the stage is now at ({x_pos},{y_pos},{z_pos})mm. But aimed position is out of the limit of Z axis and the stage cannot move to position ({x},{y},{z})mm.',
                    "initial_position": {"x": initial_x, "y": initial_y, "z": initial_z},
                    "final_position": {"x": x_pos, "y": y_pos, "z": z_pos}
                }

        return {
            "success": True,
            "message": f'The stage moved to position ({x},{y},{z})mm from ({initial_x},{initial_y},{initial_z})mm successfully.',
            "initial_position": {"x": initial_x, "y": initial_y, "z": initial_z},
            "final_position": {"x": x_pos, "y": y_pos, "z": z_pos}
        }

    def get_status(self, context=None):
        """
        Get the current status of the microscope.
        """
        current_x, current_y, current_z, current_theta = self.squidController.navigationController.update_pos(microcontroller=self.squidController.microcontroller)
        is_illumination_on = self.squidController.liveController.illumination_on
        scan_channel = self.squidController.multipointController.selected_configurations

        self.parameters = {
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
        return self.parameters

    def update_parameters_from_client(self, new_parameters, context=None):
        """
        Update the parameters from the client side.
        """
        if self.parameters is None:
            self.parameters = {}

        # Update only the specified keys
        for key, value in new_parameters.items():
            if key in self.parameters:
                self.parameters[key] = value
                print(f"Updated {key} to {value}")

                # Update the corresponding instance variable if it exists
                if hasattr(self, key):
                    setattr(self, key, value)
                else:
                    print(f"Attribute {key} does not exist on self, skipping update.")
            else:
                print(f"Key {key} not found in parameters, skipping update.")

        return {"success": True, "message": "Parameters updated successfully.", "updated_parameters": new_parameters}


    def one_new_frame(self, context=None):
        gray_img = self.squidController.snap_image(0, 50, 100)

        min_val = np.min(gray_img)
        max_val = np.max(gray_img)
        if max_val > min_val:  # Avoid division by zero if the image is completely uniform
            gray_img = (gray_img - min_val) * (255 / (max_val - min_val))
            gray_img = gray_img.astype(np.uint8)  # Convert to 8-bit image
        else:
            gray_img = np.zeros((512, 512), dtype=np.uint8)  # If no variation, return a black image
        bgr_img = np.stack((gray_img,) * 3, axis=-1)  # Duplicate grayscale data across 3 channels to simulate BGR format.
        return bgr_img

    def snap(self, exposure_time, channel, intensity, context=None):
        """
        Get the current frame from the camera as a grayscale image.
        """
        gray_img = self.squidController.snap_image(channel, intensity, exposure_time)
        print('The image is snapped')
        gray_img = gray_img.astype(np.uint8)
        # Resize the image to a standard size
        resized_img = cv2.resize(gray_img, (2048, 2048))

        # Encode the image directly to PNG without converting to BGR
        _, png_image = cv2.imencode('.png', resized_img)

        # Store the PNG image
        file_id = self.datastore.put('file', png_image.tobytes(), 'snapshot.png', "Captured microscope image in PNG format")
        data_url = self.datastore.get_url(file_id)
        print(f'The image is snapped and saved as {data_url}')
        
        #update the current illumination channel and intensity
        if channel == 0:
            self.BF_intensity_exposure = [intensity,exposure_time]
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
            
        return data_url

    def open_illumination(self, context=None):
        """
        Turn on the bright field illumination.
        """
        self.squidController.liveController.turn_on_illumination()
        print('Bright field illumination turned on.')

    def close_illumination(self, context=None):
        """
        Turn off the bright field illumination.
        """
        self.squidController.liveController.turn_off_illumination()
        print('Bright field illumination turned off.')

    def scan_well_plate(self, context=None):
        """
        Scan the well plate according to the pre-defined position list.
        """
        print("Start scanning well plate")
        self.squidController.scan_well_plate(action_ID='Test')

    def set_illumination(self, channel, intensity, context=None):
        """
        Set the intensity of the bright field illumination.
        """
        self.squidController.liveController.set_illumination(channel, intensity)
        print(f'The intensity of the channel {channel} illumination is set to {intensity}.')

    def set_camera_exposure(self, exposure_time, context=None):
        """
        Set the exposure time of the camera.
        """
        self.squidController.camera.set_exposure_time(exposure_time)
        print(f'The exposure time of the camera is set to {exposure_time}.')

    def stop_scan(self, context=None):
        """
        Stop the well plate scanning.
        """
        self.squidController.liveController.stop_live()
        print("Stop scanning well plate")

    def home_stage(self, context=None):
        """
        Home the stage in z, y, and x axis.
        """
        self.squidController.home_stage()
        print('The stage moved to home position in z, y, and x axis')
        return 'The stage moved to home position in z, y, and x axis'

    def move_to_loading_position(self, context=None):
        """
        Move the stage to the loading position.
        """
        self.squidController.slidePositionController.move_to_slide_loading_position()
        print('The stage moved to loading position')

    def auto_focus(self, context=None):
        """
        Auto focus the camera.
        """
        self.squidController.do_autofocus()
        print('The camera is auto-focused')

    def navigate_to_well(self, row, col, wellplate_type, context=None):
        """
        Navigate to the specified well position in the well plate.
        """
        if wellplate_type is None:
            wellplate_type = '96'
        self.squidController.platereader_move_to_well(row, col, wellplate_type)
        print(f'The stage moved to well position ({row},{col})')

    def get_chatbot_url(self, context=None):
        print(f"chatbot_service_url: {self.chatbot_service_url}")
        return self.chatbot_service_url

    # Chatbot extension
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

    def snap_image_schema(self, config: SnapImageInput, context=None):
        image_url = self.snap(config.exposure, config.channel, config.intensity, context)
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

    def get_schema(self, context=None):
        return {
            "move_by_distance": self.MoveByDistanceInput.schema(),
            "move_to_position": self.MoveToPositionInput.schema(),
            "home_stage": self.HomeStageInput.schema(),
            "auto_focus": self.AutoFocusInput.schema(),
            "snap_image": self.SnapImageInput.schema(),
            "inspect_tool": self.InspectToolInput.schema(),
            "move_to_loading_position": self.MoveToLoadingPositionInput.schema(),
            "navigate_to_well": self.NavigateToWellInput.schema()
        }

    async def start_hypha_service(self, server, service_id):
        await server.register_service(
            {
                "name": "Microscope Control Service",
                "id": service_id,
                "config": {
                    "visibility": "public",
                    "run_in_executor": True
                },
                "type": "echo",
                "move_by_distance": self.move_by_distance,
                "snap": self.snap,
                "off_illumination": self.close_illumination,
                "on_illumination": self.open_illumination,
                "set_illumination": self.set_illumination,
                "set_camera_exposure": self.set_camera_exposure,
                "scan_well_plate": self.scan_well_plate,
                "stop_scan": self.stop_scan,
                "home_stage": self.home_stage,
                "move_to_position": self.move_to_position,
                "move_to_loading_position": self.move_to_loading_position,
                "auto_focus": self.auto_focus,
                "get_status": self.get_status,
                "update_parameters_from_client": self.update_parameters_from_client,
                "get_chatbot_url": self.get_chatbot_url,
            },
        )

        print(
            f"Service (service_id={service_id}) started successfully, available at http://{self.server_url}/{server.config.workspace}/services"
        )
        print(f"You can access at https://cccoolll.github.io/reef-imaging/?service_id={service_id}")

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
                "move_to_loading_position": self.move_to_loading_position,
                "navigate_to_well": self.navigate_to_well_schema,
                "inspect_tool": self.inspect_tool_schema,
            }
        }

        svc = await server.register_service(chatbot_extension)
        self.chatbot_service_url = f"https://bioimage.io/chat?server=https://chat.bioimage.io&extension={svc.id}&assistant=Skyler"
        print(f"Extension service registered with id: {svc.id}, you can visit the service at:\n {self.chatbot_service_url}")

    async def setup(self):
        token = await login({"server_url": self.server_url})
        server = await connect_to_server(
            {"server_url": self.server_url, "token": token}
        )
        await self.start_hypha_service(server, service_id="microscope-control-squid-test")

        self.datastore = HyphaDataStore()
        try:
            await self.datastore.setup(server, service_id="data-store")
        except TypeError as e:
            if "Future" in str(e):
                # If config is a Future, wait for it to resolve
                config = await asyncio.wrap_future(server.config)
                await self.datastore.setup(server, service_id="data-store", config=config)
            else:
                raise e
        chatbot_id = "squid-microscope-control-test"
        chatbot_server_url = "https://chat.bioimage.io"
        token = await login({"server_url": chatbot_server_url})
        chatbot_server = await connect_to_server({"server_url": chatbot_server_url, "token": token})
        await self.start_chatbot_service(chatbot_server, chatbot_id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Squid microscope control services for Hypha."
    )
    parser.add_argument(
        "--simulation",
        dest="simulation",
        action="store_true",
        default=True,
        help="Run in simulation mode (default: True)"
    )
    parser.add_argument("--verbose", "-v", action="count")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    microscope = Microscope(is_simulation=args.simulation)

    loop = asyncio.get_event_loop()

    async def main():
        try:
            await microscope.setup()
        except Exception:
            traceback.print_exc()

    loop.create_task(main())
    loop.run_forever()
