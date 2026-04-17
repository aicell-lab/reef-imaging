"""Hypha API endpoint mixin (all @schema_function methods)."""
import asyncio
from datetime import datetime
from hypha_rpc.utils.schema import schema_function
from .core import logger, HamiltonBusyError
from reef_imaging.orchestration import ResourceBusyError


class APIMixin:
    @schema_function(skip_self=True)
    async def get_hamilton_status(self):
        """Return Hamilton executor connectivity and the current executor status."""

        try:
            executor_status = await self._get_hamilton_executor_status()
            admission_snapshot = await self.admission_controller.snapshot()
            hamilton_operations = [
                operation
                for operation in admission_snapshot["active_operations"]
                if self._hamilton_resource() in operation.get("resources", [])
            ]
            return {
                "success": True,
                "connected": executor_status is not None,
                "service_id": self.hamilton_executor_id,
                "executor_status": executor_status,
                "active_operations": hamilton_operations,
            }
        except (ConnectionError, OSError, RuntimeError) as e:
            logger.error(f"Failed to get Hamilton status: {e}", exc_info=True)
            return {
                "success": False,
                "connected": False,
                "service_id": self.hamilton_executor_id,
                "executor_status": None,
                "active_operations": [],
                "message": str(e),
            }

    @schema_function(skip_self=True)
    async def run_hamilton_protocol(
        self,
        script_content: str,
        timeout: int = 3600,
    ):
        """Start a Hamilton script on the existing executor service without transport."""

        if not isinstance(script_content, str) or not script_content.strip():
            return {"success": False, "message": "script_content must be a non-empty string."}
        if timeout <= 0:
            return {"success": False, "message": "timeout must be greater than zero."}

        try:
            service = await self._get_hamilton_executor_proxy()
            if service is None:
                raise RuntimeError(
                    f"Hamilton executor service '{self.hamilton_executor_id}' is not available."
                )

            request = self._build_request(
                "hamilton-execution",
                extra_resources=(self._hamilton_resource(),),
                metadata={"timeout_seconds": timeout},
            )

            async with self.admission_controller.hold(request):
                start_result = await service.start_execution(
                    script_content=script_content,
                    timeout=timeout,
                )
                if not start_result.get("accepted"):
                    live_status = await self._get_hamilton_executor_status(refresh_if_missing=False)
                    message = start_result.get("error") or "Hamilton executor rejected the run request."
                    response = {
                        "success": False,
                        "message": message,
                        "start_result": start_result,
                        "hamilton_status": live_status,
                    }
                    if start_result.get("busy"):
                        response["state"] = "busy"
                    return response

            return {
                "success": True,
                "message": "Hamilton protocol accepted and started.",
                "start_result": start_result,
                "action_id": start_result.get("action_id"),
                "state": start_result.get("status", "running"),
                "hamilton_status": await self.get_hamilton_status(),
                "runtime_status": await self.get_runtime_status(),
            }
        except ResourceBusyError as busy_error:
            return self._busy_response(
                "Hamilton protocol request rejected - Hamilton-related resources are busy.",
                busy_error,
            )
        except (ConnectionError, OSError, RuntimeError, asyncio.TimeoutError) as e:
            logger.error(f"Hamilton protocol execution failed: {e}", exc_info=True)
            return {
                "success": False,
                "message": str(e),
                "hamilton_status": await self.get_hamilton_status(),
                "runtime_status": await self.get_runtime_status(),
            }

    @schema_function(skip_self=True)
    async def ping(self):
        """Returns pong for health checks."""
        logger.info("ping service method called.")
        return "pong"

    @schema_function(skip_self=True)
    async def add_imaging_task(self, task_definition: dict):
        """Adds/updates imaging task. scan_mode: 'full_automation' (default) or 'microscope_only'. For microscope_only, saved_data_type: 'raw_images_well_plate' or 'raw_image_flexible'. Optional focus_map_points: List[List[float]] with 3 reference points [[x,y,z], [x,y,z], [x,y,z]] in mm for focus interpolation. For raw_image_flexible, optional move_for_autofocus: bool to enable stage movement for autofocus."""
        logger.info(f"Attempting to add/update imaging task: {task_definition.get('name')}")
        if not isinstance(task_definition, dict) or "name" not in task_definition or "settings" not in task_definition:
            raise ValueError("Invalid task definition: must be a dict with 'name' and 'settings'.")

        task_name = task_definition["name"]
        new_settings = task_definition["settings"]

        scan_mode = new_settings.get("scan_mode", "full_automation")
        if scan_mode not in ["full_automation", "microscope_only"]:
            raise ValueError(f"Invalid scan_mode '{scan_mode}'. Must be 'full_automation' or 'microscope_only'.")
        
        common_required = ["allocated_microscope", "pending_time_points", "illumination_settings", "do_contrast_autofocus", "do_reflection_af"]
        
        # Check if saved_data_type is provided (for both modes now)
        saved_data_type = new_settings.get("saved_data_type")
        if not saved_data_type:
            raise ValueError(f"Missing required field 'saved_data_type' for task '{task_name}'. Must be 'raw_images_well_plate' or 'raw_image_flexible'.")
        if saved_data_type not in ["raw_images_well_plate", "raw_image_flexible"]:
            raise ValueError(f"Invalid saved_data_type '{saved_data_type}'. Must be 'raw_images_well_plate' or 'raw_image_flexible'.")
        
        if scan_mode == "full_automation":
            required_settings = common_required + ["incubator_slot"]
            if saved_data_type == "raw_images_well_plate":
                required_settings.extend(["wells_to_scan", "Nx", "Ny"])
            else:
                required_settings.append("positions")
        else:
            required_settings = common_required + ["saved_data_type"]
            if saved_data_type == "raw_images_well_plate":
                required_settings.extend(["wells_to_scan", "Nx", "Ny"])
            else:
                required_settings.append("positions")
        
        for req_field in required_settings:
            if req_field not in new_settings:
                raise ValueError(f"Missing required field '{req_field}' for task '{task_name}' (scan_mode: {scan_mode}, saved_data_type: {saved_data_type}).")
        
        if not isinstance(new_settings["pending_time_points"], list):
            raise ValueError(f"'pending_time_points' must be a list for task '{task_name}'.")

        for tp_str in new_settings["pending_time_points"]:
            try:
                datetime.fromisoformat(tp_str)
                if 'Z' in tp_str or '+' in tp_str.split('T')[-1]:
                    raise ValueError("Time point must be naive local time")
            except ValueError as ve:
                raise ValueError(f"Invalid time point '{tp_str}' for task '{task_name}': {ve}")
        
        if "imaged_time_points" not in new_settings:
            new_settings["imaged_time_points"] = []
        elif not isinstance(new_settings["imaged_time_points"], list):
            raise ValueError(f"'imaged_time_points' must be a list for task '{task_name}'.")
        
        for tp_str in new_settings["imaged_time_points"]:
            try:
                datetime.fromisoformat(tp_str)
                if 'Z' in tp_str or '+' in tp_str.split('T')[-1]:
                    raise ValueError("Time point must be naive local time")
            except ValueError as ve:
                raise ValueError(f"Invalid imaged_time_point '{tp_str}' for task '{task_name}': {ve}")
        
        if saved_data_type == "raw_image_flexible":
            positions = new_settings.get("positions", [])
            if not isinstance(positions, list) or len(positions) == 0:
                raise ValueError(f"'positions' must be a non-empty list for task '{task_name}'.")
            for idx, pos in enumerate(positions):
                if not isinstance(pos, dict) or "x" not in pos or "y" not in pos:
                    raise ValueError(f"Position {idx} must be a dict with 'x' and 'y' for task '{task_name}'.")
            
            # Validate move_for_autofocus if provided (optional for raw_image_flexible)
            if "move_for_autofocus" in new_settings:
                if not isinstance(new_settings["move_for_autofocus"], bool):
                    raise ValueError(f"'move_for_autofocus' must be a boolean for task '{task_name}'.")

        # Validate focus_map_points if provided (optional for both scan types)
        if "focus_map_points" in new_settings:
            focus_map_points = new_settings["focus_map_points"]
            if not isinstance(focus_map_points, list):
                raise ValueError(f"'focus_map_points' must be a list for task '{task_name}'.")
            if len(focus_map_points) != 3:
                raise ValueError(f"'focus_map_points' must contain exactly 3 reference points for task '{task_name}' (got {len(focus_map_points)}).")
            for idx, point in enumerate(focus_map_points):
                if not isinstance(point, list):
                    raise ValueError(f"Focus map point {idx} must be a list [x, y, z] for task '{task_name}'.")
                if len(point) != 3:
                    raise ValueError(f"Focus map point {idx} must have exactly 3 coordinates [x, y, z] for task '{task_name}' (got {len(point)}).")
                try:
                    # Validate that all elements are numeric
                    [float(coord) for coord in point]
                except (ValueError, TypeError):
                    raise ValueError(f"Focus map point {idx} must contain numeric coordinates [x, y, z] in mm for task '{task_name}'.")

        has_pending = bool(new_settings["pending_time_points"])
        has_imaged = bool(new_settings.get("imaged_time_points", []))

        current_status = "pending"
        if not has_pending:
            current_status = "completed"
        
        new_settings["imaging_completed"] = not has_pending
        new_settings["imaging_started"] = has_imaged or (not has_pending and has_imaged)

        async with self._config_lock:
            try:
                config_data = {"samples": []}
                try:
                    with open(CONFIG_FILE_PATH, 'r') as f:
                        config_data = json.load(f)
                except FileNotFoundError:
                    logger.warning(f"{CONFIG_FILE_PATH} not found. Will create a new one.")
                except (json.JSONDecodeError, OSError) as e:
                    logger.warning(f"{CONFIG_FILE_PATH} is corrupted or unreadable: {e}. Will create a new one.")
                
                if "samples" not in config_data or not isinstance(config_data["samples"], list):
                     config_data["samples"] = []

                task_exists_at_index = -1
                existing_task_status = None
                for i, existing_task in enumerate(config_data["samples"]):
                    if existing_task.get("name") == task_name:
                        task_exists_at_index = i
                        existing_task_status = existing_task.get("operational_state", {}).get("status", "pending")
                        break
                
                # For existing tasks, preserve "uploading" status if it was uploading
                final_status = current_status
                if task_exists_at_index != -1 and existing_task_status == "uploading" and not has_pending:
                    final_status = "uploading"  # Keep uploading status for existing tasks
                    logger.info(f"Task '{task_name}' is currently uploading. Preserving uploading status.")
                
                op_state = {
                    "status": final_status,
                    "last_updated_by_orchestrator": datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                }

                if task_exists_at_index != -1:
                    logger.info(f"Task '{task_name}' already exists. Updating its settings and operational_state.")
                    config_data["samples"][task_exists_at_index]["settings"] = new_settings
                    config_data["samples"][task_exists_at_index]["operational_state"] = op_state
                else:
                    logger.info(f"Adding new task '{task_name}'.")
                    new_task_entry = {
                        "name": task_name,
                        "settings": new_settings,
                        "operational_state": op_state
                    }
                    config_data["samples"].append(new_task_entry)

                with open(CONFIG_FILE_PATH_TMP, 'w') as f:
                    json.dump(config_data, f, indent=4)
                os.replace(CONFIG_FILE_PATH_TMP, CONFIG_FILE_PATH)
                logger.info(f"Task '{task_name}' processed (added/updated) in {CONFIG_FILE_PATH}.")

            except Exception as e:
                logger.error(f"Failed to add/update imaging task '{task_name}' in config: {e}", exc_info=True)
                return {"success": False, "message": f"Error processing task: {str(e)}"}

        await self._load_and_update_tasks() # Refresh orchestrator's internal task list
        return {"success": True, "message": f"Task '{task_name}' added/updated successfully."}

    @schema_function(skip_self=True)
    async def delete_imaging_task(self, task_name: str):
        """Deletes an imaging task from the configuration."""
        logger.info(f"Attempting to delete imaging task: {task_name}")
        if not task_name:
            return {"success": False, "message": "Task name cannot be empty."}
        if task_name in self._scheduled_cycle_tasks:
            return {
                "success": False,
                "message": f"Task '{task_name}' is currently running and cannot be deleted.",
                "state": "busy",
            }

        async with self._config_lock:
            try:
                config_data = {"samples": []}
                try:
                    with open(CONFIG_FILE_PATH, 'r') as f:
                        config_data = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError):
                    logger.warning(f"{CONFIG_FILE_PATH} not found or corrupted. Cannot delete task.")
                    return {"success": False, "message": f"{CONFIG_FILE_PATH} not found or corrupted."}

                if "samples" not in config_data or not isinstance(config_data["samples"], list):
                    logger.warning(f"No 'samples' list in {CONFIG_FILE_PATH}. Cannot delete task.")
                    return {"success": False, "message": "No 'samples' list in configuration."}

                original_count = len(config_data["samples"])
                config_data["samples"] = [task for task in config_data["samples"] if task.get("name") != task_name]
                
                if len(config_data["samples"]) == original_count:
                    logger.warning(f"Task '{task_name}' not found in {CONFIG_FILE_PATH}. No deletion occurred.")
                    return {"success": False, "message": f"Task '{task_name}' not found."}

                with open(CONFIG_FILE_PATH_TMP, 'w') as f:
                    json.dump(config_data, f, indent=4)
                os.replace(CONFIG_FILE_PATH_TMP, CONFIG_FILE_PATH)
                logger.info(f"Task '{task_name}' deleted from {CONFIG_FILE_PATH}.")

            except Exception as e:
                logger.error(f"Failed to delete imaging task '{task_name}' from config: {e}", exc_info=True)
                return {"success": False, "message": f"Error deleting task: {str(e)}"}
        
        await self._load_and_update_tasks() # Refresh orchestrator's internal task list
        return {"success": True, "message": f"Task '{task_name}' deleted successfully."}

    @schema_function(skip_self=True)
    async def pause_imaging_task(self, task_name: str):
        """Pauses an imaging task, preventing it from being processed until resumed."""
        if task_name not in self.tasks:
            return {"success": False, "message": f"Task '{task_name}' not found."}
        if task_name in self._scheduled_cycle_tasks:
            return {
                "success": False,
                "message": f"Task '{task_name}' is currently running and cannot be paused.",
                "state": "busy",
            }
        if self.tasks[task_name]["status"] == "paused":
            return {"success": True, "message": f"Task '{task_name}' is already paused."}
        await self._update_task_state_and_write_config(task_name, status="paused")
        return {"success": True, "message": f"Task '{task_name}' paused."}

    @schema_function(skip_self=True)
    async def resume_imaging_task(self, task_name: str):
        """Resumes a paused imaging task, allowing it to be processed again."""
        if task_name not in self.tasks:
            return {"success": False, "message": f"Task '{task_name}' not found."}
        if self.tasks[task_name]["status"] != "paused":
            return {"success": False, "message": f"Task '{task_name}' is not paused."}
        new_status = "completed" if not self.tasks[task_name]["config"].get("pending_datetimes") else "pending"
        await self._update_task_state_and_write_config(task_name, status=new_status)
        return {"success": True, "message": f"Task '{task_name}' resumed."}

    @schema_function(skip_self=True)
    async def get_all_imaging_tasks(self):
        """Retrieves all imaging task configurations from config.json."""
        logger.debug(f"Attempting to read all imaging tasks from {CONFIG_FILE_PATH}")
        async with self._config_lock:
            try:
                with open(CONFIG_FILE_PATH, 'r') as f:
                    config_data = json.load(f)
                return config_data.get("samples", []) # Return the list of samples
            except FileNotFoundError:
                logger.warning(f"{CONFIG_FILE_PATH} not found when trying to get all tasks.")
                return [] 
            except json.JSONDecodeError:
                logger.error(f"Error decoding JSON from {CONFIG_FILE_PATH} when getting all tasks.")
                return []
            except Exception as e:
                logger.error(f"Failed to get all imaging tasks: {e}", exc_info=True)
                return {"error": str(e), "success": False}

    @schema_function(skip_self=True)
    async def get_runtime_status(self):
        """Return a runtime snapshot for operator preflight checks and failure triage."""
        try:
            if not self.configured_microscopes_info and not self.tasks:
                await self._load_and_update_tasks()

            expected_microscopes = list(self.configured_microscopes_info.keys())
            local_services_ready = (
                self.incubator is not None
                and self.robotic_arm is not None
                and all(microscope_id in self.microscope_services for microscope_id in expected_microscopes)
            )
            if expected_microscopes and not local_services_ready:
                try:
                    await self.setup_connections()
                except Exception as setup_error:
                    logger.warning(f"Could not refresh local service proxies while building runtime status: {setup_error}")

            admission_snapshot = await self.admission_controller.snapshot()
            connected_microscopes = sorted(self.microscope_services.keys())
            sample_on_flags_per_microscope = {
                mic_id: self.sample_on_microscope_flags.get(mic_id, False)
                for mic_id in expected_microscopes
            }
            connected_services = {
                "incubator": self.incubator is not None,
                "robotic_arm": self.robotic_arm is not None,
                "hamilton": self.hamilton_executor is not None,
                "microscopes": {
                    mic_id: mic_id in self.microscope_services
                    for mic_id in expected_microscopes
                },
            }

            return {
                "success": True,
                "active_task": self.active_task_name,
                "active_tasks": sorted(self._active_task_names),
                "active_operations": admission_snapshot["active_operations"],
                "held_resources": admission_snapshot["held_resources"],
                "connected_services": connected_services,
                "connected_microscopes": connected_microscopes,
                "configured_microscopes": expected_microscopes,
                "critical_services": sorted(
                    [
                        {"service_type": service_type, "service_id": service_id}
                        for service_type, service_id in self.critical_services
                    ],
                    key=lambda item: (item["service_type"], item["service_id"]),
                ),
                "in_critical_operation": self.in_critical_operation,
                "sample_on_microscope_flags": sample_on_flags_per_microscope,
                "local_hypha": {
                    "server_url": self.server_url,
                    "workspace": self.local_workspace,
                    "connected": self.local_server_connection is not None,
                },
                "cloud_hypha": {
                    "server_url": self.orchestrator_hypha_server_url,
                    "workspace": self.workspace,
                    "connected": self.orchestrator_hypha_server_connection is not None,
                },
            }
        except Exception as e:
            logger.error(f"Failed to get runtime status: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    @schema_function(skip_self=True)
    async def cancel_microscope_scan(self, microscope_id: str):
        """Operator emergency API to cancel a running microscope scan.
        
        Note: scan_cancel() performs async cancellation. The scan may continue briefly
        in the background before stopping. The result message indicates whether the
        scan was stopped immediately or is stopping gracefully.
        """
        if microscope_id not in self.configured_microscopes_info:
            return {"success": False, "message": f"Unknown microscope ID '{microscope_id}'."}

        try:
            if microscope_id not in self.microscope_services:
                await self.setup_connections()
            microscope_service = self.microscope_services.get(microscope_id)
            if not microscope_service:
                raise Exception(f"Microscope service {microscope_id} is not available.")

            result = await microscope_service.scan_cancel()
            
            # Handle new async cancellation behavior
            # scan_cancel() may return a message indicating the scan is stopping in background
            result_msg = result.get("message", "") if isinstance(result, dict) else str(result)
            is_async_cancel = "stopping in the background" in result_msg.lower()
            
            if is_async_cancel:
                logger.info(f"Scan on {microscope_id} is stopping in the background. "
                           f"Result: {result_msg}")
            else:
                logger.info(f"Scan cancelled on {microscope_id}: {result_msg}")
            
            return {
                "success": True,
                "message": f"Cancel command sent to microscope {microscope_id}.",
                "result": result,
                "async_cancellation": is_async_cancel,
            }
        except Exception as e:
            logger.error(f"Failed to cancel scan on {microscope_id}: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    @schema_function(skip_self=True)
    async def halt_robotic_arm(self):
        """Operator emergency API to halt the robotic arm immediately."""
        try:
            if not self.robotic_arm:
                setup_ok = await self.setup_connections()
                if not setup_ok or not self.robotic_arm:
                    raise Exception("Robotic arm service is not available.")

            result = await self.robotic_arm.halt()
            return {
                "success": True,
                "message": "Robotic arm halt command sent.",
                "result": result,
            }
        except Exception as e:
            logger.error(f"Failed to halt robotic arm: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    @schema_function(skip_self=True)
    async def get_lab_video_stream_urls(self):
        """Returns public Hypha URLs for current lab video stream apps, including Hamilton when exposed."""
        base = f"{self.orchestrator_hypha_server_url}/{self.workspace}/apps"
        return {
            "reef-lab-camera-1": f"{base}/reef-lab-camera-1",
            "reef-lab-camera-2": f"{base}/reef-lab-camera-2",
            "reef-hamilton-feed": f"{base}/reef-hamilton-feed",
        }

    @schema_function(skip_self=True)
    async def process_timelapse_offline_api(self, experiment_id: str, upload_immediately: bool = True, cleanup_temp_files: bool = True):
        """API wrapper for offline stitching and upload timelapse functionality."""
        logger.info(f"API call: process_timelapse_offline for experiment_id: {experiment_id}")

        # Find matching tasks
        matching_tasks = [name for name in self.tasks.keys() if experiment_id in name]
        if not matching_tasks:
            return {"success": False, "message": f"No tasks found matching experiment_id: {experiment_id}"}

        allocated_microscopes = {
            self.tasks[task_name]["config"].get("allocated_microscope")
            for task_name in matching_tasks
            if task_name in self.tasks
        }
        allocated_microscopes.discard(None)

        if not allocated_microscopes:
            return {"success": False, "message": f"No allocated microscope found for experiment_id: {experiment_id}"}
        if len(allocated_microscopes) != 1:
            return {
                "success": False,
                "message": (
                    f"Experiment '{experiment_id}' spans multiple microscopes {sorted(allocated_microscopes)}. "
                    "Offline processing must be run per microscope."
                ),
            }

        microscope_id = next(iter(allocated_microscopes))
        request = self._build_request(
            "offline-processing",
            microscope_id=microscope_id,
            extra_resources=tuple(self._task_resource(task_name) for task_name in matching_tasks),
            metadata={"experiment_id": experiment_id},
        )

        try:
            if microscope_id not in self.microscope_services:
                logger.info(f"Microscope {microscope_id} not connected. Attempting to setup connections...")
                await self.setup_connections()

            microscope_service = self.microscope_services.get(microscope_id)
            if not microscope_service:
                raise Exception(f"Microscope service {microscope_id} is not available for offline processing")

            async with self.admission_controller.hold(request):
                for task_name in matching_tasks:
                    await self._update_task_state_and_write_config(task_name, status="uploading")

                result = await microscope_service.process_timelapse_offline(
                    experiment_id=experiment_id,
                    upload_immediately=upload_immediately,
                    cleanup_temp_files=cleanup_temp_files
                )

                for task_name in matching_tasks:
                    await self._update_task_state_and_write_config(task_name, status="completed")

                return {
                    "success": True,
                    "message": f"Offline processing completed for {len(matching_tasks)} tasks on {microscope_id}",
                    "result": result,
                    "microscope_id": microscope_id,
                }
        except ResourceBusyError as busy_error:
            message = (
                f"Offline processing for experiment_id '{experiment_id}' rejected because microscope "
                f"{microscope_id} or related tasks are busy."
            )
            logger.warning(message)
            return self._busy_response(message, busy_error)
        except (ConnectionError, OSError, RuntimeError) as e:
            logger.error(f"Offline processing failed for experiment_id {experiment_id}: {e}")
            # Set tasks to error status
            for task_name in matching_tasks:
                await self._update_task_state_and_write_config(task_name, status="error")
            return {"success": False, "message": str(e)}

    @schema_function(skip_self=True)
    async def scan_microscope_only_api(self, microscope_id: str, scan_config: dict,
                                       task_name: str = None, action_id: str = None):
        """
        API endpoint to run a scan directly on a microscope without load/unload operations.
        
        This bypasses robotic arm and incubator integration, assuming the sample is already
        manually placed on the microscope. Can optionally link to an existing task for status tracking.
        
        Args:
            microscope_id: ID of the microscope to use (e.g., 'microscope-squid-1')
            scan_config: Scan configuration dictionary containing:
                - saved_data_type: 'raw_images_well_plate' or 'raw_image_flexible'
                - For 'raw_images_well_plate':
                    - wells_to_scan: List[str] (e.g., ['A1', 'B2'])
                    - Nx, Ny: int (grid dimensions)
                    - dx, dy: float (position intervals in mm)
                    - well_plate_type: str (optional, default '96')
                    - focus_map_points: List[List[float]] (optional): 3 reference points [[x,y,z], [x,y,z], [x,y,z]] in mm for focus interpolation
                - For 'raw_image_flexible':
                    - positions: List[dict] with x, y, z, Nx, Ny, Nz, dx, dy, dz, name
                    - focus_map_points: List[List[float]] (optional): 3 reference points [[x,y,z], [x,y,z], [x,y,z]] in mm for focus interpolation
                    - move_for_autofocus: bool (optional): Whether to move stage for autofocus
                - illumination_settings: List[dict] (required for both)
                - do_contrast_autofocus: bool (required for both)
                - do_reflection_af: bool (required for both)
            task_name: Optional task name to link for status tracking
            action_id: Optional custom action ID (auto-generated if not provided)
            
        Returns:
            Dictionary with success status, message, and scan details
        """
        logger.info(f"API call: scan_microscope_only for microscope {microscope_id}, task_name={task_name}")
        
        # Validate microscope_id
        if microscope_id not in self.configured_microscopes_info:
            msg = f"Microscope ID '{microscope_id}' not found in configured microscopes."
            logger.error(msg)
            return {"success": False, "message": msg}
        
        # Validate scan_config
        if not isinstance(scan_config, dict):
            msg = "scan_config must be a dictionary"
            logger.error(msg)
            return {"success": False, "message": msg}
        
        # Validate required fields
        required_fields = ["saved_data_type", "illumination_settings", "do_contrast_autofocus", "do_reflection_af"]
        for field in required_fields:
            if field not in scan_config:
                msg = f"Missing required field '{field}' in scan_config"
                logger.error(msg)
                return {"success": False, "message": msg}
        
        saved_data_type = scan_config["saved_data_type"]
        if saved_data_type not in ["raw_images_well_plate", "raw_image_flexible"]:
            msg = f"Invalid saved_data_type '{saved_data_type}'. Must be 'raw_images_well_plate' or 'raw_image_flexible'"
            logger.error(msg)
            return {"success": False, "message": msg}
        
        # Validate type-specific fields
        if saved_data_type == "raw_images_well_plate":
            required_raw_images_fields = ["wells_to_scan", "Nx", "Ny", "dx", "dy"]
            for field in required_raw_images_fields:
                if field not in scan_config:
                    msg = f"Missing required field '{field}' for raw_images_well_plate scan type"
                    logger.error(msg)
                    return {"success": False, "message": msg}
        elif saved_data_type == "raw_image_flexible":
            if "positions" not in scan_config:
                msg = "Missing required field 'positions' for raw_image_flexible scan type"
                logger.error(msg)
                return {"success": False, "message": msg}
            if not isinstance(scan_config["positions"], list) or len(scan_config["positions"]) == 0:
                msg = "'positions' must be a non-empty list for raw_image_flexible scan type"
                logger.error(msg)
                return {"success": False, "message": msg}
            # Validate move_for_autofocus if provided (optional for raw_image_flexible)
            if "move_for_autofocus" in scan_config:
                if not isinstance(scan_config["move_for_autofocus"], bool):
                    msg = "'move_for_autofocus' must be a boolean for raw_image_flexible scan type"
                    logger.error(msg)
                    return {"success": False, "message": msg}
        
        # Validate focus_map_points if provided (optional for both scan types)
        if "focus_map_points" in scan_config:
            focus_map_points = scan_config["focus_map_points"]
            if not isinstance(focus_map_points, list):
                msg = "'focus_map_points' must be a list"
                logger.error(msg)
                return {"success": False, "message": msg}
            if len(focus_map_points) != 3:
                msg = f"'focus_map_points' must contain exactly 3 reference points (got {len(focus_map_points)})"
                logger.error(msg)
                return {"success": False, "message": msg}
            for idx, point in enumerate(focus_map_points):
                if not isinstance(point, list):
                    msg = f"Focus map point {idx} must be a list [x, y, z]"
                    logger.error(msg)
                    return {"success": False, "message": msg}
                if len(point) != 3:
                    msg = f"Focus map point {idx} must have exactly 3 coordinates [x, y, z] (got {len(point)})"
                    logger.error(msg)
                    return {"success": False, "message": msg}
                try:
                    # Validate that all elements are numeric
                    [float(coord) for coord in point]
                except (ValueError, TypeError):
                    msg = f"Focus map point {idx} must contain numeric coordinates [x, y, z] in mm"
                    logger.error(msg)
                    return {"success": False, "message": msg}
        
        # Ensure microscope service is connected
        try:
            if microscope_id not in self.microscope_services:
                logger.info(f"Microscope {microscope_id} not connected. Attempting to setup connections...")
                await self.setup_connections()
            
            microscope_service = self.microscope_services.get(microscope_id)
            if not microscope_service:
                raise Exception(f"Microscope service {microscope_id} is not available after connection attempt")
        except Exception as e:
            msg = f"Failed to connect to microscope {microscope_id}: {e}"
            logger.error(msg)
            return {"success": False, "message": msg}

        linked_task_name = None
        if task_name:
            if task_name not in self.tasks:
                logger.warning(f"Task '{task_name}' not found for linking")
            else:
                linked_task_name = task_name

        # Generate action_id if not provided
        if not action_id:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            action_id = f"microscope_only_scan_{microscope_id}_{timestamp}"

        request = self._build_request(
            "manual-scan",
            task_name=linked_task_name,
            microscope_id=microscope_id,
            metadata={"action_id": action_id, "saved_data_type": saved_data_type},
        )

        logger.info(f"Starting microscope-only scan with action_id: {action_id}")

        try:
            async with self.admission_controller.hold(request):
                if linked_task_name:
                    logger.info(f"Linking scan to task '{linked_task_name}', updating status to 'active'")
                    await self._update_task_state_and_write_config(linked_task_name, status="active")

                # Mark microscope as critical during scan
                self.in_critical_operation = True
                logger.info(f"CRITICAL OPERATION START: Microscope-only scan on {microscope_id}")
                try:
                    self.critical_services.add(('microscope', microscope_id))
                except Exception:
                    pass

                try:
                    # Build scan configuration based on saved_data_type
                    full_scan_config = {
                        "saved_data_type": saved_data_type,
                        "illumination_settings": scan_config["illumination_settings"],
                        "do_contrast_autofocus": scan_config["do_contrast_autofocus"],
                        "do_reflection_af": scan_config["do_reflection_af"],
                        "action_ID": action_id,
                    }

                    if saved_data_type == "raw_images_well_plate":
                        # Add well plate type (default to '96' if not provided)
                        full_scan_config["well_plate_type"] = scan_config.get("well_plate_type", "96")
                        full_scan_config["wells_to_scan"] = scan_config["wells_to_scan"]
                        full_scan_config["Nx"] = scan_config["Nx"]
                        full_scan_config["Ny"] = scan_config["Ny"]
                        full_scan_config["dx"] = scan_config["dx"]
                        full_scan_config["dy"] = scan_config["dy"]
                        # Optional focus_map_points for raw_images_well_plate
                        if "focus_map_points" in scan_config:
                            full_scan_config["focus_map_points"] = scan_config["focus_map_points"]
                    elif saved_data_type == "raw_image_flexible":
                        full_scan_config["positions"] = scan_config["positions"]
                        # Optional focus_map_points for raw_image_flexible
                        if "focus_map_points" in scan_config:
                            full_scan_config["focus_map_points"] = scan_config["focus_map_points"]
                        # Optional move_for_autofocus for raw_image_flexible
                        if "move_for_autofocus" in scan_config:
                            full_scan_config["move_for_autofocus"] = scan_config["move_for_autofocus"]

                    logger.info(f"Initiating scan with config: {full_scan_config}")

                    scan_result = await microscope_service.scan_start(config=full_scan_config)
                    logger.info(f"Scan initiated successfully: {scan_result}")

                    await self._poll_scan_status(
                        microscope_service=microscope_service,
                    )

                    logger.info(f"Microscope-only scan completed successfully for action_id: {action_id}")

                    if linked_task_name and linked_task_name in self.tasks:
                        logger.info(f"Updating linked task '{linked_task_name}' status to 'waiting_for_next_run'")
                        await self._update_task_state_and_write_config(linked_task_name, status="waiting_for_next_run")

                    return {
                        "success": True,
                        "message": f"Microscope-only scan completed successfully on {microscope_id}",
                        "action_id": action_id,
                        "microscope_id": microscope_id,
                        "scan_type": saved_data_type
                    }
                finally:
                    self.in_critical_operation = False
                    logger.info(f"CRITICAL OPERATION END: Microscope-only scan on {microscope_id}")
                    try:
                        self.critical_services.discard(('microscope', microscope_id))
                    except Exception:
                        pass
        except ResourceBusyError as busy_error:
            message = f"Microscope-only scan on {microscope_id} rejected because the microscope is busy."
            logger.warning(message)
            return self._busy_response(message, busy_error)
        except (ConnectionError, OSError, RuntimeError) as e:
            error_msg = f"Microscope-only scan failed: {e}"
            logger.error(error_msg, exc_info=True)
            
            # Update linked task to error status
            if linked_task_name and linked_task_name in self.tasks:
                await self._update_task_state_and_write_config(linked_task_name, status="error")
            
            return {"success": False, "message": error_msg, "action_id": action_id}

