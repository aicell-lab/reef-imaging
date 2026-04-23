"""Task scheduling and time-lapse orchestration mixin."""
import asyncio
import copy
from datetime import datetime
from hypha_rpc import connect_to_server
from .core import logger, CONFIG_READ_INTERVAL, ORCHESTRATOR_LOOP_SLEEP
from reef_imaging.orchestration import ResourceBusyError


class TaskMixin:
    async def _poll_scan_status(self, microscope_service):
        """
        Poll the scan status from microscope service using scan_get_status().

        This method continuously polls the microscope service every 10 seconds to check
        the scan progress. It handles WebSocket interruptions gracefully by retrying
        failed status checks. Also monitors busy_status for enhanced state tracking.

        Args:
            microscope_service: The microscope service proxy to poll

        Returns:
            None when scan completes successfully

        Raises:
            Exception: If scan fails or encounters an error
        """
        poll_interval = 10  # seconds between status checks
        consecutive_failures = 0
        max_consecutive_failures = 3

        logger.info(f"Starting scan status polling (interval: {poll_interval}s)")

        while True:
            try:
                # Poll status from microscope service
                status_response = await asyncio.wait_for(
                    microscope_service.scan_get_status(),
                    timeout=15  # Give extra time for the RPC call itself
                )
                
                # Reset failure counter on successful poll
                consecutive_failures = 0
                
                # Extract status information
                status = status_response.get("state", "unknown")
                busy_status = status_response.get("busy_status", "unknown")
                
                # Log full response for debugging when status is unknown
                if status == "unknown":
                    logger.debug(f"Full scan status response: {status_response}")
                
                # Check terminal states
                if status == "completed":
                    logger.info("Scan completed successfully")
                    return
                
                elif status == "failed":
                    error = status_response.get("error", "Unknown error")
                    error_msg = f"Scan failed: {error}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                
                elif status == "running":
                    # Log busy_status if available for enhanced debugging
                    if busy_status != "unknown":
                        logger.debug(f"Scan running - busy_status: {busy_status}")
                    # Continue polling
                    await asyncio.sleep(poll_interval)
                
                elif status == "idle":
                    # Microscope service indicates it's idle (scan completed)
                    logger.info("Scan completed - microscope service is idle")
                    return
                
                elif status == "unknown":
                    # Handle unknown status
                    logger.warning(f"Unknown scan status: {status}. Continuing to poll...")
                    await asyncio.sleep(poll_interval)
                
                else:
                    logger.warning(f"Unknown scan status: {status}. Continuing to poll...")
                    await asyncio.sleep(poll_interval)
                    
            except asyncio.TimeoutError:
                consecutive_failures += 1
                logger.warning(f"Status poll timed out (attempt {consecutive_failures}/{max_consecutive_failures})")
                
                if consecutive_failures >= max_consecutive_failures:
                    error_msg = f"Failed to get scan status after {max_consecutive_failures} consecutive attempts"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                
                # Wait a bit before retrying
                await asyncio.sleep(poll_interval)
                
            except Exception as e:
                consecutive_failures += 1
                logger.warning(f"Error polling scan status (attempt {consecutive_failures}/{max_consecutive_failures}): {e}")
                
                if consecutive_failures >= max_consecutive_failures:
                    error_msg = f"Failed to poll scan status after {max_consecutive_failures} consecutive attempts: {e}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                
                # Wait before retrying
                await asyncio.sleep(poll_interval)

    async def _reap_finished_cycle_tasks(self):
        """Clean up completed background cycle tasks."""
        completed_task_names = [
            task_name
            for task_name, task in self._scheduled_cycle_tasks.items()
            if task.done()
        ]

        for task_name in completed_task_names:
            finished_task = self._scheduled_cycle_tasks.pop(task_name)
            try:
                await finished_task
            except Exception as exc:
                logger.error(f"Background cycle task '{task_name}' exited with an unexpected error: {exc}")

    async def _start_due_task(self, task_name: str, earliest_pending_tp: datetime) -> bool:
        """Attempt to start a due task if its resources are currently idle."""
        if task_name in self._scheduled_cycle_tasks:
            return False

        task_data = self.tasks.get(task_name)
        if not task_data:
            logger.warning(f"Task '{task_name}' disappeared before scheduling.")
            return False

        task_config_for_cycle = copy.deepcopy(task_data["config"])
        allocated_microscope_id = task_config_for_cycle.get("allocated_microscope")
        if not allocated_microscope_id:
            logger.error(f"Task {task_name} does not have an 'allocated_microscope'. Marking as error.")
            await self._update_task_state_and_write_config(task_name, status="error")
            return False

        if allocated_microscope_id not in self.configured_microscopes_info:
            logger.error(f"Task '{task_name}' references unknown microscope '{allocated_microscope_id}'.")
            await self._update_task_state_and_write_config(task_name, status="error")
            return False

        scan_mode = task_config_for_cycle.get("scan_mode", "full_automation")
        incubator_slot = None
        if scan_mode == "full_automation":
            incubator_slot = task_config_for_cycle.get("incubator_slot")
            if incubator_slot is None:
                logger.error(f"Task '{task_name}' is full_automation but missing 'incubator_slot'.")
                await self._update_task_state_and_write_config(task_name, status="error")
                return False

        try:
            if scan_mode == "full_automation":
                if not self.incubator or not self.robotic_arm or allocated_microscope_id not in self.microscope_services:
                    await self.setup_connections()
                if not self.incubator or not self.robotic_arm:
                    logger.warning(f"Shared transport services are not ready for task '{task_name}'. Will retry later.")
                    return False
            elif allocated_microscope_id not in self.microscope_services:
                await self.setup_connections()
        except Exception as setup_error:
            logger.warning(f"Failed to prepare services for task '{task_name}': {setup_error}")
            return False

        target_microscope_service = self.microscope_services.get(allocated_microscope_id)
        if not target_microscope_service:
            logger.warning(f"Microscope '{allocated_microscope_id}' is not available for task '{task_name}'.")
            return False

        request = self._build_request(
            "scheduled-cycle",
            task_name=task_name,
            microscope_id=allocated_microscope_id,
            incubator_slot=incubator_slot,
            metadata={
                "trigger": "scheduler",
                "timepoint": earliest_pending_tp.strftime("%Y-%m-%dT%H:%M:%S"),
                "scan_mode": scan_mode,
            },
        )

        try:
            lease = await self.admission_controller.try_acquire(request)
        except ResourceBusyError as busy_error:
            blockers = ", ".join(
                f"{blocker.resource}:{blocker.operation_type}"
                for blocker in busy_error.blockers
            )
            logger.info(f"Task '{task_name}' is due but blocked by active resources: {blockers}")
            return False

        await self._update_task_state_and_write_config(task_name, status="active")
        self._mark_task_running(task_name)

        try:
            cycle_task = asyncio.create_task(
                self._run_scheduled_cycle(
                    task_name=task_name,
                    task_config_for_cycle=task_config_for_cycle,
                    target_microscope_service=target_microscope_service,
                    allocated_microscope_id=allocated_microscope_id,
                    current_pending_tp_to_process=earliest_pending_tp,
                    lease=lease,
                )
            )
            self._scheduled_cycle_tasks[task_name] = cycle_task
            return True
        except Exception:
            self._mark_task_not_running(task_name)
            await self.admission_controller.release(lease.operation_id)
            raise

    async def _run_scheduled_cycle(
        self,
        *,
        task_name: str,
        task_config_for_cycle: dict,
        target_microscope_service,
        allocated_microscope_id: str,
        current_pending_tp_to_process: datetime,
        lease,
    ):
        """Run one scheduled cycle in the background while holding the task lease."""
        scan_mode = task_config_for_cycle.get("scan_mode", "full_automation")

        try:
            if scan_mode == "full_automation":
                await self.run_cycle(task_config_for_cycle, target_microscope_service, allocated_microscope_id)
            else:
                await self.run_microscope_only_cycle(task_config_for_cycle, target_microscope_service, allocated_microscope_id)

            logger.info(
                f"Cycle for task {task_name} on {allocated_microscope_id}, "
                f"time point {current_pending_tp_to_process.isoformat()} success."
            )
            await self._update_task_state_and_write_config(
                task_name,
                status="waiting_for_next_run",
                current_tp_to_move_to_imaged=current_pending_tp_to_process,
            )
        except Exception as cycle_error:
            logger.error(
                f"Cycle for task {task_name} on {allocated_microscope_id}, "
                f"time point {current_pending_tp_to_process.isoformat()} failed: {cycle_error}"
            )
            await self._update_task_state_and_write_config(task_name, status="error")
        finally:
            self._mark_task_not_running(task_name)
            self._scheduled_cycle_tasks.pop(task_name, None)
            await self.admission_controller.release(lease.operation_id)

    async def run_cycle(self, task_config, microscope_service, allocated_microscope_id): # MODIFIED: added microscope_service, allocated_microscope_id
        """Run the complete load-scan-unload process for a given task on a specific microscope."""
        task_name = task_config["name"]
        incubator_slot = task_config["incubator_slot"]
        action_id = f"{task_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting imaging cycle for task: {task_name} on microscope {allocated_microscope_id} with action_id: {action_id}")

        # Verify essential services (incubator, arm) are available - microscope_service is passed in and presumed connected by caller
        if not self.incubator or not self.robotic_arm:
            error_msg = f"Incubator or Robotic Arm not available for task {task_name} on microscope {allocated_microscope_id}."
            logger.error(error_msg)
            # Attempt to reconnect essential shared services.
            # We don't try to reconnect the specific microscope here as that's handled by run_time_lapse logic.
            await self.setup_connections() 
            if not self.incubator or not self.robotic_arm: # Check again
                raise Exception(f"Essential services still unavailable after reconnect attempt: {error_msg}")
        
        # Reset all task status on the services themselves before starting a new cycle
        try:
            logger.info(f"Resetting task statuses on services for task {task_name} (microscope: {allocated_microscope_id})...")
            logger.info(f"Service task statuses reset for task {task_name} on {allocated_microscope_id}.")
        except Exception as e:
            logger.error(f"Error resetting task statuses on services for {task_name} on {allocated_microscope_id}: {e}. Proceeding with caution.")

        try:
            # Pass allocated_microscope_id to transport operations
            await self._execute_load_operation(incubator_slot=incubator_slot, microscope_id_str=allocated_microscope_id)
            
            # Get well plate type from incubator service (read-only from incubator)
            try:
                well_plate_type = await self.incubator.get_well_plate_type(incubator_slot)
                logger.info(f"Retrieved well plate type '{well_plate_type}' for slot {incubator_slot} from incubator service.")
            except Exception as e:
                logger.error(f"Failed to get well plate type from incubator for slot {incubator_slot}: {e}. Using default '96'.")
                well_plate_type = "96"  # Default fallback
            
            # Mark as critical operation - microscope will be scanning
            self.in_critical_operation = True
            logger.info("CRITICAL OPERATION START: Microscope scanning well plate")
            # Only the target microscope should be treated as critical during scan
            try:
                self.critical_services.add(('microscope', allocated_microscope_id))
            except Exception:
                pass
            
            try:
                saved_data_type = task_config.get("saved_data_type", "raw_images_well_plate")
                
                logger.info(f"Building scan config for task {task_name}: saved_data_type={saved_data_type}")
                
                scan_config = {
                    "saved_data_type": saved_data_type,
                    "illumination_settings": task_config["illumination_settings"],
                    "do_contrast_autofocus": task_config["do_contrast_autofocus"],
                    "do_reflection_af": task_config["do_reflection_af"],
                    "action_ID": action_id,
                }
                
                if saved_data_type == "raw_images_well_plate":
                    scan_config.update({
                        "well_plate_type": well_plate_type,
                        "wells_to_scan": task_config["wells_to_scan"],
                        "Nx": task_config["Nx"],
                        "Ny": task_config["Ny"],
                        "dx": task_config["dx"],
                        "dy": task_config["dy"],
                    })
                    # Optional focus_map_points for raw_images_well_plate
                    if "focus_map_points" in task_config:
                        scan_config["focus_map_points"] = task_config["focus_map_points"]
                        logger.info(f"Focus map enabled with {len(task_config['focus_map_points'])} reference points")
                    logger.info(f"Well plate scan config: wells={task_config['wells_to_scan']}, Nx={task_config['Nx']}, Ny={task_config['Ny']}")
                else:
                    scan_config["positions"] = task_config.get("positions", [])
                    # Optional focus_map_points for raw_image_flexible
                    if "focus_map_points" in task_config:
                        scan_config["focus_map_points"] = task_config["focus_map_points"]
                        logger.info(f"Focus map enabled with {len(task_config['focus_map_points'])} reference points")
                    # Optional move_for_autofocus for raw_image_flexible
                    if "move_for_autofocus" in task_config:
                        scan_config["move_for_autofocus"] = task_config["move_for_autofocus"]
                        logger.info(f"Move for autofocus enabled: {task_config['move_for_autofocus']}")
                    logger.info(f"Flexible scan config: {len(scan_config['positions'])} positions")
                
                logger.info(f"Sending scan_config to microscope: saved_data_type={scan_config['saved_data_type']}")
                await asyncio.wait_for(
                    microscope_service.scan_start(config=scan_config), timeout=60
                )

                await self._poll_scan_status(
                    microscope_service=microscope_service,
                )
                
            finally:
                # Always reset critical operation flag after scanning
                self.in_critical_operation = False
                logger.info("CRITICAL OPERATION END: Microscope scan complete")
                # Unmark microscope critical state
                try:
                    self.critical_services.discard(('microscope', allocated_microscope_id))
                except Exception:
                    pass
            
            await self._execute_unload_operation(incubator_slot=incubator_slot, microscope_id_str=allocated_microscope_id)
            
            logger.info(f"Imaging cycle for task {task_name} on microscope {allocated_microscope_id} (action_id: {action_id}) completed successfully.")
            
        except Exception as e:
            logger.error(f"Cycle failed for task {task_name} on microscope {allocated_microscope_id}: {e}")
            logger.critical("CRITICAL: Cycle failed - NOT attempting automatic unload for safety reasons.")
            logger.critical("Manual intervention required. Check microscope and sample status before proceeding.")
            # DO NOT attempt cleanup unload - this is dangerous if scan is still running or timed out
            # Manual intervention is required to safely assess the situation
            raise # Re-raise the original exception

    async def run_microscope_only_cycle(self, task_config, microscope_service, allocated_microscope_id):
        """Run microscope-only scan without robotic arm/incubator. Supports raw_images_well_plate and raw_image_flexible."""
        task_name = task_config["name"]
        action_id = f"{task_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        saved_data_type = task_config.get("saved_data_type", "raw_images_well_plate")
        
        self.in_critical_operation = True
        self.critical_services.add(('microscope', allocated_microscope_id))
        
        try:
            scan_config = {
                "saved_data_type": saved_data_type,
                "illumination_settings": task_config["illumination_settings"],
                "do_contrast_autofocus": task_config["do_contrast_autofocus"],
                "do_reflection_af": task_config["do_reflection_af"],
                "action_ID": action_id,
            }
            
            if saved_data_type == "raw_images_well_plate":
                scan_config.update({
                    "well_plate_type": task_config.get("well_plate_type", "96"),
                    "wells_to_scan": task_config["wells_to_scan"],
                    "Nx": task_config["Nx"],
                    "Ny": task_config["Ny"],
                    "dx": task_config.get("dx", 0.8),
                    "dy": task_config.get("dy", 0.8),
                })
                # Optional focus_map_points for raw_images_well_plate
                if "focus_map_points" in task_config:
                    scan_config["focus_map_points"] = task_config["focus_map_points"]
            else:
                scan_config["positions"] = task_config.get("positions", [])
                # Optional focus_map_points for raw_image_flexible
                if "focus_map_points" in task_config:
                    scan_config["focus_map_points"] = task_config["focus_map_points"]
                # Optional move_for_autofocus for raw_image_flexible
                if "move_for_autofocus" in task_config:
                    scan_config["move_for_autofocus"] = task_config["move_for_autofocus"]
            
            await asyncio.wait_for(
                microscope_service.scan_start(config=scan_config), timeout=60
            )
            await self._poll_scan_status(microscope_service)
            
        finally:
            self.in_critical_operation = False
            self.critical_services.discard(('microscope', allocated_microscope_id))

    async def run_time_lapse(self):
        """Main orchestration loop to manage and run imaging tasks based on config.json."""
        logger.info("Orchestrator run_time_lapse started.")
        loop = asyncio.get_running_loop()
        last_config_read_time = 0.0

        while True:
            current_time_naive = datetime.now()

            if (loop.time() - last_config_read_time) > CONFIG_READ_INTERVAL:
                await self._load_and_update_tasks()
                last_config_read_time = loop.time()

            await self._reap_finished_cycle_tasks()

            if not self.tasks:
                logger.debug("No tasks loaded yet.")

            eligible_tasks_for_run = []

            for task_name, task_data in list(self.tasks.items()):
                if task_name in self._scheduled_cycle_tasks:
                    continue

                internal_config = task_data["config"]
                status = task_data["status"]
                pending_datetimes = internal_config.get("pending_datetimes", [])

                if status in ["completed", "error", "uploading", "paused"]:
                    continue
                
                if not pending_datetimes:
                    logger.debug(f"Task '{task_name}' skipped, no pending time points.")
                    if status not in ["completed", "uploading"]:
                        logger.warning(f"Task '{task_name}' has status '{status}' but no pending points. Marking completed.")
                        await self._update_task_state_and_write_config(task_name, status="completed")
                    continue

                earliest_tp_for_this_task = pending_datetimes[0]
                if current_time_naive >= earliest_tp_for_this_task:
                    eligible_tasks_for_run.append((task_name, earliest_tp_for_this_task))
                    logger.debug(f"Task '{task_name}' is eligible with TP: {earliest_tp_for_this_task.isoformat()}")
                else:
                    logger.debug(f"Task '{task_name}' not due yet (earliest TP: {earliest_tp_for_this_task.isoformat()}).")
            eligible_tasks_for_run.sort(key=lambda item: item[1])
            started_any = False
            for task_name, due_time in eligible_tasks_for_run:
                started = await self._start_due_task(task_name, due_time)
                started_any = started_any or started

            # Determine minimum wait time before next loop iteration
            min_wait_time = ORCHESTRATOR_LOOP_SLEEP
            next_potential_run_time = None
            for task_name, task_data_val in self.tasks.items():
                if task_name in self._scheduled_cycle_tasks:
                    continue
                if task_data_val["status"] not in ["completed", "error", "uploading", "paused"] and task_data_val["config"]["pending_datetimes"]:
                    earliest_tp = task_data_val["config"]["pending_datetimes"][0]
                    if next_potential_run_time is None or earliest_tp < next_potential_run_time:
                        next_potential_run_time = earliest_tp

            if next_potential_run_time and next_potential_run_time > current_time_naive:
                wait_seconds = (next_potential_run_time - current_time_naive).total_seconds()
                min_wait_time = max(0.1, min(wait_seconds, ORCHESTRATOR_LOOP_SLEEP))
                logger.debug(f"Calculated dynamic sleep: {min_wait_time:.2f}s until next potential task time ({next_potential_run_time.isoformat()})")
            elif eligible_tasks_for_run and not started_any:
                min_wait_time = 1.0

            if self._scheduled_cycle_tasks:
                min_wait_time = min(min_wait_time, 1.0)

            await asyncio.sleep(min_wait_time)

    async def cancel_running_cycles(self):
        """Cancel all running scheduled cycle tasks during shutdown."""
        if not self._scheduled_cycle_tasks:
            return

        logger.info(f"Cancelling {len(self._scheduled_cycle_tasks)} running scheduled cycle task(s)...")
        running_items = list(self._scheduled_cycle_tasks.items())
        for _, task in running_items:
            task.cancel()

        for task_name, task in running_items:
            try:
                await task
            except asyncio.CancelledError:
                logger.info(f"Scheduled cycle task '{task_name}' cancelled during shutdown.")
            except Exception as exc:
                logger.error(f"Scheduled cycle task '{task_name}' failed while shutting down: {exc}")

        self._scheduled_cycle_tasks.clear()
        self._active_task_names.clear()
        self._refresh_legacy_active_task_name()

    async def _register_self_as_hypha_service(self):
        registration_succeeded = False

        logger.info(
            f"Registering orchestrator as a Hypha service with ID '{self.orchestrator_hypha_service_id}' "
            f"on cloud server '{self.orchestrator_hypha_server_url}' in workspace '{self.workspace}'"
        )
        if self.token_for_orchestrator_registration:
            server_config_for_registration = {
                "server_url": self.orchestrator_hypha_server_url,
                "ping_interval": 30,
                "workspace": self.workspace,
                "token": self.token_for_orchestrator_registration,
            }

            self.orchestrator_hypha_server_connection = await connect_to_server(server_config_for_registration)
            logger.info(
                f"Successfully connected to Hypha server: {self.orchestrator_hypha_server_url} "
                "for orchestrator registration"
            )
            registered_service = await self.orchestrator_hypha_server_connection.register_service(
                self._build_service_api(),
                overwrite=True,
            )
            logger.info(
                "Orchestrator management service registered successfully on cloud Hypha. "
                f"Service ID: {registered_service.id}"
            )
            registration_succeeded = True
        else:
            logger.warning(
                "REEF_WORKSPACE_TOKEN is not set in environment. Skipping cloud orchestrator registration."
            )

        logger.info(
            f"Registering orchestrator on local Hypha server '{self.server_url}' in workspace "
            f"'{self.local_workspace or '<missing>'}'"
        )
        try:
            local_connection = await self._ensure_local_server_connection()
            if local_connection:
                registered_service = await local_connection.register_service(
                    self._build_service_api(),
                    overwrite=True,
                )
                logger.info(
                    "Orchestrator management service registered successfully on local Hypha. "
                    f"Service ID: {registered_service.id}"
                )
                registration_succeeded = True
        except (ConnectionError, OSError, RuntimeError) as exc:
            logger.error(f"Failed to register orchestrator service on local Hypha: {exc}")

        if not registration_succeeded:
            logger.warning("Orchestrator service was not registered on any Hypha endpoint.")
