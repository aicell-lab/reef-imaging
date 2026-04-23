"""Plate transport coordination mixin."""
import asyncio
from .core import logger, HamiltonBusyError, TransportPreconditionError
from reef_imaging.orchestration import ResourceBusyError


class TransportMixin:
    def _normalize_hamilton_rail_position(self, position: str) -> str:
        key = str(position).strip().lower().replace("_", "-").replace(" ", "-")
        if key == "hamilton":
            return "hamilton"
        if key in {"robotic-arm", "arm"}:
            return "robotic-arm"
        raise ValueError("position must be 'hamilton' or 'robotic-arm'")

    async def _run_manual_transport_operation(self, action: str, incubator_slot: int, microscope_id: str):
        """Execute a transport request directly if all required resources are idle."""
        if not self.incubator or not self.robotic_arm or microscope_id not in self.microscope_services:
            setup_ok = await self.setup_connections()
            if not setup_ok or microscope_id not in self.microscope_services:
                raise Exception(f"Transport services are not ready for microscope {microscope_id}.")

        request = self._build_request(
            f"manual-{action}",
            microscope_id=microscope_id,
            incubator_slot=incubator_slot,
            extra_resources=self._transport_resources(),
            metadata={"trigger": "api", "action": action},
        )

        async with self.admission_controller.hold(request):
            if action == "load":
                await self._execute_load_operation(
                    incubator_slot,
                    microscope_id,
                    manage_transport_resources=False,
                )
            elif action == "unload":
                await self._execute_unload_operation(
                    incubator_slot,
                    microscope_id,
                    manage_transport_resources=False,
                )
            else:
                raise ValueError(f"Unknown manual transport action '{action}'.")

    async def transport_plate_api(self, from_device: str, to_device: str, slot: int = None):
        """
        Unified API endpoint to transport a plate between devices.
        
        Args:
            from_device: Source device service ID - 'incubator', 'hamilton', or microscope ID
            to_device: Target device service ID - 'incubator', 'hamilton', or microscope ID  
            slot: Incubator slot number (1-42). Required when incubator is involved
                  to identify and track which plate is being moved.
        
        Supported device IDs:
        - 'incubator' - The Cytomat incubator
        - 'hamilton' - The Hamilton liquid handler
        - 'microscope-squid-1' - Microscope 1
        - 'microscope-squid-2' - Microscope 2  
        - 'microscope-squid-plus-3' - Microscope 3
        
        Examples:
        - incubator -> microscope: `transport_plate("incubator", "microscope-squid-1", slot=5)`
        - microscope -> incubator: `transport_plate("microscope-squid-1", "incubator", slot=5)`  
        - incubator -> hamilton: `transport_plate("incubator", "hamilton", slot=5)`
        - hamilton -> microscope: `transport_plate("hamilton", "microscope-squid-2", slot=5)`
        - microscope -> hamilton: `transport_plate("microscope-squid-1", "hamilton", slot=5)`
        """
        logger.info(f"API call: transport_plate from '{from_device}' to '{to_device}' (slot={slot})")
        
        from_device = from_device.lower().strip()
        to_device = to_device.lower().strip()
        
        valid_devices = {"incubator", "hamilton", "microscope-squid-1", "microscope-squid-2", "microscope-squid-plus-3"}
        if from_device not in valid_devices:
            return {"success": False, "message": f"Invalid from_device '{from_device}'. Must be one of: {valid_devices}"}
        if to_device not in valid_devices:
            return {"success": False, "message": f"Invalid to_device '{to_device}'. Must be one of: {valid_devices}"}
        if from_device == to_device:
            return {"success": False, "message": f"Source and target devices cannot be the same: '{from_device}'"}
        
        incubator_involved = from_device == "incubator" or to_device == "incubator"
        if incubator_involved:
            if slot is None:
                return {"success": False, "message": "slot is required when incubator is involved"}
            if not isinstance(slot, int) or slot < 1 or slot > 42:
                return {"success": False, "message": f"slot must be an integer between 1-42, got {slot}"}
        
        try:
            if from_device == "incubator" and to_device.startswith("microscope-"):
                return await self._load_plate_api_wrapper(slot, to_device)
            elif from_device.startswith("microscope-") and to_device == "incubator":
                return await self._unload_plate_api_wrapper(slot, from_device)
            elif from_device == "incubator" and to_device == "hamilton":
                return await self._load_to_hamilton_api_wrapper(slot)
            elif from_device == "hamilton" and to_device == "incubator":
                return await self._unload_from_hamilton_api_wrapper(slot)
            elif from_device.startswith("microscope-") and to_device == "hamilton":
                return await self._microscope_to_hamilton_api_wrapper(slot, from_device)
            elif from_device == "hamilton" and to_device.startswith("microscope-"):
                return await self._hamilton_to_microscope_api_wrapper(slot, to_device)
            elif from_device.startswith("microscope-") and to_device.startswith("microscope-"):
                return await self._microscope_to_microscope_api_wrapper(slot, from_device, to_device)
            else:
                return {"success": False, "message": f"Unsupported route: '{from_device}' -> '{to_device}'"}
        except Exception as e:
            logger.error(f"Transport operation failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _get_hamilton_executor_proxy(self, *, refresh_if_missing: bool = True):
        """Return the Hamilton executor proxy, optionally refreshing local services."""

        if self.hamilton_executor is None and refresh_if_missing:
            await self.setup_connections()
        return self.hamilton_executor

    async def _get_hamilton_executor_status(self, *, refresh_if_missing: bool = True):
        """Fetch the current Hamilton executor status, or ``None`` if unavailable."""

        service = await self._get_hamilton_executor_proxy(refresh_if_missing=refresh_if_missing)
        if service is None:
            return None
        return await service.get_status()

    async def _assert_hamilton_idle_for_transport(self):
        """Reject Hamilton transport while the executor is busy with a protocol run."""

        status = await self._get_hamilton_executor_status()
        if status is None:
            raise RuntimeError(
                f"Hamilton executor service '{self.hamilton_executor_id}' is not available."
            )
        if status.get("busy"):
            current_action = status.get("current_action_id") or status.get("action_id")
            raise HamiltonBusyError(
                f"Hamilton is busy executing a protocol"
                f"{f' ({current_action})' if current_action else ''}."
            )
        return status

    async def _execute_move_hamilton_plate_rail(
        self,
        position: str,
        *,
        manage_transport_resources: bool = True,
    ):
        """Move the Hamilton slide rail to the requested side via the robotic-arm service."""
        normalized_position = self._normalize_hamilton_rail_position(position)

        if manage_transport_resources:
            request = self._build_request(
                "move-hamilton-rail",
                extra_resources=self._hamilton_rail_resources(),
                metadata={"position": normalized_position},
            )
            async with self.admission_controller.hold(request, wait=True):
                return await self._execute_move_hamilton_plate_rail(
                    normalized_position,
                    manage_transport_resources=False,
                )

        if not self.robotic_arm or not self.hamilton_executor:
            setup_ok = await self.setup_connections()
            if not setup_ok:
                raise RuntimeError("Hamilton rail services are not ready.")
        if not self.robotic_arm:
            raise RuntimeError("Robotic arm service is not available.")
        if not self.hamilton_executor:
            raise RuntimeError(
                f"Hamilton executor service '{self.hamilton_executor_id}' is not available."
            )

        await self._assert_hamilton_idle_for_transport()
        result = await asyncio.wait_for(
            self.robotic_arm.move_plate_rail(position=normalized_position),
            timeout=120,
        )
        logger.info(f"Hamilton plate rail moved to '{normalized_position}'")
        return {
            "position": normalized_position,
            "robotic_arm_result": result,
        }

    def _raise_transport_precondition(self, *, route: str, incubator_slot: int, detail: str):
        """Raise a user-facing precondition error for a transport request."""

        raise TransportPreconditionError(
            f"Cannot execute {route} transport for slot {incubator_slot}: {detail}"
        )

    async def _execute_load_operation(
        self,
        incubator_slot: int,
        microscope_id_str: str,
        *,
        manage_transport_resources: bool = True,
    ):
        """Execute the load operation: move sample from incubator to microscope."""
        if manage_transport_resources:
            request = self._build_request(
                "transport-load",
                extra_resources=self._transport_resources(),
                metadata={
                    "phase": "load",
                    "microscope_id": microscope_id_str,
                    "incubator_slot": incubator_slot,
                },
            )
            async with self.admission_controller.hold(request, wait=True):
                await self._execute_load_operation(
                    incubator_slot,
                    microscope_id_str,
                    manage_transport_resources=False,
                )
                return

        target_microscope_service = self.microscope_services.get(microscope_id_str)
        if not target_microscope_service:
            raise Exception(f"Microscope service {microscope_id_str} is not connected.")

        # Verify actual sample location from incubator service (source of truth)
        # This protects against stale in-memory flags after a crash/restart
        try:
            actual_location = await self.incubator.get_sample_location(incubator_slot)
            logger.info(f"Incubator reports sample location for slot {incubator_slot}: {actual_location}")
            if actual_location == microscope_id_str:
                self.sample_on_microscope_flags[microscope_id_str] = True
                logger.info(f"Sample already on microscope {microscope_id_str} per incubator. Skipping load.")
                return
            elif actual_location == "incubator_slot":
                self.sample_on_microscope_flags[microscope_id_str] = False
            # For other locations (e.g. "robotic_arm"), fall through to the in-memory flag
        except (ConnectionError, OSError, asyncio.TimeoutError, AttributeError) as e:
            logger.warning(f"Could not verify sample location from incubator for slot {incubator_slot}: {e}. Falling back to in-memory flag.")

        if self.sample_on_microscope_flags.get(microscope_id_str, False):
            logger.info(f"Sample plate already on microscope {microscope_id_str} (in-memory flag). Skipping load.")
            return

        logger.info(f"Loading sample from incubator slot {incubator_slot} to microscope {microscope_id_str}...")
        
        # Mark critical operation
        self.in_critical_operation = True
        logger.info("CRITICAL OPERATION START: Robotic arm loading sample")
        critical_services = [
            ('robotic_arm', self.robotic_arm_id),
            ('microscope', microscope_id_str),
            ('incubator', self.incubator_id)
        ]
        self._mark_critical_services(critical_services)
        
        try:
            # Start parallel operations: prepare incubator and home the stage
            await asyncio.gather(
                self.incubator.get_sample_from_slot_to_transfer_station(incubator_slot),
                target_microscope_service.home_stage(),
            )
            
            # Move sample with robotic arm (unified transport API)
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await asyncio.wait_for(
                self.robotic_arm.transport_plate(from_device="incubator", to_device=microscope_id_str),
                timeout=600,
            )
            
            # Return microscope stage and update location
            await asyncio.gather(
                self.incubator.update_sample_location(incubator_slot, microscope_id_str),
                target_microscope_service.return_stage()
            )
            
            logger.info(f"Sample loaded onto microscope {microscope_id_str}.")
            self.sample_on_microscope_flags[microscope_id_str] = True
            
        except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError) as e:
            error_msg = f"Failed to load sample from slot {incubator_slot} to microscope {microscope_id_str}: {e}"
            logger.error(error_msg)
            self.sample_on_microscope_flags[microscope_id_str] = False
            raise RuntimeError(error_msg) from e
        finally:
            self.in_critical_operation = False
            logger.info("CRITICAL OPERATION END: Robotic arm load complete")
            self._unmark_critical_services(critical_services)

    async def _load_plate_api_wrapper(self, slot: int, microscope_id: str):
        """Internal wrapper for incubator -> microscope transport."""
        if microscope_id not in self.configured_microscopes_info:
            return {"success": False, "message": f"Microscope ID '{microscope_id}' not found in configured microscopes."}
        try:
            await self._run_manual_transport_operation("load", slot, microscope_id)
            return {"success": True, "message": f"Load from slot {slot} to {microscope_id} completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response("Load request rejected - orchestrator is busy.", busy_error)
        except Exception as e:
            logger.error(f"Load operation failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _unload_plate_api_wrapper(self, slot: int, microscope_id: str):
        """Internal wrapper for microscope -> incubator transport."""
        if microscope_id not in self.configured_microscopes_info:
            return {"success": False, "message": f"Microscope ID '{microscope_id}' not found in configured microscopes."}
        try:
            await self._run_manual_transport_operation("unload", slot, microscope_id)
            return {"success": True, "message": f"Unload from {microscope_id} to slot {slot} completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response("Unload request rejected - orchestrator is busy.", busy_error)
        except Exception as e:
            logger.error(f"Unload operation failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _microscope_to_microscope_api_wrapper(self, slot: int, from_microscope: str, to_microscope: str):
        """Internal wrapper for microscope -> microscope transport."""
        if from_microscope not in self.configured_microscopes_info:
            return {"success": False, "message": f"Source microscope ID '{from_microscope}' not found in configured microscopes."}
        if to_microscope not in self.configured_microscopes_info:
            return {"success": False, "message": f"Target microscope ID '{to_microscope}' not found in configured microscopes."}
        if from_microscope == to_microscope:
            return {"success": False, "message": f"Source and target microscope cannot be the same: '{from_microscope}'"}
        
        from_service = self.microscope_services.get(from_microscope)
        to_service = self.microscope_services.get(to_microscope)
        
        # Verify sample is on the source microscope
        try:
            actual_location = await self.incubator.get_sample_location(slot)
            logger.info(f"Actual sample location for slot {slot}: {actual_location}")
            if actual_location != from_microscope:
                return {"success": False, "message": f"Sample not on source microscope {from_microscope}, current location: {actual_location}"}
        except (ConnectionError, OSError, asyncio.TimeoutError, AttributeError) as e:
            logger.warning(f"Could not verify sample location from incubator for slot {slot}: {e}. Proceeding anyway.")
        
        if not self.sample_on_microscope_flags.get(from_microscope, False):
            return {"success": False, "message": f"Sample plate not on microscope {from_microscope} according to flags"}
        
        try:
            logger.info(f"Microscope-to-microscope: Moving plate from {from_microscope} to {to_microscope}")
            
            # Mark critical operation
            self.in_critical_operation = True
            logger.info("CRITICAL OPERATION START: Robotic arm microscope-to-microscope transport")
            critical_services = [
                ('robotic_arm', self.robotic_arm_id),
                ('microscope', from_microscope),
                ('microscope', to_microscope),
            ]
            self._mark_critical_services(critical_services)
            
            try:
                # Home stages on both microscopes
                await asyncio.gather(
                    from_service.home_stage(),
                    to_service.home_stage(),
                )
                
                # Update location to robotic arm
                await self.incubator.update_sample_location(slot, "robotic_arm")
                
                # Direct transport: grab from microscope 1, put on microscope 2
                await asyncio.wait_for(
                    self.robotic_arm.transport_plate(from_device=from_microscope, to_device=to_microscope),
                    timeout=600,
                )
                
                # Return stages and update location
                await asyncio.gather(
                    from_service.return_stage(),
                    to_service.return_stage(),
                )
                await self.incubator.update_sample_location(slot, to_microscope)
                
                # Update flags
                self.sample_on_microscope_flags[from_microscope] = False
                self.sample_on_microscope_flags[to_microscope] = True
                
                logger.info(f"Plate moved from {from_microscope} to {to_microscope}")
                
            finally:
                self.in_critical_operation = False
                logger.info("CRITICAL OPERATION END: Robotic arm microscope-to-microscope transport complete")
                self._unmark_critical_services(critical_services)
            
            return {"success": True, "message": f"Transport from {from_microscope} to {to_microscope} completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response("Microscope-to-microscope transport request rejected - orchestrator is busy.", busy_error)
        except Exception as e:
            logger.error(f"Microscope-to-microscope transport failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _execute_load_to_hamilton_operation(
        self,
        incubator_slot: int,
        *,
        manage_transport_resources: bool = True,
    ):
        """Execute the load operation: move sample from incubator to Hamilton."""
        if manage_transport_resources:
            request = self._build_request(
                "transport-load-hamilton",
                extra_resources=self._hamilton_transport_resources(),
                metadata={
                    "phase": "load_to_hamilton",
                    "incubator_slot": incubator_slot,
                },
            )
            async with self.admission_controller.hold(request, wait=True):
                await self._execute_load_to_hamilton_operation(
                    incubator_slot,
                    manage_transport_resources=False,
                )
                return

        # Verify sample is in incubator before loading
        try:
            actual_location = await self.incubator.get_sample_location(incubator_slot)
            logger.info(f"Incubator reports sample location for slot {incubator_slot}: {actual_location}")
            if actual_location != "incubator_slot":
                self._raise_transport_precondition(
                    route="incubator -> hamilton",
                    incubator_slot=incubator_slot,
                    detail=f"expected sample at incubator_slot, found {actual_location}",
                )
        except TransportPreconditionError:
            raise
        except Exception as e:
            logger.warning(f"Could not verify sample location from incubator for slot {incubator_slot}: {e}. Proceeding anyway.")

        await self._assert_hamilton_idle_for_transport()
        logger.info(f"Loading sample from incubator slot {incubator_slot} to Hamilton...")

        # Mark critical operation
        self.in_critical_operation = True
        logger.info("CRITICAL OPERATION START: Robotic arm loading sample to Hamilton")
        critical_services = [
            ('robotic_arm', self.robotic_arm_id),
            ('incubator', self.incubator_id)
        ]
        self._mark_critical_services(critical_services)

        try:
            # Start parallel operations: prepare incubator
            await asyncio.gather(
                self.incubator.get_sample_from_slot_to_transfer_station(incubator_slot),
            )

            # Move sample with robotic arm: incubator -> Hamilton (unified transport API)
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await asyncio.wait_for(
                self.robotic_arm.transport_plate(from_device="incubator", to_device="hamilton"),
                timeout=600,
            )

            # Update sample location to Hamilton
            await self.incubator.update_sample_location(incubator_slot, "hamilton")

            logger.info("Sample loaded onto Hamilton.")

        except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError) as e:
            error_msg = f"Failed to load sample from slot {incubator_slot} to Hamilton: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e
        finally:
            self.in_critical_operation = False
            logger.info("CRITICAL OPERATION END: Robotic arm load to Hamilton complete")
            self._unmark_critical_services(critical_services)

    async def _execute_unload_from_hamilton_operation(
        self,
        incubator_slot: int,
        *,
        manage_transport_resources: bool = True,
    ):
        """Execute the unload operation: move sample from Hamilton to incubator."""
        if manage_transport_resources:
            request = self._build_request(
                "transport-unload-hamilton",
                extra_resources=self._hamilton_transport_resources(),
                metadata={
                    "phase": "unload_from_hamilton",
                    "incubator_slot": incubator_slot,
                },
            )
            async with self.admission_controller.hold(request, wait=True):
                await self._execute_unload_from_hamilton_operation(
                    incubator_slot,
                    manage_transport_resources=False,
                )
                return

        # Verify sample is at Hamilton before unloading
        try:
            actual_location = await self.incubator.get_sample_location(incubator_slot)
            logger.info(f"Actual sample location for slot {incubator_slot}: {actual_location}")

            if actual_location != "hamilton":
                self._raise_transport_precondition(
                    route="hamilton -> incubator",
                    incubator_slot=incubator_slot,
                    detail=f"expected sample at hamilton, found {actual_location}",
                )
        except TransportPreconditionError:
            raise
        except Exception as e:
            logger.warning(f"Could not verify sample location from incubator for slot {incubator_slot}: {e}. Proceeding anyway.")

        await self._assert_hamilton_idle_for_transport()
        logger.info(f"Unloading sample from Hamilton to incubator slot {incubator_slot}...")

        # Mark critical operation
        self.in_critical_operation = True
        logger.info("CRITICAL OPERATION START: Robotic arm unloading sample from Hamilton")
        critical_services = [
            ('robotic_arm', self.robotic_arm_id),
            ('incubator', self.incubator_id)
        ]
        self._mark_critical_services(critical_services)

        try:
            # Move sample with robotic arm: Hamilton -> incubator (unified transport API)
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await asyncio.wait_for(
                self.robotic_arm.transport_plate(from_device="hamilton", to_device="incubator"),
                timeout=600,
            )

            # Put sample back to incubator slot
            await self.incubator.put_sample_from_transfer_station_to_slot(incubator_slot)
            await self.incubator.update_sample_location(incubator_slot, "incubator_slot")

            logger.info("Sample unloaded from Hamilton to incubator.")

        except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError) as e:
            error_msg = f"Failed to unload sample from Hamilton to slot {incubator_slot}: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e
        finally:
            self.in_critical_operation = False
            logger.info("CRITICAL OPERATION END: Robotic arm unload from Hamilton complete")
            self._unmark_critical_services(critical_services)

    async def _load_to_hamilton_api_wrapper(self, slot: int):
        """Internal wrapper for incubator -> hamilton transport."""
        try:
            if not self.incubator or not self.robotic_arm or not self.hamilton_executor:
                if not await self.setup_connections():
                    raise Exception("Transport services are not ready.")
            if not self.hamilton_executor:
                raise Exception("Hamilton executor service is not ready.")
            request = self._build_request(
                "manual-load-hamilton",
                incubator_slot=slot,
                extra_resources=self._hamilton_transport_resources(),
                metadata={"trigger": "api", "action": "load_to_hamilton"},
            )
            async with self.admission_controller.hold(request):
                await self._execute_load_to_hamilton_operation(slot, manage_transport_resources=False)
            return {"success": True, "message": f"Load from slot {slot} to Hamilton completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response("Load request rejected - orchestrator is busy.", busy_error)
        except HamiltonBusyError as busy_error:
            return self._hamilton_busy_response(str(busy_error))
        except Exception as e:
            logger.error(f"Load operation to Hamilton failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _unload_from_hamilton_api_wrapper(self, slot: int):
        """Internal wrapper for hamilton -> incubator transport."""
        try:
            if not self.incubator or not self.robotic_arm or not self.hamilton_executor:
                if not await self.setup_connections():
                    raise Exception("Transport services are not ready.")
            if not self.hamilton_executor:
                raise Exception("Hamilton executor service is not ready.")
            request = self._build_request(
                "manual-unload-hamilton",
                incubator_slot=slot,
                extra_resources=self._hamilton_transport_resources(),
                metadata={"trigger": "api", "action": "unload_from_hamilton"},
            )
            async with self.admission_controller.hold(request):
                await self._execute_unload_from_hamilton_operation(slot, manage_transport_resources=False)
            return {"success": True, "message": f"Unload from Hamilton to slot {slot} completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response("Unload request rejected - orchestrator is busy.", busy_error)
        except HamiltonBusyError as busy_error:
            return self._hamilton_busy_response(str(busy_error))
        except Exception as e:
            logger.error(f"Unload operation from Hamilton failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _execute_microscope_to_hamilton_operation(
        self,
        incubator_slot: int,
        microscope_id_str: str,
        *,
        manage_transport_resources: bool = True,
    ):
        """Execute the transport operation: move sample from microscope to Hamilton."""
        if manage_transport_resources:
            request = self._build_request(
                "transport-microscope-to-hamilton",
                microscope_id=microscope_id_str,
                incubator_slot=incubator_slot,
                extra_resources=self._hamilton_transport_resources(),
                metadata={
                    "phase": "microscope_to_hamilton",
                    "microscope_id": microscope_id_str,
                    "incubator_slot": incubator_slot,
                },
            )
            async with self.admission_controller.hold(request, wait=True):
                await self._execute_microscope_to_hamilton_operation(
                    incubator_slot,
                    microscope_id_str,
                    manage_transport_resources=False,
                )
                return

        target_microscope_service = self.microscope_services.get(microscope_id_str)
        if not target_microscope_service:
            raise Exception(f"Microscope service {microscope_id_str} is not connected.")

        # Verify sample is on the microscope
        try:
            actual_location = await self.incubator.get_sample_location(incubator_slot)
            logger.info(f"Actual sample location for slot {incubator_slot}: {actual_location}")

            if actual_location != microscope_id_str:
                self._raise_transport_precondition(
                    route=f"{microscope_id_str} -> hamilton",
                    incubator_slot=incubator_slot,
                    detail=f"expected sample at {microscope_id_str}, found {actual_location}",
                )
        except TransportPreconditionError:
            raise
        except Exception as e:
            logger.warning(f"Could not verify sample location from incubator for slot {incubator_slot}: {e}. Proceeding anyway.")

        if not self.sample_on_microscope_flags.get(microscope_id_str, False):
            self._raise_transport_precondition(
                route=f"{microscope_id_str} -> hamilton",
                incubator_slot=incubator_slot,
                detail=(
                    f"sample is not marked on {microscope_id_str}; "
                    "refusing a silent no-op transport"
                ),
            )

        await self._assert_hamilton_idle_for_transport()
        logger.info(f"Transporting sample from microscope {microscope_id_str} to Hamilton (slot {incubator_slot})...")

        # Mark critical operation
        self.in_critical_operation = True
        logger.info("CRITICAL OPERATION START: Robotic arm transporting sample from microscope to Hamilton")
        critical_services = [
            ('robotic_arm', self.robotic_arm_id),
            ('microscope', microscope_id_str),
            ('incubator', self.incubator_id)
        ]
        self._mark_critical_services(critical_services)

        try:
            # Home microscope stage
            await target_microscope_service.home_stage()

            # Move sample with robotic arm: microscope -> Hamilton (unified transport API)
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await asyncio.wait_for(
                self.robotic_arm.transport_plate(from_device=microscope_id_str, to_device="hamilton"),
                timeout=600,
            )

            # Return stage and update location
            await asyncio.gather(
                target_microscope_service.return_stage(),
                self.incubator.update_sample_location(incubator_slot, "hamilton")
            )

            self.sample_on_microscope_flags[microscope_id_str] = False
            logger.info("Sample transported from microscope to Hamilton.")

        except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError) as e:
            error_msg = f"Failed to transport sample from microscope {microscope_id_str} to Hamilton: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e
        finally:
            self.in_critical_operation = False
            logger.info("CRITICAL OPERATION END: Robotic arm microscope to Hamilton complete")
            self._unmark_critical_services(critical_services)

    async def _execute_hamilton_to_microscope_operation(
        self,
        incubator_slot: int,
        microscope_id_str: str,
        *,
        manage_transport_resources: bool = True,
    ):
        """Execute the transport operation: move sample from Hamilton to microscope."""
        if manage_transport_resources:
            request = self._build_request(
                "transport-hamilton-to-microscope",
                microscope_id=microscope_id_str,
                incubator_slot=incubator_slot,
                extra_resources=self._hamilton_transport_resources(),
                metadata={
                    "phase": "hamilton_to_microscope",
                    "microscope_id": microscope_id_str,
                    "incubator_slot": incubator_slot,
                },
            )
            async with self.admission_controller.hold(request, wait=True):
                await self._execute_hamilton_to_microscope_operation(
                    incubator_slot,
                    microscope_id_str,
                    manage_transport_resources=False,
                )
                return

        target_microscope_service = self.microscope_services.get(microscope_id_str)
        if not target_microscope_service:
            raise Exception(f"Microscope service {microscope_id_str} is not connected.")

        # Verify sample is at Hamilton
        try:
            actual_location = await self.incubator.get_sample_location(incubator_slot)
            logger.info(f"Incubator reports sample location for slot {incubator_slot}: {actual_location}")
            if actual_location != "hamilton":
                self._raise_transport_precondition(
                    route=f"hamilton -> {microscope_id_str}",
                    incubator_slot=incubator_slot,
                    detail=f"expected sample at hamilton, found {actual_location}",
                )
        except TransportPreconditionError:
            raise
        except Exception as e:
            logger.warning(f"Could not verify sample location from incubator for slot {incubator_slot}: {e}. Proceeding anyway.")

        if self.sample_on_microscope_flags.get(microscope_id_str, False):
            self._raise_transport_precondition(
                route=f"hamilton -> {microscope_id_str}",
                incubator_slot=incubator_slot,
                detail=(
                    f"sample is already marked on {microscope_id_str}; "
                    "refusing a silent no-op transport"
                ),
            )

        await self._assert_hamilton_idle_for_transport()
        logger.info(f"Transporting sample from Hamilton to microscope {microscope_id_str} (slot {incubator_slot})...")

        # Mark critical operation
        self.in_critical_operation = True
        logger.info("CRITICAL OPERATION START: Robotic arm transporting sample from Hamilton to microscope")
        critical_services = [
            ('robotic_arm', self.robotic_arm_id),
            ('microscope', microscope_id_str),
            ('incubator', self.incubator_id)
        ]
        self._mark_critical_services(critical_services)

        try:
            # Prepare the microscope stage before the transfer
            await asyncio.gather(
                target_microscope_service.home_stage(),
            )

            # Move sample with robotic arm: Hamilton -> microscope (unified transport API)
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await asyncio.wait_for(
                self.robotic_arm.transport_plate(from_device="hamilton", to_device=microscope_id_str),
                timeout=600,
            )

            # Return stage and update location
            await asyncio.gather(
                target_microscope_service.return_stage(),
                self.incubator.update_sample_location(incubator_slot, microscope_id_str)
            )

            self.sample_on_microscope_flags[microscope_id_str] = True
            logger.info("Sample transported from Hamilton to microscope.")

        except Exception as e:
            error_msg = f"Failed to transport sample from Hamilton to microscope {microscope_id_str}: {e}"
            logger.error(error_msg)
            self.sample_on_microscope_flags[microscope_id_str] = False
            raise Exception(error_msg)
        finally:
            self.in_critical_operation = False
            logger.info("CRITICAL OPERATION END: Robotic arm Hamilton to microscope complete")
            self._unmark_critical_services(critical_services)

    async def _microscope_to_hamilton_api_wrapper(self, slot: int, microscope_id: str):
        """Internal wrapper for microscope -> hamilton transport."""
        if microscope_id not in self.configured_microscopes_info:
            return {"success": False, "message": f"Microscope ID '{microscope_id}' not found in configured microscopes."}
        try:
            if not self.incubator or not self.robotic_arm or not self.hamilton_executor or microscope_id not in self.microscope_services:
                if not await self.setup_connections() or microscope_id not in self.microscope_services:
                    raise Exception(f"Transport services are not ready for microscope {microscope_id}.")
            if not self.hamilton_executor:
                raise Exception("Hamilton executor service is not ready.")
            request = self._build_request(
                "manual-microscope-to-hamilton",
                microscope_id=microscope_id,
                incubator_slot=slot,
                extra_resources=self._hamilton_transport_resources(),
                metadata={"trigger": "api", "action": "microscope_to_hamilton"},
            )
            async with self.admission_controller.hold(request):
                await self._execute_microscope_to_hamilton_operation(slot, microscope_id, manage_transport_resources=False)
            return {"success": True, "message": f"Transport from {microscope_id} to Hamilton completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response("Transport request rejected - orchestrator is busy.", busy_error)
        except HamiltonBusyError as busy_error:
            return self._hamilton_busy_response(str(busy_error))
        except Exception as e:
            logger.error(f"Transport from microscope to Hamilton failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _hamilton_to_microscope_api_wrapper(self, slot: int, microscope_id: str):
        """Internal wrapper for hamilton -> microscope transport."""
        if microscope_id not in self.configured_microscopes_info:
            return {"success": False, "message": f"Microscope ID '{microscope_id}' not found in configured microscopes."}
        try:
            if not self.incubator or not self.robotic_arm or not self.hamilton_executor or microscope_id not in self.microscope_services:
                if not await self.setup_connections() or microscope_id not in self.microscope_services:
                    raise Exception(f"Transport services are not ready for microscope {microscope_id}.")
            if not self.hamilton_executor:
                raise Exception("Hamilton executor service is not ready.")
            request = self._build_request(
                "manual-hamilton-to-microscope",
                microscope_id=microscope_id,
                incubator_slot=slot,
                extra_resources=self._hamilton_transport_resources(),
                metadata={"trigger": "api", "action": "hamilton_to_microscope"},
            )
            async with self.admission_controller.hold(request):
                await self._execute_hamilton_to_microscope_operation(slot, microscope_id, manage_transport_resources=False)
            return {"success": True, "message": f"Transport from Hamilton to {microscope_id} completed."}
        except ResourceBusyError as busy_error:
            return self._busy_response("Transport request rejected - orchestrator is busy.", busy_error)
        except HamiltonBusyError as busy_error:
            return self._hamilton_busy_response(str(busy_error))
        except Exception as e:
            logger.error(f"Transport from Hamilton to microscope failed: {e}", exc_info=True)
            return {"success": False, "message": str(e)}

    async def _execute_unload_operation(
        self,
        incubator_slot: int,
        microscope_id_str: str,
        *,
        manage_transport_resources: bool = True,
    ):
        """Execute the unload operation: move sample from microscope to incubator."""
        if manage_transport_resources:
            request = self._build_request(
                "transport-unload",
                extra_resources=self._transport_resources(),
                metadata={
                    "phase": "unload",
                    "microscope_id": microscope_id_str,
                    "incubator_slot": incubator_slot,
                },
            )
            async with self.admission_controller.hold(request, wait=True):
                await self._execute_unload_operation(
                    incubator_slot,
                    microscope_id_str,
                    manage_transport_resources=False,
                )
                return

        target_microscope_service = self.microscope_services.get(microscope_id_str)
        if not target_microscope_service:
            raise Exception(f"Microscope service {microscope_id_str} is not connected.")

        # Verify sample location from incubator service
        try:
            actual_location = await self.incubator.get_sample_location(incubator_slot)
            logger.info(f"Actual sample location for slot {incubator_slot}: {actual_location}")
            
            if actual_location == microscope_id_str:
                self.sample_on_microscope_flags[microscope_id_str] = True
                logger.info(f"Updated sample_on_microscope_flags[{microscope_id_str}] to True based on incubator location")
            elif actual_location == "incubator_slot":
                self.sample_on_microscope_flags[microscope_id_str] = False
                logger.info(f"Sample already in incubator slot {incubator_slot}, no unload needed")
                return
        except Exception as e:
            logger.warning(f"Could not verify sample location from incubator for slot {incubator_slot}: {e}. Proceeding with unload based on flag.")

        if not self.sample_on_microscope_flags.get(microscope_id_str, False):
            logger.info(f"Sample plate not on microscope {microscope_id_str} according to flags")
            return 
            
        logger.info(f"Unloading sample to incubator slot {incubator_slot} from microscope {microscope_id_str}...")

        # Mark critical operation
        self.in_critical_operation = True
        logger.info("CRITICAL OPERATION START: Robotic arm unloading sample")
        critical_services = [
            ('robotic_arm', self.robotic_arm_id),
            ('microscope', microscope_id_str),
            ('incubator', self.incubator_id)
        ]
        self._mark_critical_services(critical_services)

        try:
            # Home microscope stage
            await target_microscope_service.home_stage()
            
            # Move sample with robotic arm (unified transport API)
            await self.incubator.update_sample_location(incubator_slot, "robotic_arm")
            await asyncio.wait_for(
                self.robotic_arm.transport_plate(from_device=microscope_id_str, to_device="incubator"),
                timeout=600,
            )
            
            # Put sample back and return stage in parallel
            await asyncio.gather(
                self.incubator.put_sample_from_transfer_station_to_slot(incubator_slot),
                target_microscope_service.return_stage()
            )
            await self.incubator.update_sample_location(incubator_slot, "incubator_slot")
            
            logger.info(f"Sample unloaded from microscope {microscope_id_str}.")
            self.sample_on_microscope_flags[microscope_id_str] = False
            
        except Exception as e:
            error_msg = f"Failed to unload sample to slot {incubator_slot} from microscope {microscope_id_str}: {e}"
            logger.error(error_msg)
            raise Exception(error_msg)
        finally:
            self.in_critical_operation = False
            logger.info("CRITICAL OPERATION END: Robotic arm unload complete")
            self._unmark_critical_services(critical_services)
