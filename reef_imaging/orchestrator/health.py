"""Health check and connection management mixin."""
import asyncio
import sys
from hypha_rpc import connect_to_server
from .core import logger


class HealthMixin:
    _DEAD_CONNECTION_KEYWORDS = ("1011", "ping timeout", "no close frame", "keepalive", "connection closed", "websocket")

    async def check_service_health(self, service, service_type, service_identifier=None):
        """Check if the service is healthy with smart failure handling:
        - During critical operations (robotic arm moving, scanning): Retry 10 times then EXIT program
        - When idle and server connection died: Attempt full reconnection (no exit)
        - When idle and microscope offline: Drop microscope from active services and stop health check
        - When idle and incubator/arm unreachable: Log and keep retrying (no exit)
        Note: We keep the server connection stable and only refresh the service reference when idle."""
        log_service_name_part = service_identifier if service_identifier else (service.id if hasattr(service, "id") else service_type)
        service_name = f"{service_type} ({log_service_name_part})"

        logger.info(f"Health check loop started for {service_name}")
        consecutive_failures = 0
        max_failures = 10

        while True:
            try:
                # Set a timeout for the ping operation
                ping_result = await asyncio.wait_for(service.ping(), timeout=5)

                if ping_result != "pong":
                    logger.error(f"{service_name} service ping check failed: {ping_result}")
                    raise Exception("Service not healthy")

                # Service is healthy - reset failure counter
                if consecutive_failures > 0:
                    logger.info(f"{service_name} service recovered after {consecutive_failures} failures.")
                consecutive_failures = 0
                logger.debug(f"{service_name} service health check passed.")

            except Exception as e:
                if isinstance(e, (SystemExit, KeyboardInterrupt, asyncio.CancelledError, GeneratorExit)):
                    raise
                consecutive_failures += 1

                if isinstance(e, asyncio.TimeoutError):
                    logger.warning(f"{service_name} service ping timed out (failure {consecutive_failures}/{max_failures}).")
                else:
                    logger.warning(f"{service_name} service health check failed (failure {consecutive_failures}/{max_failures}): {e}")

                # Check if this specific service is in a critical operation
                is_critical_for_service = False
                if service_identifier is not None:
                    is_critical_for_service = (service_type, service_identifier) in self.critical_services
                else:
                    is_critical_for_service = (service_type, None) in self.critical_services

                if is_critical_for_service:
                    logger.warning(f"{service_name} health check failed during CRITICAL OPERATION (robotic arm moving or scanning).")

                    if consecutive_failures >= max_failures:
                        error_msg = f"{service_name} failed {max_failures} times during critical operation. Exiting program for safety."
                        logger.critical(error_msg)
                        logger.critical("Orchestrator will exit. Monitoring system will alert and restart.")
                        sys.exit(1)

                    retry_delay = 10 * consecutive_failures  # 10s, 20s, 30s...
                    logger.warning(f"Will retry {service_name} health check in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    continue

                else:
                    # Not in critical operation - safe to attempt refresh
                    logger.info(f"{service_name} health check failed while IDLE. Attempting service proxy refresh.")

                    try:
                        await self._refresh_service_proxy(service_type, service_identifier)
                        logger.info(f"{service_name} service proxy refreshed successfully.")

                        # Update the local service variable to the refreshed proxy
                        if service_type == 'incubator':
                            service = self.incubator
                        elif service_type == 'microscope' and service_identifier:
                            service = self.microscope_services.get(service_identifier)
                        elif service_type == 'robotic_arm':
                            service = self.robotic_arm
                        elif service_type == 'hamilton':
                            service = self.hamilton_executor

                        if service is None:
                            logger.error(f"Failed to get refreshed {service_name} service reference. Will retry.")
                            await asyncio.sleep(30)
                            continue

                        consecutive_failures = 0
                        await asyncio.sleep(30)
                        continue

                    except ConnectionError as conn_err:
                        # The local server connection itself is dead — attempt full reconnect
                        logger.warning(f"{service_name}: server connection dead ({conn_err}). Attempting full reconnect...")
                        try:
                            await self._reset_and_reconnect_local_server()
                            logger.info("Full reconnect succeeded. Resetting failure counter.")
                            # Refresh our local service reference after reconnect
                            if service_type == 'incubator':
                                service = self.incubator
                            elif service_type == 'microscope' and service_identifier:
                                service = self.microscope_services.get(service_identifier)
                            elif service_type == 'robotic_arm':
                                service = self.robotic_arm
                            elif service_type == 'hamilton':
                                service = self.hamilton_executor
                            consecutive_failures = 0
                        except (ConnectionError, OSError, asyncio.TimeoutError) as reconnect_err:
                            logger.error(f"Full reconnect failed: {reconnect_err}. Will retry in 60 seconds.")
                            await asyncio.sleep(60)
                        continue

                    except (ConnectionError, OSError, asyncio.TimeoutError) as refresh_error:
                        logger.error(f"Failed to refresh {service_name} service proxy: {refresh_error}")

                        if consecutive_failures >= max_failures:
                            if service_type == 'microscope':
                                # An offline microscope should not crash the orchestrator.
                                # Drop it from active services — it will be reconnected when a task needs it.
                                logger.warning(
                                    f"{service_name} unreachable after {max_failures} attempts. "
                                    f"Removing from active services. Will reconnect when a task requires it."
                                )
                                if service_identifier and service_identifier in self.microscope_services:
                                    del self.microscope_services[service_identifier]
                                if service_identifier and service_identifier in self.sample_on_microscope_flags:
                                    self.sample_on_microscope_flags[service_identifier] = False
                                key = (service_type, service_identifier) if service_identifier else service_type
                                self.health_check_tasks.pop(key, None)
                                return  # Exit health check loop for this microscope
                            else:
                                # For incubator/robotic arm: log critical but keep retrying, don't exit.
                                logger.critical(
                                    f"{service_name} unreachable after {max_failures} attempts while idle. "
                                    f"This is unexpected — check hardware. Resetting counter and continuing to retry."
                                )
                                consecutive_failures = 0

                        logger.info(f"Will retry {service_name} refresh in 60 seconds...")
                        await asyncio.sleep(60)
                        continue

            await asyncio.sleep(30)  # Check every 30 seconds when healthy

    async def _refresh_service_proxy(self, service_type, service_id):
        """Refresh a service proxy from the existing stable server connection.
        This does NOT create a new server connection, just gets a fresh service reference.
        Raises ConnectionError if the underlying server connection appears dead."""
        if not self.local_server_connection:
            raise ConnectionError("Local server connection not available")

        try:
            if service_type == 'incubator':
                logger.info(f"Refreshing incubator service proxy ({self.incubator_id})...")
                self.incubator = await self.local_server_connection.get_service(self.incubator_id)
                logger.info("Incubator service proxy refreshed.")

            elif service_type == 'microscope':
                if not service_id:
                    raise Exception("Microscope service_id required for refresh")
                logger.info(f"Refreshing microscope service proxy ({service_id})...")
                microscope_service = await self.local_server_connection.get_service(service_id)
                self.microscope_services[service_id] = microscope_service
                logger.info(f"Microscope service proxy {service_id} refreshed.")

            elif service_type == 'robotic_arm':
                logger.info(f"Refreshing robotic arm service proxy ({self.robotic_arm_id})...")
                self.robotic_arm = await self.local_server_connection.get_service(self.robotic_arm_id)
                logger.info("Robotic arm service proxy refreshed.")
            elif service_type == 'hamilton':
                logger.info(f"Refreshing Hamilton executor service proxy ({self.hamilton_executor_id})...")
                self.hamilton_executor = await self.local_server_connection.get_service(self.hamilton_executor_id)
                logger.info("Hamilton executor service proxy refreshed.")
            else:
                raise Exception(f"Unknown service type: {service_type}")
        except ConnectionError:
            raise
        except Exception as e:
            err_lower = str(e).lower()
            if any(kw in err_lower for kw in self._DEAD_CONNECTION_KEYWORDS):
                raise ConnectionError(f"Server connection dead: {e}") from e
            raise

    async def _reset_and_reconnect_local_server(self):
        """Null out the dead server connection and all service proxies, then reconnect."""
        logger.warning("Resetting dead local server connection and all service proxies for full reconnect...")
        self.local_server_connection = None
        self.incubator = None
        self.robotic_arm = None
        self.hamilton_executor = None
        self.microscope_services.clear()
        return await self.setup_connections()

    async def disconnect_single_service(self, service_type, service_id_to_disconnect=None):
        """Clear a specific service reference and stop its health check."""
        actual_service_id = service_id_to_disconnect
        if service_type == 'incubator':
            actual_service_id = self.incubator_id
        elif service_type == 'robotic_arm':
            actual_service_id = self.robotic_arm_id
        elif service_type == 'hamilton':
            actual_service_id = self.hamilton_executor_id
        
        if actual_service_id:
             await self._stop_health_check(service_type, actual_service_id)

        try:
            if service_type == 'incubator' and self.incubator:
                logger.info(f"Clearing incubator service reference ({self.incubator_id})...")
                self.incubator = None
            elif service_type == 'microscope':
                if service_id_to_disconnect and service_id_to_disconnect in self.microscope_services:
                    logger.info(f"Clearing microscope service reference ({service_id_to_disconnect})...")
                    self.microscope_services.pop(service_id_to_disconnect)
                    if service_id_to_disconnect in self.sample_on_microscope_flags:
                        self.sample_on_microscope_flags[service_id_to_disconnect] = False 
            elif service_type == 'robotic_arm' and self.robotic_arm:
                logger.info(f"Clearing robotic arm service reference ({self.robotic_arm_id})...")
                self.robotic_arm = None
            elif service_type == 'hamilton' and self.hamilton_executor:
                logger.info(f"Clearing Hamilton executor service reference ({self.hamilton_executor_id})...")
                self.hamilton_executor = None
                
        except (AttributeError, TypeError) as e:
            logger.error(f"Error clearing {service_type} service reference ({service_id_to_disconnect if service_id_to_disconnect else ''}): {e}")


    async def setup_connections(self): 
        """Set up ONE stable connection to local Hypha server and get all service proxies.
        The server connection is kept alive throughout, only service proxies are refreshed on failures."""
        try:
            connection = await self._ensure_local_server_connection()
            if not connection:
                return False

            logger.info("Reusing existing stable server connection.")

            # Get service proxies from the stable connection
            if not self.incubator:
                self.incubator = await connection.get_service(self.incubator_id)
                logger.info(f"Incubator ({self.incubator_id}) service proxy obtained.")
                await self._start_health_check('incubator', self.incubator, self.incubator_id)
                
            if not self.robotic_arm:
                self.robotic_arm = await connection.get_service(self.robotic_arm_id)
                logger.info(f"Robotic arm ({self.robotic_arm_id}) service proxy obtained.")
                await self._start_health_check('robotic_arm', self.robotic_arm, self.robotic_arm_id)

            if not self.hamilton_executor:
                try:
                    self.hamilton_executor = await connection.get_service(self.hamilton_executor_id)
                    logger.info(f"Hamilton executor ({self.hamilton_executor_id}) service proxy obtained.")
                    await self._start_health_check('hamilton', self.hamilton_executor, self.hamilton_executor_id)
                except (ConnectionError, OSError, asyncio.TimeoutError) as hamilton_error:
                    logger.warning(
                        f"Hamilton executor ({self.hamilton_executor_id}) is not currently available: {hamilton_error}"
                    )

        except (ConnectionError, OSError, asyncio.TimeoutError) as e:
            logger.error(f"Failed to setup local services (incubator/robotic arm): {e}")
            return False 

        # Get microscope service proxies from the stable connection
        connected_microscope_count = 0
        if not self.configured_microscopes_info:
            logger.warning("No microscopes defined in the configuration.")
        else:
            logger.info(f"Found {len(self.configured_microscopes_info)} configured microscopes: {list(self.configured_microscopes_info.keys())}")
        
        for mic_id in self.configured_microscopes_info.keys():
            if mic_id not in self.microscope_services: 
                logger.info(f"Getting service proxy for microscope: {mic_id}...")
                try:
                    microscope_service_instance = await connection.get_service(mic_id)
                    self.microscope_services[mic_id] = microscope_service_instance
                    if mic_id not in self.sample_on_microscope_flags:
                        self.sample_on_microscope_flags[mic_id] = False
                    logger.info(f"Microscope {mic_id} service proxy obtained.")
                    
                    await self._start_health_check('microscope', microscope_service_instance, mic_id)
                    connected_microscope_count += 1
                except (ConnectionError, OSError, asyncio.TimeoutError) as e:
                    logger.error(f"Failed to get service proxy for microscope {mic_id}: {e}")
                    if mic_id in self.microscope_services:
                        del self.microscope_services[mic_id]
            else:
                logger.info(f"Microscope {mic_id} already has service proxy.")
                connected_microscope_count += 1
        
        # Clean up microscopes no longer in config
        connected_ids = list(self.microscope_services.keys())
        for mid in connected_ids:
            if mid not in self.configured_microscopes_info:
                logger.info(f"Microscope {mid} no longer in configuration. Removing service proxy.")
                await self.disconnect_single_service('microscope', mid)

        logger.info(f'Connection setup completed. {connected_microscope_count}/{len(self.configured_microscopes_info)} microscopes ready.')
        
        return bool(self.incubator and self.robotic_arm)

    async def check_cloud_connection_health(self):
        """Monitor the cloud Hypha connection and reconnect if dropped."""
        if not self.orchestrator_hypha_server_connection:
            logger.info("No cloud connection to monitor.")
            return

        logger.info("Starting cloud connection health check...")
        consecutive_failures = 0
        max_failures = 5

        while True:
            try:
                # Quick ping via the connection's internal websocket or a lightweight op
                # Hypha connections don't have a direct ping, but we can check the server info
                await asyncio.wait_for(
                    self.orchestrator_hypha_server_connection.get_server_info(), timeout=10
                )
                if consecutive_failures > 0:
                    logger.info(f"Cloud connection recovered after {consecutive_failures} failures.")
                consecutive_failures = 0
            except Exception as e:
                if isinstance(e, (SystemExit, KeyboardInterrupt, asyncio.CancelledError, GeneratorExit)):
                    raise
                consecutive_failures += 1
                logger.warning(
                    f"Cloud connection health check failed ({consecutive_failures}/{max_failures}): {e}"
                )
                if consecutive_failures >= max_failures:
                    logger.error("Cloud connection lost. Attempting to reconnect...")
                    try:
                        if self.orchestrator_hypha_server_connection:
                            try:
                                await self.orchestrator_hypha_server_connection.disconnect()
                            except Exception:
                                pass
                        server_config = {
                            "server_url": self.orchestrator_hypha_server_url,
                            "ping_interval": 30,
                            "workspace": self.workspace,
                            "token": self.token_for_orchestrator_registration,
                        }
                        self.orchestrator_hypha_server_connection = await connect_to_server(server_config)
                        await self.orchestrator_hypha_server_connection.register_service(
                            self._build_service_api(),
                            overwrite=True,
                        )
                        logger.info("Cloud connection re-established and service re-registered.")
                        consecutive_failures = 0
                    except Exception as reconnect_err:
                        logger.error(f"Cloud reconnection failed: {reconnect_err}. Will retry in 60s.")
                        await asyncio.sleep(60)
                        continue

            await asyncio.sleep(30)

    async def disconnect_services(self):
        """Stop all health checks, clear service references, and disconnect from the stable server connection."""
        logger.info("Disconnecting all services...")
        
        # Clear service references and stop health checks
        if self.incubator:
            await self.disconnect_single_service('incubator', self.incubator_id) 
        
        microscope_ids_to_disconnect = list(self.microscope_services.keys())
        for mic_id in microscope_ids_to_disconnect:
            await self.disconnect_single_service('microscope', mic_id)
        
        if self.robotic_arm:
            await self.disconnect_single_service('robotic_arm', self.robotic_arm_id)

        if self.hamilton_executor:
            await self.disconnect_single_service('hamilton', self.hamilton_executor_id)
        
        # Disconnect the stable server connection
        if self.local_server_connection:
            try:
                logger.info("Disconnecting stable server connection...")
                await self.local_server_connection.disconnect()
                self.local_server_connection = None
                logger.info("Stable server connection disconnected.")
            except (ConnectionError, OSError, asyncio.TimeoutError) as e:
                logger.error(f"Error disconnecting stable server connection: {e}")
                
        logger.info("Disconnect process completed.")
