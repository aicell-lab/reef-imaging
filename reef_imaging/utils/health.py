"""Shared health check utilities for reef-imaging services."""

import asyncio
import logging
from enum import Enum
from typing import Optional, Callable, Any, Dict
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation, requests pass through
    OPEN = "open"          # Failure threshold reached, requests blocked
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class HealthMonitorConfig:
    """Configuration for HealthMonitor."""
    check_interval: float = 30.0          # Seconds between health checks
    max_failures: int = 10                # Max consecutive failures before circuit opens
    reset_timeout: float = 60.0           # Seconds to wait before half-open
    backoff_base: float = 10.0            # Base for exponential backoff
    backoff_max: float = 300.0            # Max backoff seconds
    ping_timeout: float = 5.0             # Timeout for ping operations
    critical_mode: bool = False           # If True, exit on max failures


@dataclass
class HealthStats:
    """Health check statistics."""
    consecutive_failures: int = 0
    total_failures: int = 0
    total_successes: int = 0
    last_error: Optional[str] = None
    last_success_time: Optional[float] = None


class HealthMonitor:
    """
    Generic health monitor with circuit breaker pattern and exponential backoff.
    
    This class provides a reusable health check mechanism that can be used
    across all services (incubator, robotic arm, orchestrator, etc.).
    
    Features:
    - Circuit breaker pattern (CLOSED/OPEN/HALF_OPEN states)
    - Exponential backoff for reconnection attempts
    - Configurable check intervals and failure thresholds
    - Callback hooks for success/failure events
    - Optional critical mode (exit on persistent failures)
    
    Example usage:
        monitor = HealthMonitor(
            service_name="incubator",
            ping_func=lambda: service.ping(),
            reconnect_func=setup_service,
            config=HealthMonitorConfig(check_interval=30.0, max_failures=5)
        )
        
        # Start monitoring
        task = asyncio.create_task(monitor.start())
        
        # Stop monitoring
        await monitor.stop()
    """
    
    def __init__(
        self,
        service_name: str,
        ping_func: Callable[[], Any],
        reconnect_func: Optional[Callable[[], Any]] = None,
        config: Optional[HealthMonitorConfig] = None,
        on_success: Optional[Callable[[], None]] = None,
        on_failure: Optional[Callable[[str], None]] = None,
        on_circuit_open: Optional[Callable[[], None]] = None,
        on_critical_failure: Optional[Callable[[], None]] = None,
    ):
        """
        Initialize the health monitor.
        
        Args:
            service_name: Name of the service being monitored
            ping_func: Async function to call for health checks (should return "pong" on success)
            reconnect_func: Optional async function to call when reconnection is needed
            config: HealthMonitorConfig instance (uses defaults if None)
            on_success: Callback when health check succeeds after failure
            on_failure: Callback when health check fails
            on_circuit_open: Callback when circuit breaker opens
            on_critical_failure: Callback when max failures reached in critical mode
        """
        self.service_name = service_name
        self.ping_func = ping_func
        self.reconnect_func = reconnect_func
        self.config = config or HealthMonitorConfig()
        self.on_success = on_success
        self.on_failure = on_failure
        self.on_circuit_open = on_circuit_open
        self.on_critical_failure = on_critical_failure
        
        self._circuit_state = CircuitState.CLOSED
        self._stats = HealthStats()
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._half_open_event = asyncio.Event()
        
    @property
    def circuit_state(self) -> CircuitState:
        """Get current circuit breaker state."""
        return self._circuit_state
    
    @property
    def is_healthy(self) -> bool:
        """Check if service is currently considered healthy."""
        return self._circuit_state == CircuitState.CLOSED and self._stats.consecutive_failures == 0
    
    @property
    def stats(self) -> HealthStats:
        """Get health check statistics."""
        return self._stats
    
    def _get_backoff_delay(self) -> float:
        """Calculate exponential backoff delay."""
        import math
        delay = self.config.backoff_base * (2 ** self._stats.consecutive_failures)
        return min(delay, self.config.backoff_max)
    
    async def _do_ping(self) -> bool:
        """Execute ping with timeout. Returns True if successful."""
        try:
            result = await asyncio.wait_for(
                self.ping_func(),
                timeout=self.config.ping_timeout
            )
            return result == "pong"
        except asyncio.TimeoutError:
            logger.warning(f"{self.service_name} health check timed out after {self.config.ping_timeout}s")
            return False
        except Exception as e:
            logger.warning(f"{self.service_name} health check failed: {e}")
            return False
    
    async def _do_reconnect(self) -> bool:
        """Attempt reconnection. Returns True if successful."""
        if self.reconnect_func is None:
            return False
        
        try:
            logger.info(f"{self.service_name}: Attempting reconnection...")
            await self.reconnect_func()
            logger.info(f"{self.service_name}: Reconnection successful")
            return True
        except Exception as e:
            logger.error(f"{self.service_name}: Reconnection failed: {e}")
            return False
    
    async def _handle_failure(self, error_msg: str):
        """Handle a health check failure."""
        self._stats.consecutive_failures += 1
        self._stats.total_failures += 1
        self._stats.last_error = error_msg
        
        if self.on_failure:
            try:
                self.on_failure(error_msg)
            except Exception as e:
                logger.error(f"Error in on_failure callback: {e}")
        
        logger.warning(
            f"{self.service_name} health check failed "
            f"({self._stats.consecutive_failures}/{self.config.max_failures}): {error_msg}"
        )
        
        # Check if circuit should open
        if self._stats.consecutive_failures >= self.config.max_failures:
            await self._open_circuit()
    
    async def _handle_success(self):
        """Handle a health check success."""
        had_failures = self._stats.consecutive_failures > 0
        
        self._stats.consecutive_failures = 0
        self._stats.total_successes += 1
        self._stats.last_success_time = asyncio.get_event_loop().time()
        
        # If circuit was half-open, close it
        if self._circuit_state == CircuitState.HALF_OPEN:
            self._circuit_state = CircuitState.CLOSED
            logger.info(f"{self.service_name}: Circuit breaker closed, service recovered")
        
        if had_failures and self.on_success:
            try:
                self.on_success()
            except Exception as e:
                logger.error(f"Error in on_success callback: {e}")
    
    async def _open_circuit(self):
        """Open the circuit breaker."""
        self._circuit_state = CircuitState.OPEN
        logger.error(f"{self.service_name}: Circuit breaker OPENED after {self.config.max_failures} failures")
        
        if self.on_circuit_open:
            try:
                self.on_circuit_open()
            except Exception as e:
                logger.error(f"Error in on_circuit_open callback: {e}")
        
        if self.config.critical_mode:
            logger.critical(f"{self.service_name}: CRITICAL - Max failures reached in critical mode")
            if self.on_critical_failure:
                try:
                    self.on_critical_failure()
                except Exception as e:
                    logger.error(f"Error in on_critical_failure callback: {e}")
            # Signal to stop the monitor
            self._stop_event.set()
        else:
            # Schedule transition to half-open
            self._half_open_event.clear()
            asyncio.create_task(self._schedule_half_open())
    
    async def _schedule_half_open(self):
        """Schedule transition from OPEN to HALF_OPEN."""
        await asyncio.sleep(self.config.reset_timeout)
        if self._circuit_state == CircuitState.OPEN:
            self._circuit_state = CircuitState.HALF_OPEN
            logger.info(f"{self.service_name}: Circuit breaker HALF_OPEN, testing recovery...")
            self._half_open_event.set()
    
    async def _check_once(self) -> bool:
        """Perform a single health check. Returns True if healthy."""
        # If circuit is open, wait for half-open
        if self._circuit_state == CircuitState.OPEN:
            logger.debug(f"{self.service_name}: Circuit open, waiting for half-open...")
            try:
                await asyncio.wait_for(
                    self._half_open_event.wait(),
                    timeout=self.config.reset_timeout
                )
            except asyncio.TimeoutError:
                pass
        
        # Perform the health check
        is_healthy = await self._do_ping()
        
        if is_healthy:
            await self._handle_success()
            return True
        else:
            await self._handle_failure("Health check failed")
            
            # Try reconnection if not in critical mode
            if not self.config.critical_mode and self.reconnect_func:
                reconnected = await self._do_reconnect()
                if reconnected:
                    # Verify connection with another ping
                    if await self._do_ping():
                        await self._handle_success()
                        return True
                    else:
                        await self._handle_failure("Reconnected but ping still failing")
            
            return False
    
    async def start(self):
        """
        Start the health check loop.
        
        This method runs indefinitely until stop() is called or
        critical mode triggers a failure.
        """
        logger.info(f"Health monitor started for {self.service_name}")
        
        while not self._stop_event.is_set():
            try:
                await self._check_once()
            except Exception as e:
                logger.error(f"{self.service_name}: Unexpected error in health check: {e}")
            
            # Wait before next check (or stop event)
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.config.check_interval
                )
            except asyncio.TimeoutError:
                pass
        
        logger.info(f"Health monitor stopped for {self.service_name}")
    
    async def stop(self):
        """Stop the health check loop."""
        logger.info(f"Stopping health monitor for {self.service_name}...")
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    def start_in_background(self) -> asyncio.Task:
        """
        Start the health monitor as a background task.
        
        Returns:
            The asyncio Task running the health monitor
        """
        self._task = asyncio.create_task(self.start())
        return self._task


class ServiceHealthManager:
    """
    Manager for multiple service health monitors.
    
    This is useful for the orchestrator which needs to monitor
    multiple services (incubator, robotic arm, microscopes).
    
    Example:
        manager = ServiceHealthManager()
        
        # Register services
        manager.register("incubator", incubator_ping, incubator_reconnect)
        manager.register("robotic_arm", arm_ping, arm_reconnect)
        
        # Start all monitors
        await manager.start_all()
        
        # Stop all monitors
        await manager.stop_all()
    """
    
    def __init__(self):
        self._monitors: Dict[str, HealthMonitor] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
    
    def register(
        self,
        service_id: str,
        ping_func: Callable[[], Any],
        reconnect_func: Optional[Callable[[], Any]] = None,
        config: Optional[HealthMonitorConfig] = None,
        **kwargs
    ) -> HealthMonitor:
        """
        Register a service for health monitoring.
        
        Args:
            service_id: Unique identifier for the service
            ping_func: Async function for health checks
            reconnect_func: Async function for reconnection
            config: Health monitor configuration
            **kwargs: Additional arguments passed to HealthMonitor
        
        Returns:
            The created HealthMonitor instance
        """
        monitor = HealthMonitor(
            service_name=service_id,
            ping_func=ping_func,
            reconnect_func=reconnect_func,
            config=config,
            **kwargs
        )
        self._monitors[service_id] = monitor
        return monitor
    
    def unregister(self, service_id: str):
        """Unregister a service from health monitoring."""
        if service_id in self._monitors:
            del self._monitors[service_id]
        if service_id in self._tasks:
            task = self._tasks.pop(service_id)
            if not task.done():
                task.cancel()
    
    async def start_all(self):
        """Start health monitoring for all registered services."""
        for service_id, monitor in self._monitors.items():
            if service_id not in self._tasks or self._tasks[service_id].done():
                self._tasks[service_id] = monitor.start_in_background()
                logger.info(f"Started health monitor for {service_id}")
    
    async def stop_all(self):
        """Stop health monitoring for all registered services."""
        for service_id, monitor in list(self._monitors.items()):
            await monitor.stop()
            if service_id in self._tasks:
                del self._tasks[service_id]
        logger.info("Stopped all health monitors")
    
    def get_monitor(self, service_id: str) -> Optional[HealthMonitor]:
        """Get the health monitor for a specific service."""
        return self._monitors.get(service_id)
    
    def is_healthy(self, service_id: str) -> bool:
        """Check if a specific service is healthy."""
        monitor = self._monitors.get(service_id)
        return monitor.is_healthy if monitor else False
    
    def get_all_stats(self) -> Dict[str, HealthStats]:
        """Get health statistics for all registered services."""
        return {sid: monitor.stats for sid, monitor in self._monitors.items()}
