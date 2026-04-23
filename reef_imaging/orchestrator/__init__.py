"""Orchestrator package — assembles mixins into OrchestrationSystem."""
from .core import (
    OrchestrationSystemBase,
    HamiltonBusyError,
    setup_logging,
    logger,
    CONFIG_FILE_PATH,
    CONFIG_FILE_PATH_TMP,
    CONFIG_READ_INTERVAL,
    ORCHESTRATOR_LOOP_SLEEP,
)
from .health import HealthMixin
from .transport import TransportMixin
from .tasks import TaskMixin
from .api import APIMixin


class OrchestrationSystem(TransportMixin, APIMixin, HealthMixin, TaskMixin, OrchestrationSystemBase):
    """Full orchestrator combining transport, API, health, and task scheduling."""
    pass


async def main():
    global logger
    logger = setup_logging(log_file="orchestrator.log")

    orchestrator = OrchestrationSystem()
    try:
        await orchestrator._register_self_as_hypha_service()
        await orchestrator.run_time_lapse()
    except KeyboardInterrupt:
        logger.info("Orchestrator shutting down due to KeyboardInterrupt...")
    finally:
        logger.info("Performing cleanup... disconnecting services.")
        if orchestrator:
            await orchestrator.cancel_running_cycles()
            if orchestrator.orchestrator_hypha_server_connection:
                try:
                    await orchestrator.orchestrator_hypha_server_connection.disconnect()
                    logger.info("Disconnected from Hypha server for orchestrator's own service.")
                except (ConnectionError, OSError) as e:
                    logger.error(f"Error disconnecting orchestrator's Hypha service: {e}")
            await orchestrator.disconnect_services()
        logger.info("Cleanup complete. Orchestrator shutdown.")


__all__ = [
    "OrchestrationSystem",
    "HamiltonBusyError",
    "setup_logging",
    "logger",
    "CONFIG_FILE_PATH",
    "CONFIG_FILE_PATH_TMP",
    "CONFIG_READ_INTERVAL",
    "ORCHESTRATOR_LOOP_SLEEP",
    "main",
]
