import os
import logging
import logging.handlers
import asyncio
import argparse
import signal
import traceback
import dotenv
from hypha_rpc import connect_to_server

dotenv.load_dotenv()
ENV_FILE = dotenv.find_dotenv()
if ENV_FILE:
    dotenv.load_dotenv(ENV_FILE)


def setup_logging(log_file="mirror_hamilton_service.log", max_bytes=100000, backup_count=3):
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


logger = setup_logging()


class MirrorHamiltonService:
    def __init__(self):
        self.cloud_server_url = "https://hypha.aicell.io"
        self.cloud_workspace = "reef-imaging"
        self.cloud_token = os.environ.get("REEF_WORKSPACE_TOKEN")
        self.cloud_service_id = "hamilton-control-service"
        self.cloud_server = None
        self.cloud_service = None

        self.local_server_url = "http://reef.dyn.scilifelab.se:9527"
        self.local_token = os.environ.get("REEF_LOCAL_TOKEN")
        self.local_service_id = "hamilton-control-service"
        self.local_server = None
        self.local_service = None

        self.setup_task = None
        self.mirrored_methods = {}
        self.shutdown_event = asyncio.Event()
        self._background_tasks = []

    async def connect_to_local_service(self):
        try:
            logger.info(f"Connecting to local service at {self.local_server_url}")
            self.local_server = await connect_to_server({
                "server_url": self.local_server_url,
                "token": self.local_token,
                "ping_interval": 30,
            })
            self.local_service = await self.local_server.get_service(self.local_service_id)
            logger.info(f"Successfully connected to local service {self.local_service_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to local Hamilton service: {e}")
            self.local_service = None
            self.local_server = None
            return False

    async def cleanup_cloud_service(self):
        try:
            if self.cloud_service:
                logger.info(f"Unregistering cloud service {self.cloud_service_id}")
                try:
                    await self.cloud_server.unregister_service(self.cloud_service_id)
                    logger.info(f"Successfully unregistered cloud service {self.cloud_service_id}")
                except Exception as e:
                    logger.warning(f"Failed to unregister cloud service {self.cloud_service_id}: {e}")
                self.cloud_service = None

            self.mirrored_methods.clear()
            logger.info("Cleared mirrored methods")
        except Exception as e:
            logger.error(f"Error during cloud service cleanup: {e}")

    def _create_mirror_method(self, method_name, local_method):
        async def mirror_method(*args, **kwargs):
            try:
                if self.local_service is None:
                    logger.warning(f"Local Hamilton service is None when calling {method_name}, attempting reconnect")
                    success = await self.connect_to_local_service()
                    if not success or self.local_service is None:
                        raise Exception("Failed to connect to local Hamilton service")

                return await local_method(*args, **kwargs)
            except Exception as e:
                logger.error(f"Failed to call {method_name}: {e}")
                raise

        if hasattr(local_method, "__schema__"):
            original_schema = getattr(local_method, "__schema__")
            if original_schema is not None:
                mirror_method.__schema__ = original_schema
                mirror_method.__doc__ = original_schema.get("description", f"Mirror of {method_name}")

        return mirror_method

    def _get_mirrored_methods(self):
        if self.local_service is None:
            logger.warning("Cannot create mirror methods: local Hamilton service is None")
            return {}

        mirrored_methods = {}
        excluded_methods = {
            "name", "id", "config", "type",
            "__class__", "__doc__", "__dict__", "__module__",
        }

        for attr_name in dir(self.local_service):
            if attr_name.startswith("_") or attr_name in excluded_methods:
                continue

            attr = getattr(self.local_service, attr_name)
            if callable(attr):
                logger.info(f"Creating Hamilton mirror method for: {attr_name}")
                mirrored_methods[attr_name] = self._create_mirror_method(attr_name, attr)

        logger.info(
            f"Hamilton mirror: created {len(mirrored_methods)} methods: {list(mirrored_methods.keys())}"
        )
        return mirrored_methods

    async def check_service_health(self):
        logger.info("Starting Hamilton mirror health check task")
        while True:
            try:
                if self.cloud_service_id and self.cloud_server:
                    service = await self.cloud_server.get_service(self.cloud_service_id)
                    ping_result = await asyncio.wait_for(service.ping(), timeout=10)
                    if ping_result != "pong":
                        raise Exception(f"Cloud service health check failed: {ping_result}")

                if self.local_service is None:
                    logger.info("Local Hamilton service connection lost, attempting reconnect")
                    success = await self.connect_to_local_service()
                    if not success or self.local_service is None:
                        raise Exception("Failed to reconnect to local Hamilton service")

                local_ping = await asyncio.wait_for(self.local_service.ping(), timeout=10)
                if local_ping != "pong":
                    raise Exception(f"Local service health check failed: {local_ping}")
            except Exception as e:
                logger.error(f"Hamilton mirror health check failed: {e}")
                logger.info("Attempting cleanup and setup retry for Hamilton mirror...")
                try:
                    await self.cleanup_cloud_service()
                    if self.cloud_server:
                        await self.cloud_server.disconnect()
                    if self.local_server:
                        await self.local_server.disconnect()
                    if self.setup_task:
                        self.setup_task.cancel()
                except Exception as cleanup_error:
                    logger.error(f"Error during Hamilton mirror cleanup: {cleanup_error}")
                finally:
                    self.cloud_server = None
                    self.cloud_service = None
                    self.local_server = None
                    self.local_service = None
                    self.mirrored_methods.clear()

                retry_count = 0
                max_retries = 50
                base_delay = 10
                while retry_count < max_retries:
                    try:
                        delay = base_delay * (2 ** min(retry_count, 5))
                        logger.info(
                            f"Retrying Hamilton mirror setup in {delay} seconds "
                            f"(attempt {retry_count + 1}/{max_retries})"
                        )
                        await asyncio.sleep(delay)
                        self.setup_task = asyncio.create_task(self.setup())
                        await self.setup_task
                        logger.info("Hamilton mirror setup successful after reconnection")
                        break
                    except Exception as setup_error:
                        retry_count += 1
                        logger.error(
                            f"Failed to rerun Hamilton mirror setup "
                            f"(attempt {retry_count}/{max_retries}): {setup_error}"
                        )
                        if retry_count >= max_retries:
                            logger.error("Max retries reached for Hamilton mirror setup")
                            await asyncio.sleep(60)
                            break

            await asyncio.sleep(10)

    async def shutdown(self):
        logger.info("Hamilton mirror shutdown signal received")

        for task in self._background_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        await self.cleanup_cloud_service()

        if self.cloud_server:
            try:
                await self.cloud_server.disconnect()
                logger.info("Disconnected from cloud server")
            except Exception as e:
                logger.error(f"Error disconnecting cloud server: {e}")

        if self.local_server:
            try:
                await self.local_server.disconnect()
                logger.info("Disconnected from local server")
            except Exception as e:
                logger.error(f"Error disconnecting local server: {e}")

        self.cloud_server = None
        self.local_server = None
        self.local_service = None
        self.shutdown_event.set()
        logger.info("Hamilton mirror shutdown complete")

    async def start_hypha_service(self, server):
        self.cloud_server = server

        if self.local_service is None:
            logger.info("Local Hamilton service not connected, connecting before creating mirror methods")
            success = await self.connect_to_local_service()
            if not success:
                raise Exception("Cannot start Hamilton mirror without local service connection")

        self.mirrored_methods = self._get_mirrored_methods()
        service_config = {
            "name": "Mirror Hamilton Control Service",
            "id": self.cloud_service_id,
            "config": {
                "visibility": "protected",
                "run_in_executor": True,
            },
            "type": "service",
        }
        service_config.update(self.mirrored_methods)

        self.cloud_service = await server.register_service(service_config, overwrite=True)
        logger.info(
            f"Mirror Hamilton service (service_id={self.cloud_service_id}) started successfully "
            f"with {len(self.mirrored_methods)} mirrored methods"
        )

    async def setup(self):
        logger.info(f"Connecting to cloud workspace {self.cloud_workspace} at {self.cloud_server_url}")
        server = await connect_to_server({
            "server_url": self.cloud_server_url,
            "token": self.cloud_token,
            "workspace": self.cloud_workspace,
            "ping_interval": 30,
        })

        logger.info("Connecting to local Hamilton service before setting up mirror")
        success = await self.connect_to_local_service()
        if not success or self.local_service is None:
            raise Exception("Failed to connect to local Hamilton service during setup")

        ping_result = await asyncio.wait_for(self.local_service.ping(), timeout=10)
        if ping_result != "pong":
            raise Exception(f"Local Hamilton service verification failed: {ping_result}")

        await asyncio.sleep(1)
        await self.start_hypha_service(server)
        logger.info("Hamilton mirror setup completed successfully")

    def ping(self):
        return "pong"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mirror service for Hamilton control service.")
    parser.add_argument(
        "--cloud-service-id",
        help="Override the cloud service ID to register.",
    )
    parser.add_argument(
        "--local-service-id",
        help="Override the local service ID to connect to.",
    )
    args = parser.parse_args()

    mirror_service = MirrorHamiltonService()
    if args.cloud_service_id:
        mirror_service.cloud_service_id = args.cloud_service_id
    if args.local_service_id:
        mirror_service.local_service_id = args.local_service_id
    loop = asyncio.get_event_loop()

    async def main():
        try:
            mirror_service.setup_task = asyncio.create_task(mirror_service.setup())
            await mirror_service.setup_task
            hc_task = asyncio.create_task(mirror_service.check_service_health())
            mirror_service._background_tasks.append(hc_task)
            await mirror_service.shutdown_event.wait()
        except Exception:
            traceback.print_exc()
        finally:
            if not mirror_service.shutdown_event.is_set():
                await mirror_service.shutdown()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(mirror_service.shutdown()))
        except Exception:
            pass

    loop.run_until_complete(main())
