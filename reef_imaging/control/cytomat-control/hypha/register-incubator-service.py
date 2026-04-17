import asyncio
from hypha_rpc import connect_to_server, login
from cytomat import Cytomat


class IncubatorService:
    """Singleton incubator service that reuses a single Cytomat instance."""

    def __init__(self):
        self._cytomat = None

    def _get_cytomat(self):
        if self._cytomat is None:
            self._cytomat = Cytomat(
                "/dev/ttyUSB1",
                json_path="/home/tao/workspace/cytomat-controller/docs/config.json",
            )
        return self._cytomat

    def initialize(self):
        c = self._get_cytomat()
        c.plate_handler.initialize()

    def move_plate(self, slot):
        c = self._get_cytomat()
        c.wait_until_not_busy(timeout=50)
        c.plate_handler.initialize()
        c.wait_until_not_busy(timeout=50)
        c.plate_handler.move_plate_from_transfer_station_to_slot(slot)
        c.wait_until_not_busy(timeout=50)
        c.plate_handler.move_plate_from_slot_to_transfer_station(slot)
        c.wait_until_not_busy(timeout=50)
        return f"Plate moved to slot {slot} and back to transfer station."

    def put_sample_from_transfer_station_to_slot(self, slot=5):
        c = self._get_cytomat()
        c.plate_handler.move_plate_from_transfer_station_to_slot(slot)

    def get_sample_from_slot_to_transfer_station(self, slot=5):
        c = self._get_cytomat()
        c.plate_handler.move_plate_from_slot_to_transfer_station(slot)

    def is_busy(self):
        c = self._get_cytomat()
        return c.overview_status.busy


async def start_server(server_url):
    server = await connect_to_server({"server_url": server_url, "ping_interval": 30})

    svc_impl = IncubatorService()

    svc = await server.register_service({
        "name": "Incubator Control",
        "id": "incubator-control",
        "config": {
            "visibility": "protected"
        },
        "initialize": svc_impl.initialize,
        "put_sample_from_transfer_station_to_slot": svc_impl.put_sample_from_transfer_station_to_slot,
        "get_sample_from_slot_to_transfer_station": svc_impl.get_sample_from_slot_to_transfer_station,
        "is_busy": svc_impl.is_busy,
    })

    print(f"Incubator control service registered at workspace: {server.config.workspace}, id: {svc.id}")
    print(f"You can use this service using the service id: {svc.id}")
    id = svc.id.split(":")[1]
    print(f"You can also test the service via the HTTP proxy: {server_url}/{server.config.workspace}/services/{id}/initialize")

    # Keep the server running
    await server.serve()


if __name__ == "__main__":
    server_url = "http://localhost:9527"
    asyncio.run(start_server(server_url))
