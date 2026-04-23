from dorna2 import Dorna
import time

class DornaController:
    def __init__(self, ip="192.168.2.20"):
        self.robot = Dorna()
        self.ip = ip

    def connect(self):
        self.robot.connect(self.ip)
        print("Connected to robot")

    def disconnect(self):
        self.robot.close()
        print("Disconnected from robot")

    def set_motor(self, state):
        self.robot.set_motor(state)

    def play_script(self, script_path):
        print("Playing script")
        self.robot.play_script(script_path)

    def play_script_sequence(self, script_paths):
        for script_path in script_paths:
            self.play_script(script_path)

    def is_busy(self):
        status = self.robot.track_cmd()
        print(f"Robot status: {status}")
        return status["union"].get("stat", -1) != 2

    def transport_plate(self, from_device, to_device):
        """
        Transport a plate between devices.
        
        Args:
            from_device: Source device - 'incubator', 'hamilton', 'squid-1', 'squid-2', 'squid-plus-3'
            to_device: Target device - 'incubator', 'hamilton', 'squid-1', 'squid-2', 'squid-plus-3'
        """
        self.set_motor(1)
        grab_path = f"paths/grab_from_{from_device}.txt"
        put_path = f"paths/put_on_{to_device}.txt"
        self.play_script_sequence((grab_path, put_path))

    def grab_from(self, device):
        """Grab a sample from a device."""
        self.set_motor(1)
        self.play_script(f"paths/grab_from_{device}.txt")
    
    def put_on(self, device):
        """Place a sample on a device."""
        self.set_motor(1)
        self.play_script(f"paths/put_on_{device}.txt")
    
    def halt(self):
        self.robot.halt()
        print("Robot halted")
    
    def light_on(self):
        self.robot.set_output(7, 0) # set the value of the out0 to 1

    def light_off(self):
        self.robot.set_output(7, 1) # set the value of the out0 to 0

        

if __name__ == "__main__":
    controller = DornaController()
    # Example usage
    controller.connect()
    #controller.transport_plate("squid-1", "incubator")
    print("Is robot busy?", controller.is_busy())
    #controller.halt()
    print(controller.robot.get_all_joint())

    controller.light_on()
    time.sleep(1)
    controller.light_off()
    controller.disconnect()
