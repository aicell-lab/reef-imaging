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

    def is_busy(self):
        status = self.robot.track_cmd()
        print(f"Robot status: {status}")
        return status["union"].get("stat", -1) != 2

    def move_sample_from_microscope1_to_incubator(self):
        self.set_motor(1)
        self.play_script("paths/microscope1_to_incubator.txt")
    
    def grab_sample_from_microscope1(self):
        self.set_motor(1)
        self.play_script("paths/grab_from_microscope1.txt")
    
    def grab_sample_from_incubator(self):
        self.set_motor(1)
        self.play_script("paths/grab_from_incubator.txt")
    
    def put_sample_on_microscope1(self):
        self.set_motor(1)
        self.play_script("paths/put_on_microscope1.txt")
    
    def put_sample_on_incubator(self):
        self.set_motor(1)
        self.play_script("paths/put_on_incubator.txt")
    
    def transport_from_incubator_to_microscope1(self):
        self.set_motor(1)
        self.play_script("paths/transport_from_incubator_to_microscope1.txt")
    
    def transport_to_incubator(self):
        self.set_motor(1)
        self.play_script("paths/transport_to_incubator.txt")

    def move_sample_from_incubator_to_microscope1(self):
        self.set_motor(1)
        self.play_script("paths/incubator_to_microscope1.txt")

    def move_plate(self, source, destination):
        if source == "microscope" and destination == "incubator":
            self.transport_to_incubator()
        elif source == "incubator" and destination == "microscope1":
            self.move_sample_from_incubator_to_microscope1()
        else:
            print(f"Invalid source-destination combination: {source} to {destination}")
    
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
    #move_plate(controller, "microscope1", "incubator")
    print("Is robot busy?", controller.is_busy())
    #controller.halt()
    print(controller.robot.get_all_joint())

    controller.light_on()
    time.sleep(1)
    controller.light_off()
    controller.disconnect()
