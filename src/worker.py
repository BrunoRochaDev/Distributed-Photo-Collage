import math #For resizing image
import socket #For creating websockets
import threading #For parallelism
import os #For clearing the console
from datetime import datetime #For making timestamps
import time #For sleeping (simulate complex work)
import random #For picking random sleep intervals
import sys #For closing the app

from .message_manager import ImageRequest, MessageManager

#For sending and receiving messages
from .protocol import *

from PIL import Image #For processing images
import base64 #For encoding images
from io import BytesIO #For encoding images

#Wrapper class for holding image data for ease of access
#DIFFERENT FROM THE ONE IN THE BROKER
class ImageWrapper:

    #Properties
    id = ""
    image_encoded = b""

    #Creates the image wrapper
    def __init__(self, id : str, PIL_image : Image) -> None:

        self.id = id
        self.image_encoded = ImageWrapper.encode(PIL_image)

        pass

    #Gets the number of fragments this image has
    def fragment_count(self):
        return math.ceil(len(self.image_encoded) / MAX_FRAGMENT)

    #Gets an specific fragment
    def get_fragment(self, piece : int) -> bytes:
        start = piece * MAX_FRAGMENT
        return self.image_encoded[start:start + MAX_FRAGMENT]

    #Convert Image to Base64 
    @classmethod
    def encode(cls, img : Image) -> str:
        buffer = BytesIO()
        img.save(buffer, format="JPEG")
        return base64.b64encode(buffer.getvalue())

    #Convert Base64 to Image 
    @classmethod
    def decode(cls, data : str) -> Image:
        buff = BytesIO(base64.b64decode(data))
        return Image.open(buff)

class Worker:

    #Times for sleeping (in seconds) to simulate complex work
    MIN_SLEEP = 1
    MAX_SLEEP = 3

    #The odds of failure for every operation
    FAILURE_ODDS = 0.01

    #If on, the worker will randomly crash or freeze
    simulate_failure = False

    #Whether the worker is running or not. Turned off by the broker
    running = True

    #If the broker has acknowledged the connection. Turned on by a hello message
    connected = False

    #The worker's id, given by the broker
    id = 0

    #The current status of the worker. Can be either IDLE, MERGING, RESIZING or DONE 
    status = "OFF"

    #The dictionary of the images
    images = {}

    #Whether has a completed tasking pending confirmation
    pending_task = False

    def __init__(self, port : int, address : str = socket.gethostname(), simulate_failure : bool = False, min_sleep : float = None, max_sleep : float = None, fail_odds : float = None, format_output : bool = True) -> None:

        #Set default values
        address = socket.gethostname() if address == None else address
        self.simulate_failure = False if simulate_failure == None else simulate_failure
        self.MIN_SLEEP = self.MIN_SLEEP if min_sleep == None else min_sleep
        self.MAX_SLEEP = self.MAX_SLEEP if max_sleep == None else max_sleep
        self.FAILURE_ODDS = self.FAILURE_ODDS if fail_odds == None else fail_odds

        #Formats the output or not
        self.format_output = format_output

        self.broker_sock = (address, port)

        #Starts the client and connects to the broker
        self.start_client()

        if self.simulate_failure:
            self.output("Failure simulation is activated.")

        #Listens to requests from the broker
        self.run()

    #Starts the client and connects to the broker
    def start_client(self):

        #Creates the workers's client (UDP)
        self.sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.port = self.sock.getsockname()[1]
        self.output("Starting up client...")

        #Creates the message manager, for sendind and reciving messages
        self.message_manager = MessageManager(self.sock)

        #Sends to broker a hello message
        self.message_manager.send(self.broker_sock, HelloMessage())
        self.output(f"Attemping to connect to broker {self.broker_sock}...")


    #Wait for commands from the broker
    def run(self):

        #Wait for commands from the broker
        try:
            while self.running:

                #Gets message from broker
                message = self.message_manager.receive()[0]
                
                #If has a pending task, continue sending it until confirmation is given
                if self.pending_task != False:
                    self.message_manager.send(self.broker_sock, self.pending_task)

                #Requests any pending fragments that there might have
                self.message_manager.request_fragments()

                #If message is None, then it's being handled by the message_manager. Skip
                if message == None:
                    continue

                #Creates a thread for the worker
                worker_thread = threading.Thread(target = self.handle_message, args = (message,))
                worker_thread.daemon = True
                worker_thread.start()
            
        #Shutdown the broker if the user interrupts the proccess
        except KeyboardInterrupt:
            self.poweroff()

    #Decides what to do with the message received
    def handle_message(self, msg : Message):
        if msg.type == "HELLO":
            self.handle_hello(msg)
        elif msg.type == "KEEPALIVE":
            self.handle_keep_alive(msg)
        elif msg.type == "TASKCONFIRM":
            self.handle_task_confirmation()
        elif msg.type == "RESIZEREQUEST":
            self.handle_resize_request(msg)
        elif msg.type == "MERGEREQUEST":
            self.handle_merge_request(msg)
        elif msg.type == "FRAGREQUEST":
            self.handle_fragment_request(msg)
        elif msg.type == "DONE":
            self.handle_done()
    #This message means the broker has accepted the connection
    def handle_hello(self,msg : HelloMessage):

        #Worker has acknowledged the connection and received an ID
        self.connected = True
        self.id = msg.id
        self.status = "IDLE"

        #Update the interface
        self.output("Broker has acknowledged the connection.")

    #Simply sends the message back so that the broker knows this is alive
    def handle_keep_alive(self, msg : KeepAliveMessage):
        self.message_manager.send(self.broker_sock, msg)

    #Handles the task confimation, to be sure that the message was not lost
    def handle_task_confirmation(self):
        if self.pending_task != False:
            self.output("Received confirmation that task was received.")
            self.pending_task = False

    #Firstly collect all the image fragments and then resizes
    def handle_resize_request(self, msg : ResizeRequestMessage):

        #Can only accept new tasks if the last one was confirmed
        if self.pending_task != False:
            return

        self.status = "RESIZING"
        self.output("Received a resize operation request. Resizing...")

        #Sends a confirmation that it received the task
        #TODO: Colocar id da tarefa na confirmation?
        self.message_manager.send(self.broker_sock, TaskConfimationMessage())

        #Invokes merge callback when the image is reconstructed
        self.message_manager.request_image(self.broker_sock, msg.id, msg.fragments, self.resize_callback, {"desired_height" : msg.height})


    #Invoked when all the fragments of the image is collected and the image is constructed
    def resize_callback(self, request : ImageRequest):

        #Gets the desired height from the request
        desired_height = request.data["desired_height"]

        PIL_image = ImageWrapper.decode(request.image_base64)

        #Calculates the new dimension
        width, height = PIL_image.size
        ratio = width/height

        new_width = math.ceil(desired_height * ratio)
        new_height = math.ceil(desired_height)

        #Resizes the image and stores it in the image dict
        PIL_image = PIL_image.resize((new_width, new_height))

        #Either hangs, crashes or freezes if failure simulation is on
        if self.simulate_failure:
            self.fail()

        self.images[request.id] = ImageWrapper(request.id, PIL_image)

        #Notifies the broker it's done
        self.status = "IDLE"
        self.output("Resized the image. Sending it to the broker...")
        self.pending_task = OperationReplyMessage(request.id, self.images[request.id].fragment_count())
        self.message_manager.send(self.broker_sock, self.pending_task)
                
    #Firstly collect all the image fragments and then merges
    def handle_merge_request(self, msg : MergeRequestMessage):

        #Can only accept new tasks if the last one was confirmed
        if self.pending_task != False:
            return

        self.status = "MERGING"
        self.output("Received a merge operation request. Merging...")

        #Sends a confirmation that it received the task
        self.message_manager.send(self.broker_sock, TaskConfimationMessage())

        self.A_image : Image = None
        self.B_image : Image = None

        #Invokes merge callback when the image is reconstructed
        self.message_manager.request_image(self.broker_sock, (msg.id,0), msg.fragments[0], self.merge_callback)
        self.message_manager.request_image(self.broker_sock, (msg.id,1), msg.fragments[1], self.merge_callback)

    #Invoked when all the fragments of the image is collected and the image is constructed
    #Waits until both images were reconstructed to merge it
    def merge_callback(self, request : ImageRequest):
        #Reconstruct the image...
        if request.id[1] == 0:
           self.A_image = ImageWrapper.decode(request.image_base64)
        else:
            self.B_image = ImageWrapper.decode(request.image_base64)

        #When both images are reconstructed, then merge
        if self.A_image != None and self.B_image != None:
            self.merge(request)


    #Finally merges after both images have been reconstructed
    def merge(self, request : ImageRequest):
        #Merges the images
        A_image_size = self.A_image.size
        B_image_size = self.B_image.size

        #Try to merge the images, sometimes "mage file is truncated" is thrown
        try:
            merged_image = Image.new('RGB',(A_image_size[0] + B_image_size[0], A_image_size[1]), (250,250,250))
            merged_image.paste(self.A_image, (0,0))
            merged_image.paste(self.B_image, (A_image_size[0],0))
        except:
            self.output("An error occured while merging. Potential data corruption")

        #Either hangs, crashes or freezes if failure simulation is on
        if self.simulate_failure:
            self.fail()

        #Stores it
        self.images[request.id[0]] = ImageWrapper(request.id, merged_image)

        #Notifies the broker it's done
        self.status = "IDLE"
        self.output("Merged the images. Sending it to the broker...")
        self.pending_task = OperationReplyMessage(request.id, self.images[request.id[0]].fragment_count())
        self.message_manager.send(self.broker_sock, self.pending_task)

    #Handles the request for image fragments
    def handle_fragment_request(self, msg : FragmentRequestMessage):
        #Sends back the requested piece

        img = self.images[msg.id]
        
        fragment = img.get_fragment(msg.piece)
        reply = FragmentReplyMessage(msg.id, fragment.decode('utf-8'), msg.piece)
        self.message_manager.send(self.broker_sock, reply)  

    #Powers off by the broker's request
    def handle_done(self):
        self.output("Received the order to power off from the broker.")
        self.poweroff()  

    #Shutdown the worker
    def poweroff(self):
        self.running = False
        self.connected = False
        self.status = "OFF"

        self.output("Powering off...")
        self.sock.close()

        sys.exit()

    #region INTERFACE

    #Helper method for the output history queue
    OUTPUT_QUEUE_LENGTH = 20    
    output_history = ['...' for i in range(0,OUTPUT_QUEUE_LENGTH)]
    def output(self, value : str):

        #Get the current time for timestamp
        curr_time = datetime.now()
        time = "{:02d}:{:02d}:{:02d}".format(curr_time.hour, curr_time.minute, curr_time.second)

        #Put the output in the queue
        value = "{:10s} {}".format(time, value)
        for i in reversed(range(0, self.OUTPUT_QUEUE_LENGTH)):
            if i == 0:
                self.output_history[i] = value
            else:
                self.output_history[i] = self.output_history[i-1]

        #Prints as an interface or not, depending on args
        if self.format_output:
            self.print_interface()
        else:
            print(value)

    #Prints the interface
    def print_interface(self) -> None:
        #return
        os.system("cls||clear") #Clears on both windows and linux

        #Worker info
        print("WORKER "+(f"(Connected)" if self.connected else "(Not connected)")+"\n"+"-"*47)
        print(f"Port: {self.port}")
        print(f"Identifer: " + (str(self.id) if self.id != 0 else "N/A"))
        print(f"Broker: " + (f"{self.broker_sock[0]}:{self.broker_sock[1]}" if self.connected else "N/A"))
        print(f"Status: {self.status}")

        #Output window
        print("\nOUTPUT\n"+"-"*47)
        print("\n".join(self.output_history))
        pass

    #endregion

    #Either sleeps, crashes or freezes
    def fail(self):

        chance = random.uniform(0, 1)

        #Failure...
        if chance < self.FAILURE_ODDS:

            #Crash
            if random.uniform(0,1) > 0.5:
                raise Exception("Simulating a process crash.")
            else:
            #Freeze
                self.output("Sleeping for an infinite amount of time to simulate freezing")
                while True:
                    time.sleep(1)

            pass
        #Delay
        else:
            delay_time = random.uniform(self.MIN_SLEEP, self.MAX_SLEEP)
            self.output("Sleeping for {:.1f} seconds to simulate hangs.".format(delay_time))
            time.sleep(delay_time)
            pass