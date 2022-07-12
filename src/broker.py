import os #For managing files
import errno #For error handling

import socket #For creating websockets
import threading #For parallelism

import time #For calculating elapsed time
from datetime import datetime

import base64 #For encoding images

#For sending and receiving messages
from .protocol import KeepAliveMessage, Message, Protocol

#Wrapper class for holding image data for ease of access
class ImageWrapper:

    #Properties
    img_names = []
    image_str = ""
    state = 'PENDING'
    modified_time = 0
    neighbour = None

    #Static method for creating a wrapper object from a single image
    @classmethod
    def create(cls, name : str, path : str):

        #The object to be created
        img = ImageWrapper()

        #The names of the images this object is composed of
        img.img_names = [name]

        #Encodes the image to base64
        with open(path, "rb") as file:
            img.image_str = base64.b64encode(file.read()).decode('utf-8')

        #The time the file was last modified. Used for sorting
        img.modified_time = os.stat(path).st_mtime

        return img

    #Set double link between neighbours
    def set_neighbour(self, neighbour):
        self.neighbour = neighbour
        neighbour.neighbour = self
    
    #Formats for interface
    def __str__(self) -> str:
        return '{:13s} {:3s}'.format(self.state, '+'.join(self.img_names))

#Keep alive stats
KEEP_ALIVE_DELAY = 1
KEEP_ALIVE_TOLERANCE = 6

#Class for holding worker info
class WorkerInfo:
    
    #Properties
    addr = None
    state = "IDLE"
    missed_keep_alives = 0

    #Creates the worker
    def __init__(self, addr) -> None:
        self.addr = addr
    
    #Tags a dead worker as alive again
    def resurrect(self):
        self.state = "IDLE"
        self.missed_keep_alives = 0

        #Update the interface
        self.print_interface()
    
    #Formats for interface
    def __str__(self) -> str:
        return '{}:{:10s} {:10s} {}'.format(self.addr[0], str(self.addr[1]), self.state, f'{self.missed_keep_alives}/{KEEP_ALIVE_TOLERANCE}')

#Actual implementation of the Broker object
class Broker:

    #Whether the broker is running or not. When every job is done, the broker turns itself off.
    running = True

    #region STATISTICS

    #The number of total resize requests done (still counts even if it wasn't completed)
    count_resizes = 0
    count_merges = 0

    time_total = 0 #The elapsed time from beginning to end, in seconds
    time_resizes = [0.0,0.0,0.0] #Minimum, mean and maximum time respectively
    time_marges = [0.0,0.0,0.0] #Ditto

    #endregion

    #List of every worker (and info about them)
    workers = {}

    #Only constructor. Takes two arguments:
    #   1. The path to the images folder
    #   2. The height of the merged image should have
    def __init__(self, path : str, height : int) -> None:

        #Setup the images before anything else
        self.path = path
        self.height = height
        self.setup_images()

        #Start the server after images are validated
        self.start_server()

        #Wait for messages continuously
        self.run()

    #region IMAGE MANAGEMENT

    #Makes note of each valid image in the directory
    def setup_images(self):
        #Throw exception if directory doesn't exist
        if(not os.path.isdir(self.path)):
            raise FileNotFoundError( errno.ENOENT, os.strerror(errno.ENOENT), self.path)

        #The combined height must be a positive whole number.
        if self.height <= 0:
            raise ValueError(f"Desired height must be a positive integer.")
        pass

        #Iterates through the files in the images directory and creates the image object
        self.images = []
        for filename in os.listdir(self.path):
            f = os.path.join(self.path, filename)
            #Ignore non-image files (support only .jpg for now)
            if os.path.isfile(f) and filename.split('.')[-1] in ['jpg', 'jpeg']:
                #Creates the image and stores it the dictionary
                self.images.append(ImageWrapper.create(filename, f))

        #Sorts lists by date of last modification and sets neighbours
        self.images.sort(key=lambda x: x.modified_time, reverse=True)
        last_img = None
        for img in self.images:
            if last_img != None:
                img.set_neighbour(last_img)
            last_img = img

        #Print result
        self.put_outout_history(f"{len(self.images)} image(s) found.")

    #endregion

    def start_server(self):
        #Creates the brokers's server (UDP)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        #Added this line to prevent an error message stating that the previous address was already in use
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        #Binds the server socket to an interface address and port (> 1023)
        self.sock.bind((socket.gethostname(), 1024))

        self.put_outout_history(f"Started server at port {1024}.")

    #Serve clients countinously
    def run(self):

        #Creates a thread for sending keep alive messages
        keep_alive_thread = threading.Thread(target = self.send_keep_alive)
        keep_alive_thread.daemon = True
        keep_alive_thread.start()

        #Keeps servicing requests until the broker is shut off
        try:
            while self.running:

                #Gets message from worker
                message, worker_address = Protocol.receive(self.sock)

                #Creates a thread for the worker
                worker_thread = threading.Thread(target = self.handle_message, args = (message, worker_address))
                worker_thread.daemon = True
                worker_thread.start()
            
        #Shutdown the broker if the user interrupts the proccess
        except KeyboardInterrupt:
            self.poweroff()

    #Sends keep alive messages to every worker periodically. Also deletes workers who are potentially dead
    def send_keep_alive(self):
        while self.running:

            #Evaluate every worker
            for worker in self.workers.values():
                if worker.state == 'DEAD':
                    continue

                worker.missed_keep_alives += 1 #Increment the missed count

                #If missed too many messages, probably dead. Delete it
                if worker.missed_keep_alives >= KEEP_ALIVE_TOLERANCE:
                    worker.state = "DEAD"
                    self.put_outout_history(f"{worker.addr} is dead.")
                else:
                    Protocol.send(self.sock, worker.addr, KeepAliveMessage())

            #Update interfaces
            self.print_interface()

            #Wait ten seconds
            time.sleep(KEEP_ALIVE_DELAY)
    
    #Decides what to do with the message received
    def handle_message(self, msg : Message, addr):
        if msg.type == "HELLO":
            self.handle_hello(addr)
        elif msg.type == "KEEPALIVE":
            self.handle_keep_alive(addr)

    #This message means a worker connected to the broker
    def handle_hello(self, addr):
        #Either creates or resurrects the worker
        if addr not in self.workers.keys():
            self.workers[addr] = WorkerInfo(addr)
        else:
            self.workers[addr].resurrect()

        #Update the interface
        self.put_outout_history(f"{addr} worker just joined.")

    #Handles the keep alive by clearing the worker from suspicion for now
    def handle_keep_alive(self, addr):
        self.workers[addr].missed_keep_alives = 0

    #Shutdown the broker and workers
    def poweroff(self):
        self.running = False
        self.sock.close()

    #region INTERFACE

    #Helper method for the output history queue
    OUTPUT_QUEUE_LENGTH = 20    
    output = ['...' for i in range(0,OUTPUT_QUEUE_LENGTH)]
    def put_outout_history(self, value : str):

        #Get the current time for timestamp
        curr_time = datetime.now()
        time = "{:02d}:{:02d}:{:02d}".format(curr_time.hour, curr_time.minute, curr_time.second)

        #Put the output in the queue
        value = "{:10s} {}".format(time, value)
        for i in reversed(range(0, self.OUTPUT_QUEUE_LENGTH)):
            if i == 0:
                self.output[i] = value
            else:
                self.output[i] = self.output[i-1]
        self.print_interface()

    #Prints the interface
    def print_interface(self) -> None:
        os.system("cls||clear") #Clears on both windows and linux

        #Broker info
        print("BROKER\n"+"-"*47)  
        print(f"Address: {socket.gethostname()}")
        print("Port: 1024")

        #Image window
        print("\nIMAGES\n"+"-"*47)
        for id, img in enumerate(self.images):
            print(f'{id+1}.\t{img}')

        #Worker window
        print("\nWORKERS\n"+"-"*47)
        for id, worker in enumerate(self.workers.values()):
            print(f'{id+1}.\t{worker}')

        #Output window
        print("\nOUTPUT\n"+"-"*47)
        print("\n".join(self.output))
        pass

    #endregion