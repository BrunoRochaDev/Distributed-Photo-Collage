import socket #For creating websockets
from PIL import Image #For processing images
import threading #For parallelism
import os #For clearing the console
from datetime import datetime #For making timestamps

#For sending and receiving messages
from .protocol import *

class Worker:

    #Whether the worker is running or not. Turned off by the broker
    running = True

    #If the broker has acknowledged the connection. Turned on by a hello message
    connected = False

    #The worker's id, given by the broker
    id = 0

    #The current status of the worker. Can be either IDLE, MERGING, RESIZING or DONE 
    status = "IDLE"

    def __init__(self, port : int, address : str = socket.gethostname()) -> None:
        
        self.broker_sock = (address, port)

        #Starts the client and connects to the broker
        self.start_client()

        #Listens to requests from the broker
        self.run()

    #Starts the client and connects to the broker
    def start_client(self):

        #Creates the workers's client (UDP)
        self.sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.put_outout_history("Starting up client...")

        #Sends to broker a hello message
        Protocol.send(self.sock, self.broker_sock, HelloMessage())
        self.put_outout_history(f"Attemping to connect to broker {self.broker_sock}...")


    #Wait for commands from the broker
    def run(self):

        #Wait for commands from the broker
        try:
            while self.running:

                #Gets message from worker
                message, worker_address = Protocol.receive(self.sock)

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

    #This message means the broker has accepted the connection
    def handle_hello(self,msg : HelloMessage):

        #Worker has acknowledged the connection and received an ID
        self.connected = True
        self.id = msg.id

        #Update the interface
        self.put_outout_history("Broker has acknowledged the connection.")


    #Simply sends the message back so that the broker knows this is alive
    def handle_keep_alive(self, msg : KeepAliveMessage):
        Protocol.send(self.sock, self.broker_sock, msg)

    #Shutdown the worker
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

        #Worker info
        print("WORKER "+(f"(Connected)" if self.connected else "(Not connected)")+"\n"+"-"*47)  
        print(f"Identifer: " + (str(self.id) if self.id != 0 else "N/A"))
        print(f"Broker: " + (f"{self.broker_sock[0]}:{self.broker_sock[1]}" if self.connected else "N/A"))
        print(f"Status: {self.status}")

        #Output window
        print("\nOUTPUT\n"+"-"*47)
        print("\n".join(self.output))
        pass

    #endregion