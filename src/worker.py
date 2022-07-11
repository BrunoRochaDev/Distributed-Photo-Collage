import socket #For creating websockets
from PIL import Image #For processing images
import threading #For parallelism

#For sending and receiving messages
from .protocol import *

class Worker:

    #Whether the worker is running or not. Turned off by the broker
    running = True

    def __init__(self, port : int, address : str = socket.gethostname()) -> None:
        
        self.broker_sock = (address, port)

        #Starts the client and connects to the broker
        self.start_client()

        #Run the client and waits for commands
        self.run()

    #Starts the client and connects to the broker
    def start_client(self):

        #Creates the workers's client (UDP)
        self.sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

        #Sends to broker a hello message
        Protocol.send(self.sock, self.broker_sock, HelloMessage())

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
        if msg.type == "KEEPALIVE":
            self.handle_keep_alive(msg)

    #Simply sends the message back so that the broker knows this is alive
    def handle_keep_alive(self, msg : KeepAliveMessage):
        Protocol.send(self.sock, self.broker_sock, msg)

    #Shutdown the worker
    def poweroff(self):
        self.running = False
        self.sock.close()