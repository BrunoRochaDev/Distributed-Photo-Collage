import socket #For creating websockets

#For sending and receiving messages
from .protocol import Protocol, HelloMessage

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
        while self.running:
            pass
