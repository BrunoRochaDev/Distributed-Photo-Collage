import json #For JSON serialization
import socket #For creating websockets
import struct
import zlib #For checksum

#Base message. Other messages should extend this
class Message:
    type = ""
    #Serializes to JSON (can be overriden)
    def encode(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=False)

    #Static method for parsing a JSON dict to a message
    @classmethod
    def decode(cls, msg : dict):
        try:
            
            JSON = json.loads(msg)
            type = JSON["type"]

            if type == "HELLO":
                return HelloMessage(JSON["id"])

            if type == "KEEPALIVE":
                return KeepAliveMessage()

            if type == "OPREQUEST":
                return OperationRequestMessage(JSON["operation"], JSON["fragments"])

        except:
            raise ValueError("Could not parse JSON to message.")
        pass

#Hello message, either from the broker or worker.
#When sent by the worker, informs the broker that the worker is connect to it
#When sent by the broker, informs the worker that the broker has acknowledged the connecion
class HelloMessage(Message):
    def __init__(self, id : int = 0):
        self.type = "HELLO"
        self.id  = id #The id is set by the broker, informing the worker's identifier

#Keep alive message. Used for knowing when a worker dies
class KeepAliveMessage(Message):
    def __init__(self):
        self.type = "KEEPALIVE"

#Requests a worker to do an operation with a file
#The request comes with the number of fragments for each image has
#Worker must request fragments individually. This is done because UDP is an unreliable protocol
class OperationRequestMessage(Message):
    def __init__(self, operation : str, fragments : list):
        self.type = "OPREQUEST"
        self.operation = operation #Can be "MERGE" or "RESIZE"
        self.fragments = fragments

#The protocols for sending text messages and images
class Protocol:

    #The number of bytes for the header
    HEADER_BYTES = 4

    #The maximum size a packet can be in bytes
    MAX_PACKET = 65000

    #Sends a message
    @classmethod
    def send(cls,sock : socket, addr, msg : Message) -> None:

        #Construct the message with a header
        byte_msg = str.encode(msg.encode())
        checksum = cls.calculate_checksum(byte_msg)

        sock.sendto(byte_msg,addr)
    
    @classmethod
    def calculate_checksum(cls, data) -> int:
        return zlib.crc32(data)

    #Receives a message
    @classmethod
    def receive(cls,sock : socket):
        data, client_address = sock.recvfrom(1024)
        return (Message.decode(data.decode('utf-8')), client_address)
    pass