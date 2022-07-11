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

            type = json.loads(msg)["type"]

            if type == "HELLO":
                return HelloMessage()

            if type == "KEEPALIVE":
                return KeepAliveMessage()

        except:
            raise ValueError("Could not parse JSON to message.")
        pass

#Hello message from the worker. Informs the broker that this worker is connect to it
class HelloMessage(Message):
    def __init__(self):
        self.type = "HELLO"

    def __str__(self) -> str:
        return "Hello, I'm ready for work."

#Keep alive message. Used for knowing when a worker dies
class KeepAliveMessage(Message):
    def __init__(self):
        self.type = "KEEPALIVE"

    def __str__(self) -> str:
        return "Keeping alive..."

#The protocols for sending text messages and images
class Protocol:

    #The number of bytes for the header
    HEADER_BYTES = 4

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