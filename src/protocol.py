import json #For JSON serialization
import socket #For creating websockets

#Base message. Other messages should extend this
class Message:
    #Serializes to JSON (can be overriden)
    def encode(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=False)

    #Static method for parsing a JSON dict to a message
    @classmethod
    def decode(cls, JSON : dict):
        try:
            type = JSON["type"]

            if type == "HELLO":
                return HelloMessage()

        except:
            raise ValueError("Could not parse JSON to message.")
        pass

#Hello message from the worker. Informs the broker that this worker is connect to it
class HelloMessage(Message):
    def __init__(self):
        self.type = "HELLO"

    def __str__(self) -> str:
        return "Hello, I'm ready for work."

#The protocols for sending text messages and images
class Protocol:

    #The number of bytes for the header
    HEADER_BYTES = 4

    #Sends a message
    @classmethod
    def send(cls,sock : socket, addr, msg : Message) -> None:

        #Construct the message with a header
        byte_msg = str.encode(msg.encode())
        header = len(byte_msg)
        msg = header.to_bytes(cls.HEADER_BYTES, byteorder='big') + byte_msg

        #Keeps sending until all bytes were send

        while header > 0:
            header -= sock.sendto(msg, addr)
        pass
    
    #Receives a message
    @classmethod
    def receive(cls,sock : socket) -> str:
        try:
            byte_msg = b''
            total_bytes = int.from_bytes(sock.recvfrom(cls.HEADER_BYTES)[0], byteorder='big')
            while len(byte_msg) < total_bytes:
                byte_msg += sock.recvfrom(total_bytes - len(byte_msg))[0]
                print(byte_msg)

            json_msg = json.loads(byte_msg.decode("UTF-8"))

            return Message.decode(json_msg)
        except Exception as e:
            #Either a bad message was received or the socket connection ended abruptly.
            print("BAD MESSAGE")
            pass
    pass