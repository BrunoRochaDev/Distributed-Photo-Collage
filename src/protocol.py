import json #For JSON serialization
import socket #For creating websockets
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
                return OperationRequestMessage(JSON["operation"],JSON["id"], JSON["fragments"])

            if type == "FRAGREQUEST":
                return FragmentRequestMessage(JSON["id"], JSON["piece"])
            if type == "FRAGREPLY":
                return FragmentReplyMessage(JSON["id"], JSON["data"], JSON["piece"])

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
    def __init__(self, operation : str, id :str, fragments : list):
        self.type = "OPREQUEST"
        self.operation = operation #Can be "MERGE" or "RESIZE"
        self.id = id #The name of the file
        self.fragments = fragments

#For requesting and receiving fragments
#Must contain it's piece because packets can arrive out of order
class FragmentRequestMessage(Message):
    def __init__(self, id : str, piece : int):
        self.type = "FRAGREQUEST"
        self.id = id
        self.piece = piece
class FragmentReplyMessage(Message):
    def __init__(self, id : str, data : str, piece : int):
        self.type = "FRAGREPLY"
        self.id = id
        self.data = data
        self.piece = piece

#The protocols for sending text messages and images
class Protocol:

    #The number of bytes for the header
    HEADER_BYTES = 4

    #The maximum size a packet can be in bytes
    MAX_PACKET = 64000

    #Basically, the max packet minus a few to account for the JSON overhead
    MAX_FRAGMENT = MAX_PACKET - 70 

    #Sends a message
    @classmethod
    def send(cls,sock : socket, addr, msg : Message) -> None:

        #Construct the message with the checksum
        byte_msg = str.encode(msg.encode())
        checksum = cls.calculate_checksum(byte_msg)

        sock.sendto(byte_msg,addr)
    
    @classmethod
    def calculate_checksum(cls, data) -> int:
        return zlib.crc32(data)

    #Receives a message
    @classmethod
    def receive(cls,sock : socket) -> Message:
        data, client_address = sock.recvfrom(cls.MAX_PACKET)
        return (Message.decode(data.decode('utf-8')), client_address)
    pass

    #Gets each fragment of an image and reconstruct it into a base64 string
    @classmethod
    def request_image(cls, sock : socket, addr, id :str, fragment_count : int) -> str:
        
        received_pieces = []
        received_fragments = [None for i in range(fragment_count)]

        #Keeps trying until all fragment are obtained
        while len(received_pieces) < fragment_count:

            #Asks for the missing pieces
            for i in range(0, fragment_count):
                if i not in received_pieces:
                    cls.send(sock, addr, FragmentRequestMessage(id, i))

            for j in range(0, fragment_count):
                #Ignore unrelated messages (might be dangerous, change this later?)
                msg = cls.receive(sock)[0]
                if(msg.type != "FRAGREPLY"):
                    continue
                #Add the fragment to the collection
                if msg.piece not in received_pieces:
                    received_pieces.append(msg.piece)
                    received_fragments[msg.piece] = msg.data

        #Reconstruct it and send it
        return ''.join(received_fragments)
