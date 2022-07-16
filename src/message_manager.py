from datetime import datetime
import threading
from .protocol import * #Import all the messages
import socket #For creating websockets

class ImageRequest:
    
    def __init__(self, addr, id : str, fragment_count : int, callback, data : dict = None, worker : int = 0) -> None:
        self.addr = addr
        self.id = id
        self.fragment_count = fragment_count
        self.callback = callback
        self.data = data #Anything that needs to be used in the callback function
        self.worker = worker
        self.received_pieces = set()
        self.received_fragments = [None for i in range(fragment_count)]
        self.image_base64 = ""
    
    #Returns the fragments still needed
    def fragments_needed(self) -> set:
        return set(i for i in range(self.fragment_count)).difference(self.received_pieces)

    #Adds a fragment and returns if it has the image completed
    def add_fragment(self, piece : int, fragment : str) -> bool:
        self.received_pieces.add(piece)
        self.received_fragments[piece] = fragment

        #Can complete the image?
        if len(self.fragments_needed()) == 0:
            self.image_base64 = ''.join(self.received_fragments)
            self.callback(self)
            return True
        else:
            return False


#Class used by the worker and broker to send end receive messages
class MessageManager:
    
    #Receives the socket on creation
    def __init__(self, sock : socket) -> None:
        self.sock = sock
        self.request_dict = {}

        #Use a lock to make sure only one thread uses the sendto() method at a time.
        self.sock_lock = threading.Lock()

    #Sends a message
    def send(self, addr, msg : Message) -> None:
        #if(msg.type == "OPREPLY"):
            #print(datetime.now(),"sending", msg.encode())

        #Construct the message with the checksum
        byte_msg = str.encode(msg.encode())
        checksum = self.calculate_checksum(byte_msg)

        with self.sock_lock:
            self.sock.sendto(byte_msg,addr)

    def receive(self):
        data, client_address = self.sock.recvfrom(MAX_PACKET)

        #Decodes the image
        msg = Message.decode(data.decode('utf-8'))
        #if(msg.type != "KEEPALIVE"):
            #print(datetime.now(),"receiving", msg.type)

        #If it's not a fragment, returns it to be handled by the broker / worker
        if msg.type != "FRAGREPLY":
            return (msg, client_address)
        #If it is, it's handled by the received fragment
        else:
            self.received_fragment(msg)
            return (None, client_address)

    #Used for corruption verification
    def calculate_checksum(self, data) -> int:
        return zlib.crc32(data)

    #
    def request_image(self, addr, id :str, fragment_count : int, callback, data : dict = None, worker : int = 0) -> str:
        #print(datetime.now(),"requesting img", id)
        #Stores the new request in the dict
        request_obj = ImageRequest(addr, id, fragment_count, callback, data, worker)
        self.request_dict[id] = request_obj

    #Request the pending fragments
    def request_fragments(self):

        for request in self.request_dict.copy().values():
            for piece in request.fragments_needed():
                #print(datetime.now(),"requesting fragment", piece)
                self.send(request.addr, FragmentRequestMessage(request.id, piece))
        pass

    #When receives a fragment, shares it w/ requesters
    def received_fragment(self, msg : FragmentReplyMessage):
        if(msg.id not in self.request_dict.keys()):
            return
        
        piece = int(msg.piece)

        request = self.request_dict[msg.id]
        completed = request.add_fragment(piece, msg.data)

        #Remove from list if completed
        if completed:
            self.request_dict.pop(msg.id)
            #print(datetime.now(),"completed",msg.id)