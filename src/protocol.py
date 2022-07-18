import json #For JSON serialization
import socket #For creating websockets
import zlib #For checksum

#The maximum size a packet can be in bytes
MAX_PACKET = 45000

#Basically, the max packet minus a few to account for the JSON overhead
MAX_FRAGMENT = MAX_PACKET - 100

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
            elif type == "KEEPALIVE":
                return KeepAliveMessage()
            elif type == "TASKCONFIRM":
                return TaskConfimationMessage()

            elif type == "RESIZEREQUEST":
                return ResizeRequestMessage(JSON["id"], JSON["fragments"], JSON["height"])
            elif type == "MERGEREQUEST":
                return MergeRequestMessage(JSON["id"], JSON["fragments"])
            elif type == "OPREPLY":
                return OperationReplyMessage(JSON["id"], JSON["fragments"])

            elif type == "FRAGREQUEST":
                return FragmentRequestMessage(JSON["id"], JSON["piece"])
            elif type == "FRAGREPLY":
                return FragmentReplyMessage(JSON["id"], JSON["data"], JSON["piece"])

            elif type == "DONE":
                return DoneMessage()

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

#A worker must confirm that he received the task, otherwise it is given to someone else
class TaskConfimationMessage(Message):
    def __init__(self) -> None:
        self.type = "TASKCONFIRM"

#Messages for requesting a worker to do an operation (resize and merge)
#The request comes with the number of fragments for each image has
#Worker must request fragments individually. This is done because UDP is an unreliable protocol
class ResizeRequestMessage(Message):
    def __init__(self,  id :str, fragments : int, height : int):
        self.type = "RESIZEREQUEST"
        self.height = height
        self.id = id #The id of the task
        self.fragments = fragments
class MergeRequestMessage(Message):
    def __init__(self,  id :tuple, fragments : tuple):
        self.type = "MERGEREQUEST"
        self.id = id #The id of the task
        self.fragments = fragments #A tuple, containing the fragment count of both images
class OperationReplyMessage(Message):
    def __init__(self, id : str, fragments: int):
        self.type = "OPREPLY"
        self.id = id #the id of the task
        self.fragments = fragments
        pass

#For requesting and receiving fragments
#Must contain it's piece because packets can arrive out of order
class FragmentRequestMessage(Message):
    def __init__(self, id : str, piece : int):
        self.type = "FRAGREQUEST"
        self.id = id #A tuple, containing the task id and the image index
        self.piece = piece
class FragmentReplyMessage(Message):
    def __init__(self, id : str, data : str, piece : int):
        self.type = "FRAGREPLY"
        self.id = id
        self.data = data
        self.piece = piece

#Message sent by the broker saying that the workers should powerdown
class DoneMessage(Message):
    def __init__(self) -> None:
        self.type = "DONE"