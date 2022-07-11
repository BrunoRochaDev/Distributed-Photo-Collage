import os #For managing files
import errno #For error handling
import time #For calculating elapsed time
from PIL import Image #For processing images
import base64 #For encoding images
import socket #For creating websockets

#For sending and receiving messages
from .protocol import Protocol

#Wrapper class for holding image data for ease of access
class ImageWrapper:

    #Properties
    imgNames = []
    image_str = ""
    resized = False
    modified_time = 0
    neighbour = None

    #Static method for creating a wrapper object from a single image
    @classmethod
    def Create(cls, name : str, path : str):

        #The object to be created
        img = ImageWrapper()

        #The names of the images this object is composed of
        img.imgNames = [name]

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
    pass

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

    #List of every worker who was sent some task (still counts even if it wasn't completed)
    #The list holds another list inside, the very element being the worker's ID, the second the list of tasks it has performed and the the third is whether it's currently free
    workers_operations = []

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
        images = []
        for filename in os.listdir(self.path):
            f = os.path.join(self.path, filename)
            #Ignore non-image files (support only .jpg for now)
            if os.path.isfile(f) and filename.split('.')[-1] in ['jpg', 'jpeg']:
                #Creates the image and stores it the dictionary
                images.append(ImageWrapper.Create(filename, f))

        #Sorts lists by date of last modification and sets neighbours
        images.sort(key=lambda x: x.modified_time, reverse=True)
        last_img = None
        for img in images:
            if last_img != None:
                img.set_neighbour(last_img)
            last_img = img

        #Print result
        img_count = len(images)
        print(f"{img_count} image(s) found.")

    #endregion

    def start_server(self):

        #Creates the brokers's server (UDP)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        #Added this line to prevent an error message stating that the previous address was already in use
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        #Binds the server socket to an interface address and port (> 1023)
        self.sock.bind((socket.gethostname(), 1024))

        print(f"Started server at port {1024}.")

    #Continuously receive messages 
    def run(self):

        while self.running:
            message = Protocol.receive(self.sock)

            clientMsg = "Message from Client:{}".format(message)
            
            print(clientMsg)
            pass