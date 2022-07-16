import os #For managing files
import errno #For error handling

import socket #For creating websockets
import threading #For parallelism

import time #For sleeping
from datetime import datetime #For calculating elapsed time and making timestamps

from PIL import Image #for processing images
import base64 #For encoding images
from io import BytesIO #For encoding images

import math #pretty much only for rounding up numbers

from .message_manager import ImageRequest, MessageManager

#For sending and receiving messages
from .protocol import *

#Wrapper class for holding image data for ease of access
class ImageWrapper:

    #Properties
    img_names = []
    id = "" #A hash
    image_encoded = b""
    resized = False
    modified_time = 0
    worker_responsible = None

    #Elapsed time for resizing
    resize_start = None

    #Elapsed time for merging
    merge_start = None

    #Static method for creating a wrapper object from a single image
    @classmethod
    def create(cls, name : str, path : str):

        #The object to be created
        img = ImageWrapper()

        #The id and names of the images this object is composed of
        img.img_names = [name]
        img.set_id()

        #Encodes the image to base64
        #https://stackoverflow.com/questions/52411503/convert-image-to-base64-using-python-pil
        PIL_image = Image.open(path)
        img.image_encoded = ImageWrapper.encode(PIL_image)

        #The time the file was last modified. Used for sorting
        img.modified_time = os.stat(path).st_mtime

        return img

    #Combines the different names into an id
    def set_id(self) -> str:
        self.id = str(hash("".join(self.img_names)))[1:10]
    
    #If the worker is dead, clears it
    def update_worker(self):
        if self.worker_responsible != None and self.worker_responsible.change_state("DEAD"):
            self.worker_responsible = None

    #Updates the image (done when a resizing reply comes through)
    def update_image_resized(self, new_image : str):
        self.resized = True
        self.image_encoded = new_image

        #Records the completion of the task
        TaskInfo.register("RESIZE", self.img_names, self.resize_start, self.worker_responsible)

    #Gets the number of fragments this image has
    def fragment_count(self):
        return math.ceil(len(self.image_encoded) / MAX_FRAGMENT)

    #Gets an specific fragment
    def get_fragment(self, piece : int) -> bytes:
        start = piece * MAX_FRAGMENT
        return self.image_encoded[start:start + MAX_FRAGMENT]

    #Merge with it's neighbour
    def merge(self, neighbour, new_image : str):

        #Records the completion of the task
        TaskInfo.register("MERGE", [self.img_names, neighbour.img_names], self.merge_start, self.worker_responsible)

        #Merges the image's name too
        self.img_names = self.img_names + neighbour.img_names
        self.set_id()
        #self.resized = True #TODO remove?

        #Updates the image
        self.image_encoded = str.encode(new_image)

    #Convert Image to Base64 
    @classmethod
    def encode(cls, img : Image) -> str:
        buffer = BytesIO()
        img.save(buffer, format="JPEG")
        return base64.b64encode(buffer.getvalue())

    #Convert Base64 to Image 
    @classmethod
    def decode(cls, data : str) -> Image:
        buff = BytesIO(base64.b64decode(data))
        return Image.open(buff)

    #Formats for interface
    def __str__(self) -> str:
        state = ""
        if self.resized:
            state = "RESIZED"
        else:
            state = "ASSIGNED" if self.worker_responsible != None else "PENDING"

        return '{:13s} {:3s}'.format(state, ' + '.join(self.img_names))

#Keep alive stats
KEEP_ALIVE_DELAY = 1
KEEP_ALIVE_TOLERANCE = 6
TASK_CONFIRM_TOLERANCE = 3

#Class for holding worker info
class WorkerInfo:

    #Creates the worker
    def __init__(self, addr, id : int) -> None:
        self.addr = addr
        self.id = id
        self.state = "IDLE" #Can be IDLE, RESIZING, MERGING or DEAD
        self.missed_keep_alives = 0
        self.confirmation_pending = False #Whether the worker is yet to confirm it's task
    
    #Tags a dead worker as alive again
    def resurrect(self):
        self.change_state("IDLE")
        self.missed_keep_alives = 0

        #Update the interface
        self.print_interface()

    def change_state(self, new_state : str):
        self.state = new_state
        if new_state == "MERGE" or new_state == "RESIZE":
            self.confirmation_pending = 0
        else:
            self.confirmation_pending = False

    #Formats for interface
    def __str__(self) -> str:
        return '{}:{:10s} {:10s} {}'.format(self.addr[0], str(self.addr[1]), self.state, f'{self.missed_keep_alives}/{KEEP_ALIVE_TOLERANCE}')

#Class for holding task information
class TaskInfo:

    #Static list of all tasks
    history = []

    resizes_count = 0
    merges_count = 0
    
    #Creates and stores a task info in the static list
    @classmethod
    def register(cls, type : str, img_names : list, start_time : datetime, worker : WorkerInfo):
        task = TaskInfo(type, img_names, start_time, worker)
        cls.history.append(task)

        if type == "MERGE":
            cls.merges_count += 1
        else:
            cls.resizes_count += 1

    #Should use the create method
    def __init__(self, type : str, img_names : list, start_time : datetime, worker : WorkerInfo) -> None:
        self.type = type #MERGE or RESIZE
        self.timestamp = datetime.now()
        self.img_names = img_names
        self.elapsed_time = datetime.now() - start_time
        self.worker = worker
        pass

    #Gets every task, MERGE or RESIZE
    @classmethod
    def get_by_type(cls, type : str) -> list:
        res = []
        for t in cls.history:
            if t.type == type:
                res.append(t)
        return res
    
    #Gets every worker that completed a task
    #Returns a dict, where the keys are the workers and the value the amount of task it completed
    @classmethod
    def get_workers(cls) -> dict:
        res = {}
        for t in cls.history:
            if t.worker not in res.keys():
                res[t.worker] = 1
            else:
                res[t.worker] += 1
        return res

    #Formats in a way to be printed when everything's done
    def __str__(self) -> str:
        res = ''
        if self.type == "MERGE":
            res = f"Images '{' + '.join(self.img_names[0])}' and '{' + '.join(self.img_names[1])}' were merged by Worker {self.worker.id}"
        else:
            res = f"Image '{self.img_names[0]}' was resized by Worker {self.worker.id}"

        #Adds the timestamp
        time = "{:02d}:{:02d}:{:02d}".format(self.timestamp.hour, self.timestamp.minute, self.timestamp.second)
        return "{:10s} {}".format(time, res)
    pass

#Actual implementation of the Broker object
class Broker:

    #Whether the broker is running or not. When every job is done, the broker turns itself off.
    running = True

    #List of every worker (and info about them)
    workers = {}

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
        self.images = []
        for filename in os.listdir(self.path):
            f = os.path.join(self.path, filename)
            #Ignore non-image files (support only .jpg for now)
            if os.path.isfile(f) and filename.split('.')[-1] in ['jpg', 'jpeg']:
                #Creates the image and stores it the dictionary
                self.images.append(ImageWrapper.create(filename, f))

        #Sorts lists by date of last modification
        self.images.sort(key=lambda x: x.modified_time, reverse=True)

        #Print result
        self.image_count = len(self.images)
        self.put_outout_history(f"{self.image_count} image(s) found.")

    #endregion

    def start_server(self):

        #Creates the brokers's server (UDP)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        #Added this line to prevent an error message stating that the previous address was already in use
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        #Binds the server socket to an interface address and port (> 1023)
        self.sock.bind((socket.gethostname(), 1024))

        self.put_outout_history(f"Started server at port {1024}.")

        #Creates the message manager, for sendind and reciving messages
        self.message_manager = MessageManager(self.sock)


    #Serve clients countinously
    def run(self):

        #Creates a thread for sending keep alive messages
        keep_alive_thread = threading.Thread(target = self.send_keep_alive)
        keep_alive_thread.daemon = True
        keep_alive_thread.start()

        #Keeps servicing requests until the broker is shut off
        try:
            while self.running:

                #Gets message from worker
                message, worker_address = self.message_manager.receive()
                
                #Requests any pending fragments that there might have
                self.message_manager.request_fragments()

                #If message is None, then it's being handled by the message_manager. Skip
                if message == None:
                    continue

                #Creates a thread for the worker
                worker_thread = threading.Thread(target = self.handle_message, args = (message, worker_address))
                worker_thread.daemon = True
                worker_thread.start()
            
        #Shutdown the broker if the user interrupts the proccess
        except KeyboardInterrupt:
            self.poweroff()

    #Sends keep alive messages to every worker periodically, as well was detect potentially missed tasks assingment
    keep_alive_count = 0 #Used for detecting missed tasks
    def send_keep_alive(self):
        while self.running:

            #Evaluate every worker
            some_dead = False
            for worker in self.workers.values():
                if worker.state == 'DEAD':
                    continue

                worker.missed_keep_alives += 1 #Increment the missed count

                #If missed too many messages, probably dead. Delete it
                if worker.missed_keep_alives >= KEEP_ALIVE_TOLERANCE:
                    some_dead = True
                    worker.change_state("DEAD")

                    #If it had a task, remove it
                    for img in self.images:
                        if img.worker_responsible == worker:
                            img.worker_responsible = None
                            continue

                    self.put_outout_history(f"Worker {worker.id} is dead.")
                    self.assign_task() #Try to assign it again
                else:
                    self.message_manager.send(worker.addr, KeepAliveMessage())

                #If there's a task confirmation pending...
                if str(worker.confirmation_pending) != False: #Why is 0 == False in python...
                    worker.confirmation_pending += 1

                    #If waited for too long, remove the task from it
                    if worker.confirmation_pending < TASK_CONFIRM_TOLERANCE:
                        continue

                    worker.change_state("IDLE")

                    for img in self.images:
                        if img.worker_responsible == worker:
                            img.worker_responsible = None
                            continue

                    self.put_outout_history(f"Worker {worker.id} missed it's job assignment.")
                    self.assign_task() #Try to assign it again

            #Makes it so dead workers are not assigned to images
            if some_dead:
                for i in self.images:
                    i.update_worker()

            #Update interfaces
            self.print_interface()

            #Wait ten seconds
            time.sleep(KEEP_ALIVE_DELAY)
    
    #Decides what to do with the message received
    def handle_message(self, msg : Message, addr):
        if msg.type == "HELLO":
            self.handle_hello(addr)
        elif msg.type == "KEEPALIVE":
            self.handle_keep_alive(addr)
        elif msg.type == "TASKCONFIRM":
            self.handle_task_confirmation(addr)
        elif msg.type == "FRAGREQUEST":
            self.handle_fragment_request(msg, addr)
        elif msg.type == "OPREPLY":
            self.handle_operation_reply(msg, addr)

    #This message means a worker connected to the broker
    first_join = True #Bool for detecting when the first worker joins
    def handle_hello(self, addr):

        #Records the time when the first worker joined
        if self.first_join:
            self.first_join = False
            self.start_time = datetime.now()
            #self.done()
            #return

        #Either creates or resurrects the worker
        if addr not in self.workers.keys():
            self.workers[addr] = WorkerInfo(addr, len(self.workers)+1)
        else:
            self.workers[addr].resurrect()

        id = self.workers[addr].id

        #Sents hello message back, acknowledging the connection
        self.message_manager.send(addr, HelloMessage(id)) #Gives the worker it's ID

        #Update the interface
        self.put_outout_history(f"Worker {id} just joined.")

        #Gives it a task if needed
        self.assign_task()

    #Handles the keep alive by clearing the worker from suspicion for now
    def handle_keep_alive(self, addr):
        self.workers[addr].missed_keep_alives = 0

    #Handles the task confimation, to be sure that the message was not lost
    def handle_task_confirmation(self, addr):
        self.workers[addr].confirmation_pending = 0

    #Handles the request for image fragments
    def handle_fragment_request(self, msg : FragmentRequestMessage, addr):
        #Sends back the requested piece
        try: #TODO fix
            img = None
            for i in self.images:
                if i.id == msg.id:
                    img = i
                    break
            
            fragment = img.get_fragment(msg.piece)
            reply = FragmentReplyMessage(msg.id, fragment.decode('utf-8'), msg.piece)
            self.message_manager.send(addr, reply)
        except:
            pass

    #Receives the result of an operation from a worker
    def handle_operation_reply(self, msg : OperationReplyMessage, addr):

        #Sends a confirmation that it received the task
        self.message_manager.send(addr, TaskConfimationMessage())

        #Flags that the worker is done with their operation
        worker = self.workers[addr]
        worker.change_state("IDLE")

        #If it was a resize...
        if msg.operation == "RESIZE":

            #Invokes resize callback when the image is reconstructed
            self.message_manager.request_image(addr, msg.id, msg.fragments, self.resize_callback)

        #If it's a merge
        else:
            #Invokes merge callback when the image is reconstructed
            self.message_manager.request_image(addr, msg.id[0], msg.fragments, self.merge_callback, {"merge_ids" : msg.prev_ids})

    #Invoked when all the fragments of the image is collected and the image is constructed
    def resize_callback(self, request : ImageRequest):
        #Updates the image to the resized varient
        for img in self.images:
            if img.id == request.id:
                img.update_image_resized(str.encode(request.image_base64))

        self.put_outout_history(f"Worker {request.worker} is done resizing.")

        #See if there's a new task for the worker
        self.assign_task()

    #Invoked when all the fragments of the image is collected and the image is constructed
    def merge_callback(self, request : ImageRequest):
        try: #Fix

            #Get the merge ids from the request
            merge_ids = request.data["merge_ids"]

            #Get the images
            A_img = None
            B_img = None
            for img in self.images:
                if A_img != None and B_img != None:
                    break
                
                print(img.id, merge_ids[0], merge_ids[1])

                if img.id == merge_ids[0]:
                    A_img = img
                elif img.id == merge_ids[1]:
                    B_img = img

            #Merges the two images
            A_img.merge(B_img, request.image_base64)
            self.images.remove(B_img)
            self.put_outout_history(f"Worker {request.worker} is done merging.")
        except:
            pass
        #See if there's a new task for the worker
        self.assign_task()

    #Invoked whenever a task is completed or a new worker has joined. Sends tasks to workers if needed
    def assign_task(self):
        #If the images were all resized and/or merged...
        if len(self.images) == 1 and self.images[0].resized:
            #All tasks were done. All finished!
            self.done()
            return

        #If there are no idle workers, there's nothing to be done
        idle_workers = self.get_idle_workers()
        if len(idle_workers) == 0:
            return

        #Evaluate if any of the images need to be operated on
        for index, img in enumerate(self.images):
            #Don't bother if there ano idle workers
            if len(idle_workers) == 0:
                break      

            #Resize if needed
            if not img.resized and img.worker_responsible == None:
                #Assign a worker to it
                worker = idle_workers.pop()
                worker.change_state("RESIZE")
                worker.confirmation_pending = 0
                img.worker_responsible = worker
                img.resize_start = datetime.now()

                #Sends the command to the worker
                msg = ResizeRequestMessage(img.id,img.fragment_count(), self.height)
                self.message_manager.send(worker.addr, msg)

                self.put_outout_history(f"Assinging worker {worker.id} to resize {img.img_names[0]}.")
            #Look for merges
            else:
                next_img = self.images[(index + 1) % len(self.images)]

                #If it is also resized, then merge
                if next_img != img and next_img.resized:
                    #Assign a worker to it
                    worker = idle_workers.pop()
                    worker.change_state("MERGE")
                    worker.confirmation_pending = 0
                    img.worker_responsible = worker
                    next_img.worker_responsible = worker
                    img.merge_start = datetime.now()

                    #Asks a worker to merge it
                    msg = MergeRequestMessage((img.id, next_img.id), (img.fragment_count(), next_img.fragment_count()))
                    self.message_manager.send(worker.addr, msg)

                    self.put_outout_history(f"Assinging worker {worker.id} to merge two images.")


    #Returns a list of all workers without tasks
    def get_idle_workers(self) -> list:
        res = []

        for w in self.workers.values():
            if w.state == 'IDLE':
                res.append(w)

        return res

    #Shutdown the broker and workers
    def poweroff(self):

        #Shutdown all workers
        msg = DoneMessage()
        for w in self.workers.values():
            if w.state != "DEAD":
                self.message_manager.send(w.addr, msg)

        #Shutdown itself
        self.running = False
        self.sock.close()

    #region INTERFACE

    #Invoked when all images are resized and merged together.
    #Prints stats to terminal
    def done(self):
        
        #Calculate some stats
        elapsed_time = (datetime.now() - self.start_time)

        resizes = TaskInfo.get_by_type("RESIZE")
        merges = TaskInfo.get_by_type("MERGE")
        workers = TaskInfo.get_workers()

        mean_resizes_count = len(resizes)/len(workers)
        mean_merges_count = len(merges)/len(workers)

        time_resize_min = None
        time_resize_max = None
        time_resize_mean = None
        for r in resizes:
            #Get min
            if time_resize_min == None or r.elapsed_time < time_resize_min:
                time_resize_min = r.elapsed_time
            #Get max
            if time_resize_max == None or r.elapsed_time > time_resize_max:
                time_resize_max = r.elapsed_time
            #Get sum
            if time_resize_mean == None:
                time_resize_mean = r.elapsed_time
            else:
                time_resize_mean += r.elapsed_time
        #Get the mean from the sum
        time_resize_mean /= len(resizes)

        time_merge_min = None
        time_merge_max = None
        time_merge_mean = None
        for r in merges:
            #Get min
            if time_merge_min == None or r.elapsed_time < time_merge_min:
                time_merge_min = r.elapsed_time
            #Get max
            if time_merge_max == None or r.elapsed_time > time_merge_max:
                time_merge_max = r.elapsed_time
            #Get sum
            if time_merge_mean == None:
                time_merge_mean = r.elapsed_time
            else:
                time_merge_mean += r.elapsed_time
        #Get the mean from the sum
        time_merge_mean /= len(merges)

        #TODO: Se uma task falhar e for reiniciada por outro worker, o tempo deve resetar?

        #PRINTS STATS
        os.system("cls||clear") #Clears on both windows and linux
        #Broker info
        print("BROKER\n"+"-"*50)  
        print(f"Address: {socket.gethostname()}")
        print("Port: 1024")

        print("\nALL DONE!")  
        print("\nSTATISTICS:\n"+"-"*50)  
        print('{:35s} {}'.format("Total resize count:", len(resizes)))
        print('{:35s} {}'.format("Total merge count:", len(merges)))

        print('{:35s} {:.1f}'.format("Mean resizes per worker:", mean_resizes_count))
        print('{:35s} {:.1f}'.format("Mean merges per worker:", mean_merges_count))

        print('{:35s} {}    {}    {}'.format("Min, max and mean resize time:", time_resize_min, time_resize_max, time_resize_mean))
        print('{:35s} {}    {}    {}'.format("Min, max and mean merge time:", time_merge_min, time_merge_max, time_merge_mean))

        print('{:35s} {}'.format("Total elapsed time:", str(elapsed_time)))

        print("\nALL WORKERS:\n"+"-"*50)  
        for w in self.workers.values():

            task_count = "(Did no tasks)"
            if w in workers.keys():
                task_count = f"({workers[w]} task(s))"

            print(f'{str(w.id)}.\t{(w.addr[0]+":"+str(w.addr[1]))} {task_count}')
        print("\n*Not every worker was necessarily alive up until the end.")  

        print("\nALL TASKS:\n"+"-"*50)  
        for t in reversed(TaskInfo.history):
            print(t)   
        print("\n*Tasks given but not finished by the worker are not listed.")  

        #Opens the final image and saves to disk
        final_image = ImageWrapper.decode(self.images[0].image_encoded)
        final_image.show()
        print("\nOpening and saving the final image to disk.")  

        #Power off and tells workers to power off too
        print("\nPowering down...")  
        self.poweroff()

        pass

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

        #Broker info
        print("BROKER\n"+"-"*50)  
        print(f"Address: {socket.gethostname()}")
        print("Port: 1024")

        #Image window
        print("\nIMAGES")
        total_char = 30
        percentage = (TaskInfo.merges_count+TaskInfo.resizes_count)/((self.image_count*2)-1)
        progress_bar = "█"*int(total_char*percentage)+"░"*int(total_char*(1-percentage))
        if percentage > 0:
            print(f"{progress_bar} {int(100*percentage)}%")
        print("-"*50)
        for id, img in enumerate(self.images):
            print(f'{id+1}.\t{img}')

        #Worker window
        print("\nWORKERS")
        print('{}\t{:20s} {:10s} {}'.format("Id", "Address", "Status", "Keep Alive"))
        print("-"*50)
        for id, worker in enumerate(self.workers.values()):
            print(f'{id+1}.\t{worker}')
        if len(self.workers) == 0:
            print("No workers connected.")

        #Output window
        print("\nOUTPUT\n"+"-"*50)
        print("\n".join(self.output))
        pass

    #endregion