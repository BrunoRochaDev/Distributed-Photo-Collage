from heapq import merge
import os #For managing files
import errno #For error handling

import socket #For creating websockets
import sys #For closing the app
import threading #For parallelism

import time #For sleeping
from datetime import datetime #For calculating elapsed time and making timestamps

from PIL import Image #for processing images
import base64 #For encoding images
from io import BytesIO #For encoding images

import math #pretty much only for rounding up numbers

import itertools #For auto incrementing IDs

from .message_manager import ImageRequest, MessageManager

#For sending and receiving messages
from .protocol import *

#Keep alive stats
KEEP_ALIVE_DELAY = 1
KEEP_ALIVE_TOLERANCE = 6
TASK_CONFIRM_TOLERANCE = 3

#Wrapper class for holding image data for ease of access
class ImageWrapper:

    @classmethod
    def create(cls, name : str, path : str) -> object:
        img = ImageWrapper()

        img.name = name

        #Encodes the image to base64
        #https://stackoverflow.com/questions/52411503/convert-image-to-base64-using-python-pil
        PIL_image = Image.open(path)
        img.image_encoded = ImageWrapper.encode(PIL_image)

        img.modification_time = os.stat(path).st_mtime
        img.resized = False
        img.merged = None #Pointer to a merged image in which this is a part of

        img.task = None #Not associated with any tasks when created

        return img

    #The maximum size of name in characters
    MAX_NAME = 40
    def get_name(self) -> str:
        return (self.name[:self.MAX_NAME] + '[...]') if len(self.name) > self.MAX_NAME else self.name

    def get_encoded(self) -> str:
        return self.merged.get_encoded() if self.merged != None else self.image_encoded

    #Updates the image (done when a resizing reply comes through)
    def update_image_resized(self, new_image : str):
        self.resized = True
        self.image_encoded = new_image

    #Gets the number of fragments this image has
    def fragment_count(self):
        return math.ceil(len(self.image_encoded) / MAX_FRAGMENT)

    #Gets an specific fragment
    def get_fragment(self, piece : int) -> bytes:
        start = piece * MAX_FRAGMENT
        return self.image_encoded[start:start + MAX_FRAGMENT]

    #Merges images together
    #In actuallity, it just points to a new image
    @classmethod
    def merge(self, images : list, new_image : str):

        merged_image = ImageWrapper()

        merged_image.name = ' + '.join(img.name for img in images)

        merged_image.resized = True
        merged_image.task = None
        merged_image.merged = None

        merged_image.image_encoded = str.encode(new_image)

        for img in images:
            img.merged = merged_image

    #Recursivaly finds terminal image
    def get_terminal_images(self) -> object:
        if self.merged == None:
            return self
        else:
            return self.merged.get_terminal_images()

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
        if self.merged != None:
            state = "MERGED"
        elif self.resized:
            state = "RESIZED"
        else:
            state = "ASSIGNED" if self.task != None else "PENDING"

        return '{:13s} {:3s}'.format(state, self.get_name())

#Class for holding worker info
class WorkerInfo:

    #For statistics
    dead_worker_count = 0

    #Creates the worker
    def __init__(self, addr, id : int) -> None:
        self.addr = addr
        self.id = id
        self.state = "IDLE" #Can be IDLE, RESIZING, MERGING or DEAD
        self.missed_keep_alives = 0
        self.task_count = 0
        self.task = None #Starts with no task
    
    def kill(self):
        #If it had a task, remove it
        if self.task != None:
            self.task.denied()

        self.state = "DEAD"

        type(self).dead_worker_count += 1

    #Should NOT be called manually, it's called automatically when a task instance is created
    def assign_task(self, task):
        self.task = task

        if task == None:
            self.state = "IDLE"
        else:
            self.state = task.type

    #Formats for interface
    def __str__(self) -> str:
        return '{}:{:10s} {:10s} {}'.format(self.addr[0], str(self.addr[1]), self.state, f'{self.missed_keep_alives}/{KEEP_ALIVE_TOLERANCE}')

class Task:

    #Static list of all COMPLETED tasks
    history = {}

    current_tasks = {}

    id_iter = itertools.count()

    merge_count = 0
    resize_count = 0

    @classmethod
    def register(cls, type : str, images : tuple, worker : WorkerInfo) -> object:
        task = Task(type, images, worker)
        cls.current_tasks[task.id] = task
        return task

    def __init__(self, type : str, images : tuple, worker : WorkerInfo) -> None:

        self.id = next(Task.id_iter) #Thread safe
        self.timestamp = datetime.now()

        self.type = type

        #Assign itself as the image's task
        self.images = images
        for img in self.images:
            img.task = self

        #Assign itself as the worker's task
        self.worker = worker
        self.worker.assign_task(self)

        self.start_time = datetime.now()

        self.confirmed = False
        self.missed_confirmations = 0
        self.completed = False

    def confirm(self):
        self.confirmed = True

    #Returns whether the task was denied or not
    def missed_confirm(self) -> bool:
        self.missed_confirmations += 1

        if self.missed_confirmations > TASK_CONFIRM_TOLERANCE:
            self.denied()

    def denied(self):
        self.confirmed = False
        self.worker.assign_task(None)
        for img in self.images:
            img.task = None

    def complete(self):
        self.complete = True
        self.elapsed_time = datetime.now() - self.start_time

        if self.type == "MERGE":
            type(self).merge_count += 1
        else:
            type(self).resize_count += 1

        #Frees the workers and images
        self.worker.assign_task(None)
        self.worker.task_count += 1
        for img in self.images:
            img.task = None

        type(self).history[self.id] = self

    #Gets every task, MERGE or RESIZE
    @classmethod
    def get_by_type(cls, type : str) -> list:
        res = []
        for t in cls.history.values():
            if t.type == type:
                res.append(t)
        return res

    #Gets every worker that completed a task
    #Returns a dict, where the keys are the workers and the value the amount of task it completed
    @classmethod
    def get_workers(cls) -> dict:
        res = {}
        for t in cls.history.values():
            if t.worker not in res.keys():
                res[t.worker] = 1
            else:
                res[t.worker] += 1
        return res

    #Formats in a way to be printed when everything's done
    def __str__(self) -> str:
        res = ''
        if self.type == "MERGE":
            res = f"Worker {self.worker.id} merged '{self.images[0].get_name()}' with {self.images[1].get_name()}"
        else:
            res = f"Worker {self.worker.id} resized '{self.images[0].get_name()}'"

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
    def __init__(self, path : str, height : int, format_output : bool = True) -> None:

        #Formats the output or not
        self.format_output = format_output

        #Setup the images before anything else
        self.path = path
        self.height = height
        self.image_count = 0
        self.images = []
        self.setup_images()

        #Start the server after images are validated
        self.start_server()

        #Wait for messages continuously
        self.run()

    #region IMAGE MANAGEMENT

    #Makes note of each valid image in the directory
    def setup_images(self):
        self.output("Setting up images...")
        if self.format_output:
            self.print_interface()

        #Throw exception if directory doesn't exist
        if(not os.path.isdir(self.path)):
            raise FileNotFoundError( errno.ENOENT, os.strerror(errno.ENOENT), self.path)

        #The combined height must be a positive whole number.
        if self.height <= 0:
            raise ValueError(f"Desired height must be a positive integer.")
        pass

        #Iterates through the files in the images directory and creates the image object
        for filename in os.listdir(self.path):
            f = os.path.join(self.path, filename)
            #Ignore non-image files (support only .jpg for now)
            if os.path.isfile(f) and filename.split('.')[-1] in ['jpg', 'jpeg']:
                #Creates the image and stores it the dictionary
                self.images.append(ImageWrapper.create(filename, f))

        #Sorts lists by date of last modification
        self.images.sort(key=lambda x: x.modification_time, reverse=True)

        #Print result
        self.image_count = len(self.images)
        self.output(f"{self.image_count} image(s) found.")

    #endregion

    def start_server(self):

        #Creates the brokers's server (UDP)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        #Added this line to prevent an error message stating that the previous address was already in use
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        #Binds the server socket to an interface address and port (> 1023)
        self.sock.bind((socket.gethostname(), 1024))

        self.output(f"Started server at port {1024}.")

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
            for worker in self.workers.values():
                if worker.state == 'DEAD':
                    continue

                worker.missed_keep_alives += 1 #Increment the missed count

                #If missed too many messages, probably dead. Delete it
                if worker.missed_keep_alives >= KEEP_ALIVE_TOLERANCE:
                    worker.kill()

                    self.output(f"Worker {worker.id} deemed dead.")
                    self.assign_task() #Try to assign it again
                else:
                    self.message_manager.send(worker.addr, KeepAliveMessage())

                #If there's a task confirmation pending...
                if worker.task != None and worker.task.confirmed == False:
                    denied = worker.task.missed_confirm()
                    
                    if denied:
                        self.output(f"Worker {worker.id} missed it's job assignment.")
                        self.assign_task() #Try to assign it again

            #Update interfaces
            if self.format_output:
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

        #Either creates or resurrects the worker
        self.workers[addr] = WorkerInfo(addr, len(self.workers)+1)

        id = self.workers[addr].id

        #Sents hello message back, acknowledging the connection
        self.message_manager.send(addr, HelloMessage(id)) #Gives the worker it's ID

        #Update the interface
        self.output(f"Worker {id} just joined.")

        #Gives it a task if needed
        self.assign_task()

    #Handles the keep alive by clearing the worker from suspicion for now
    def handle_keep_alive(self, addr):
        self.workers[addr].missed_keep_alives = 0

    #Handles the task confimation, to be sure that the message was not lost
    def handle_task_confirmation(self, addr):
        self.workers[addr].task.missed_confirmations = 0

    #Handles the request for image fragments
    def handle_fragment_request(self, msg : FragmentRequestMessage, addr):
        #Sends back the requested piece

        #It can be either a list or int
        if type(msg.id) == list:
            task_id = msg.id[0]
            image_index = msg.id[1]
        else:
            task_id = msg.id
            image_index = 0

        if task_id not in Task.current_tasks.keys():
            return

        img = Task.current_tasks[task_id].images[image_index]
        
        fragment = img.get_fragment(msg.piece)
        reply = FragmentReplyMessage(msg.id, fragment.decode('utf-8'), msg.piece)
        self.message_manager.send(addr, reply)

    #Receives the result of an operation from a worker
    def handle_operation_reply(self, msg : OperationReplyMessage, addr):
        worker : WorkerInfo = self.workers[addr]

        #It can be either a list or int
        if type(msg.id) == list:
            task_id = msg.id[0]
        else:
            task_id = msg.id

        #Sends a confirmation that it received the task
        self.message_manager.send(addr, TaskConfimationMessage())

        #Ignore responses for tasks that are already completed
        if worker.task == None or task_id in Task.history.keys():
            return

        #Only accept tasks from the worker assigned to it
        if task_id not in Task.current_tasks.keys() or Task.current_tasks[task_id].worker != worker:
            self.output(f"Alert: Worker {worker.id} sent in a task that was not assigned to it.")  
            return

        #Flags that the worker is done with their operation
        worker.task.complete()

        task = Task.current_tasks[task_id]

        #If it was a resize...
        if task.type == "RESIZE":

            #Invokes resize callback when the image is reconstructed
            self.message_manager.request_image(addr, task.id, msg.fragments, self.resize_callback, {"task_id" : task.id}, worker.id)

        #If it's a merge
        else:
            #Invokes merge callback when the image is reconstructed
            self.message_manager.request_image(addr, task.id, msg.fragments, self.merge_callback, {"task_id" : task.id}, worker.id)

    #Invoked when all the fragments of the image is collected and the image is constructed
    def resize_callback(self, request : ImageRequest):
        #Updates the image to the resized varient
        task_id = request.data["task_id"]
        Task.current_tasks[task_id].images[0].update_image_resized(str.encode(request.image_base64))

        self.output(f"Worker {request.worker} is done resizing.")

        #See if there's a new task for the worker
        self.assign_task()

    #Invoked when all the fragments of the image is collected and the image is constructed
    def merge_callback(self, request : ImageRequest):

        #Get the merge ids from the request
        task_id = request.data["task_id"]

        #Get the images
        A_img : ImageWrapper = Task.current_tasks[task_id].images[0]
        B_img : ImageWrapper = Task.current_tasks[task_id].images[1]

        #Merges the two images
        ImageWrapper.merge([A_img, B_img], request.image_base64)

        self.output(f"Worker {request.worker} is done merging.")


        #See if there's a new task for the worker
        self.assign_task()

    #Invoked whenever a task is completed or a new worker has joined. Sends tasks to workers if needed
    def assign_task(self):

        #If all the images are merged...
        if all([x.merged != None for x in self.images]):
            #All tasks were done. All finished!
            self.done()

        #If there are no idle workers, there's nothing to be done
        idle_workers = self.get_idle_workers()
        if len(idle_workers) == 0:
            return

        #Evaluate if any of the images need to be operated on
        terminal_images = []
        for img in self.images: 
            img : ImageWrapper

            if img.task != None or len(idle_workers) == 0:
                break

            #Resize if needed
            if not img.resized:
                #Assign a worker to it
                worker : WorkerInfo = idle_workers.pop()

                #Creates the task
                task : Task = Task.register("RESIZE", [img], worker)

                #Sends the command to the worker
                msg = ResizeRequestMessage(task.id,img.fragment_count(), self.height)
                self.message_manager.send(worker.addr, msg)

                self.output(f"Assinging worker {worker.id} to resize '{img.get_name()}'.")
            #Look for terminal merge
            else:
                if img not in terminal_images:
                    terminal_images.append( img.get_terminal_images() )

        #Find terminal images to merge
        for index, img in enumerate(terminal_images):
            img : ImageWrapper

            if img.task != None or len(idle_workers) == 0:
                break
            
            A_image : ImageWrapper = img.get_terminal_images()
            B_image : ImageWrapper = terminal_images[(index + 1) % len(terminal_images)].get_terminal_images()

            #If it is also resized, then merge
            if B_image != A_image and B_image.resized:
                #Assign a worker to it
                worker : WorkerInfo = idle_workers.pop()

                #Creates the task
                task : Task = Task.register("MERGE", [A_image, B_image], worker)     

                #Asks a worker to merge it
                msg = MergeRequestMessage(task.id, (A_image.fragment_count(), B_image.fragment_count()))
                self.message_manager.send(worker.addr, msg)
                if worker.id == 1:
                    self.output(f"Assigning worker {worker.id} to merge '{A_image.get_name()}' and '{B_image.get_name()}'.")


    #Returns a list of all workers without tasks
    def get_idle_workers(self) -> list:
        res = []
        #Sort by number of tasks done, so that load is well balenced between workers
        workers = list(self.workers.values())
        workers.sort(key=lambda x: x.task_count, reverse=True)

        #Get the idle ones
        for w in workers:
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

        #Shutdown the socket
        self.running = False
        self.sock.close()

        sys.exit()

    #region INTERFACE

    #Invoked when all images are resized and merged together.
    #Prints stats to terminal
    def done(self):
        #Calculate some stats
        elapsed_time = (datetime.now() - self.start_time)

        resizes = Task.get_by_type("RESIZE")
        merges = Task.get_by_type("MERGE")
        workers = Task.get_workers()

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
                task_count = f"( {workers[w]} task(s) )"

            print(f'{str(w.id)}.\t{(w.addr[0]+":"+str(w.addr[1]))} {task_count}')
        print(f"\n*{WorkerInfo.dead_worker_count} worker(s) died during the proccess.")
        print("*Not every worker was necessarily alive up until the end.")  

        print("\nALL TASKS:\n"+"-"*50)  
        for t in Task.history.values():
            print(t)   
        print("\n*Tasks given but not finished by the worker are not listed.")  

        #Opens the final image and saves to disk
        final_image = ImageWrapper.decode(self.images[0].get_encoded())
        final_image.show()
        print("\nOpening and saving the final image to disk.")  

        #Power off and tells workers to power off too
        print("\nPowering down...")  
        self.poweroff()

        pass

    #Helper method for the output history queue
    OUTPUT_QUEUE_LENGTH = 20    
    output_history = ['...' for i in range(0,OUTPUT_QUEUE_LENGTH)]
    def output(self, value : str):
        #Get the current time for timestamp
        curr_time = datetime.now()
        time = "{:02d}:{:02d}:{:02d}".format(curr_time.hour, curr_time.minute, curr_time.second)

        #Put the output in the queue
        value = "{:10s} {}".format(time, value)
        for i in reversed(range(0, self.OUTPUT_QUEUE_LENGTH)):
            if i == 0:
                self.output_history[i] = value
            else:
                self.output_history[i] = self.output_history[i-1]

        #Prints as an interface or not, depending on args
        if not self.format_output:
            print(value)

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
        percentage = (Task.merge_count+Task.resize_count)/((self.image_count*2)-1)
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
        print("\n".join(self.output_history))
        pass

#endregion