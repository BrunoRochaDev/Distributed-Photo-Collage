import os #For managing files
import errno #For error handling
import time
from turtle import width #For calculating elapsed time
from PIL import Image #For processing images
import base64 #For encoding images

#Wrapper class for holding image data for ease of access
class ImageWrapper:

    #Setup all image details from the image path
    def __init__(self, img_path : str) -> None:
        
        #Encodes the image to base64
        with open(img_path, "rb") as img:
            self.image_str = base64.b64encode(img.read()).decode('utf-8')

        #Opens the image and get dimensions
        with Image.open(img_path) as img:
            #Get the dimensions
            self.width, self.height = img.size

        #The time the file was last modified. Used for sorting
        self.modified_time = os.stat(img_path).st_mtime

    #Sets the desired height and returns whether new dimension is bigger at least 1x1
    def set_desired_height(self, desired_height : int) -> bool:
        self.desired_height = desired_height
        self.desired_width = int(desired_height * (self.width /self.height ))
        return self.desired_height > 0 and self.desired_width > 0

#Actual implementation of the Broker object
class Broker:

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

        pass

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
        self.total_height = 0
        for filename in os.listdir(self.path):
            f = os.path.join(self.path, filename)
            #Ignore non-image files (support only .jpg for now)
            if os.path.isfile(f) and filename.split('.')[-1] in ['jpg', 'jpeg']:
                #Creates the image and stores it
                img = ImageWrapper(f)
                self.images.append(img)
                self.total_height += img.height

        #If there are no images, do nothing.
        self.img_count = len(self.images)
        if self.img_count == 0:
            print("There are no valid images in the selected directory.")
            return

        #Iterate through every image and determine it's desired size
        invalid_images = 0
        for img in self.images:
            #Updates the image desired height. If too small, flags it as such
            if not img.set_desired_height(int((img.height * self.height)/self.total_height)):
                invalid_images += 1
            pass

        #The combined height must be enough so that every individual image is at least 1x1
        if invalid_images > 0:
            raise ValueError(f"Desired height is too small, {invalid_images} image(s) would be smaller than 1x1. The mininum height for this collection would be {self.calculate_min_height()}.")
        
        #Sort the images by date of last modification
        self.images.sort(key=lambda x: x.modified_time, reverse=True)

        print(f"{self.img_count} image(s) found.")

    #Calculates the minimum height iterativaly because algebra was too finicky
    def calculate_min_height(self) -> int:

        #The step for finding finding the min height. Starts at 1k
        step = 10**4
        min_height = step

        while True:
            #Try his new step
            new_height = min_height - step

            #Tests if the new size is valid for all images
            valid = True
            for img in self.images:
                if not img.set_desired_height(int((img.height * new_height)/self.total_height)):
                    valid = False

            #If the new size is valid, continue trying the next step
            if valid:
                min_height = new_height
            #If it's not valid...
            else:
                #...and the step is already at one, then we found the mininmum
                if step == 1:
                    break
                #...but the step is not one, we can look further.
                else:
                    step /= 10
        return int(min_height)