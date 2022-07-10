import os #For managing files
import errno #For error handling
import time #For calculating elapsed time

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

        #Throw exception if directory doesn't exist
        if(not os.path.isdir(path)):
            raise FileNotFoundError( errno.ENOENT, os.strerror(errno.ENOENT), path)

        #The combined height must be enough so that every individual image is at least 1x1
        if height <= 0:
            raise ValueError(f"Desired height is too small, some images would be smaller than 1x1. The minimum size for this collection would be {0}x{0}.")
        pass