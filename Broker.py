from src.broker import Broker #The actual broker implementation
import sys #For getting arguments

#Creates the broker. Receives a image path and desired height as arguments.
if __name__ == "__main__":

    #Validates that the arguments exist
    try:
        path = sys.argv[1]
        height = int(sys.argv[2])
    except:
        raise ValueError("A Daemon must receive a directory path and a desired height as a parameter.")

    b = Broker(path, 10)