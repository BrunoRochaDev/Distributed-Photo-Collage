from src.worker import Worker #The actual woker implementation
import sys #For getting arguments

#Creates the broker. Receives a image path and desired height as arguments.
if __name__ == "__main__":

    #Validates that the arguments exist. This was made with Linux in mind
    try:
        if len(sys.argv) == 2:
            port = int(sys.argv[1])
            Worker(port)
        else:
            address = sys.argv[1]
            port = int(sys.argv[2])
            Worker(port, address)
    except:
        raise ValueError("A Worker must receive the address (optional) and the port of the Broker.")