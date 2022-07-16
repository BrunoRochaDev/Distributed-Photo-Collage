from src.broker import Broker #The actual broker implementation
import argparse #For parsing command line arguments

#Creates the broker. Receives a image path and desired height as arguments.
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="broker for the distributed image merger by bruno rocha moura")

    #Image directory path
    parser.add_argument("path", type=str, help="absolute path to the image directory")

    #The desired image height
    parser.add_argument("height", type=int, help="desired height for the merged image in pixels (whole number)")

    #Whether to display the UI or not
    parser.add_argument("--ui", action='store_true', help="formats the output. doesn't work well when outputs happen too fast")

    args = parser.parse_args()

    if args.height <= 0:
        parser.error("the desired height must be greater than zero")
    Broker(args.path, args.height, args.ui)