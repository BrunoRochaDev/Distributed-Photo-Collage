from src.worker import Worker #The actual woker implementation
import argparse #For parsing command line arguments

#Creates the broker. Receives a image path and desired height as arguments.
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="worker for the distributed image merger by bruno rocha moura")

    #Optional broker address
    parser.add_argument("address", type=str, nargs="?", help="(optional) address of the broker. by default, it's localhost")

    #Broker port
    parser.add_argument("port", type=int, help="port of the broker. should be at least 1024")

    #Old computer simulator
    parser.add_argument("--s", action='store_true', help="simulates an old computer by randomly crashing / freezing the process")

    #Min wait time
    parser.add_argument('--min', type=float, help=f'the minimum sleep time for an operation in seconds, as per the simulation. default {Worker.MAX_SLEEP}')

    #Min wait time
    parser.add_argument('--max', type=float, help=f'the maximum sleep time for an operation in seconds, as per the simulation. default {Worker.MIN_SLEEP}')

    #Failure chance
    parser.add_argument('--fail', type=float, help=f'the failure odds for every operation. must be between 0.0 and 1.0, default is {Worker.FAILURE_ODDS}')

    #Whether to display the UI or not
    parser.add_argument("--ui", action='store_true', help="formats the output. doesn't work well when outputs happen too fast")

    args = parser.parse_args()

    if args.port <= 1023:
        parser.error("the port must be higher than 1023")

    min = args.min if args.min != None else 1
    max = args.max if args.max != None else 3

    if min < 0 or max < 0:
        parser.error("the wait times must be positive")

    if min > max:
        parser.error("the minimum wait time must be lesser than the maximum")

    if args.fail != None and (args.fail > 1 or args.fail < 0):
        parser.error("the failure rate must be between 0.0 and 1.0")

    Worker(args.port, args.address, args.s, args.min, args.max, args.fail, args.ui)
