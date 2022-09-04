#from multiprocessing import Manager # , Process 

from .DAG_executor_State import DAG_executor_State
import time
import socket
from wukongdnc.constants import TCP_SERVER_IP
from ..server.api import create, synchronize_async #,synchronize_sync 

def run():

    # This part is for testing the manager.Queue, uncomment to run
    """
    manager = Manager()
    #data_dict = manager.dict()
    process_work_queue = manager.Queue(2000)
    tests = 1000
    sum = 0.0
    for i in range(1, tests):
        start_time = time.time()
        process_work_queue.put(i)
        stop_time = time.time()
        duration = stop_time - start_time
        sum += duration
        if i % 50 == 0:
            print(i)
        time.sleep(0.1)
    
    avg = sum / tests
    #print ("Queue avg:" + str(avg))
    print("Queue avg: %.9f" % avg)
    """

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
        print("try to connect")
        websocket.connect(TCP_SERVER_IP)
        print("QueuevsBB: Successfully connected to TCP Server.")
        state = DAG_executor_State(            
            keyword_arguments = {
                    'n': 2000
            }
        )
        create(websocket, "create", "BoundedBuffer", "process_work_queue", state)
        tests = 1000
        sum = 0.0
        for i in range(1, tests):
            state.keyword_arguments["value"] = i
            start_time = time.time()
            synchronize_async(websocket,"synchronize_async", "process_work_queue", "deposit", state)
            stop_time = time.time()
            duration = stop_time - start_time
            sum += duration
            if i % 50 == 0:
                print(i)
            time.sleep(0.1)
    
    avg = sum / tests
    print("BB avg: %.9f" % avg)


    # Queue avg: 0.000027036
    # Queue avg: 0.000026937
    #
    # synch
    # BB avg: 0.008840431
    # async
    # BB avg: 0.000029499

if __name__ == "__main__":
    run()