import uuid
from threading import Thread
import cloudpickle 
import socket
import uuid 
import ujson
import base64

from counting_semaphore import CountingSemaphore
from state import State
from util import make_json_serializable, decode_and_deserialize

SERVER_IP = ("127.0.0.1", 25565)

"""
Local Client
------------

This is the version of the client code that I've been running locally on my Desktop. 

This has been kept up-to-date with the rest of the codebase.
"""

def send_object(obj, websocket):
    """
    Send obj to a remote entity via the given websocket.
    The TCP server uses a different API (streaming via file handles), so it's implemented differently. 
    This different API is in tcp_server.py.    

    Arguments:
    ----------
        obj (bytes):
            The object to be sent. Should already be serialized via cloudpickle.dumps().
        
        websocket (socket.socket):
            Socket connected to a remote client.
    """
    # First, we send the number of bytes that we're going to send.
    websocket.sendall(len(obj).to_bytes(2, byteorder='big'))
    # Next, we send the serialized object itself. 
    websocket.sendall(obj)

def recv_object(websocket):
    """
    Receive an object from a remote entity via the given websocket.

    This is used by clients. There's another recv_object() function in TCP server.
    The TCP server uses a different API (streaming via file handles), so it's implemented differently. 
    This different API is in tcp_server.py.

    Arguments:
    ----------
        websocket (socket.socket):
            Socket connected to a remote client.    
    """
    # First, we receive the number of bytes of the incoming serialized object.
    incoming_size = websocket.recv(2)
    # Convert the bytes representing the size of the incoming serialized object to an integer.
    incoming_size = int.from_bytes(incoming_size, 'big')
    print("Will receive another message of size %d bytes" % incoming_size)
    # Finally, we read the serialized object itself.
    return websocket.recv(incoming_size).strip()

def client_task(taskID):
    state = State(ID = "PyroTest", restart = False, task_id = "Task2")
    websocket = socket.socket()
    websocket.connect(SERVER_IP)
    msg_id = str(uuid.uuid4())
    print(taskID + " calling synchronize PC: " + str(state._ID) + ". Message ID=" +msg_id)

    state._pc = 2
    state.keyword_arguments = {"ID": taskID}
    message = {
        "op": "synchronize_sync", 
        "name": "b", 
        "method_name": "try_wait_b", 
        "state": base64.b64encode(cloudpickle.dumps(state)).decode('utf-8'),
        "id": msg_id
    }
    print("Calling 'synchronize' on the server.")
    msg = ujson.dumps(message).encode('utf-8')
    send_object(msg, websocket)
    print(taskID + " called synchronize PC: " + str(state._ID))

    data = recv_object(websocket)               # Should just be a serialized state object.
    state_from_server = cloudpickle.loads(data) # `state_from_server` is of type State
    blocking = state_from_server.blocking

    if blocking:
        print("Blocking is true. Terminating.")
        return 
    else:
        return_value = state_from_server.return_value
        state = state_from_server
        print(str(return_value)) 

def client_main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
        websocket.connect(("127.0.0.1", 25565))
        msg_id = str(uuid.uuid4())
        state = State(ID = "PyroTest", restart = False, task_id = "Task1")
        print("Sending 'create' message to server. Message ID=" + msg_id)

        state.keyword_arguments = {
            "n": 2
        }

        message = {
            "op": "create", 
            "type": "Barrier", 
            "name": "b", 
            "state": make_json_serializable(state),
            "id": msg_id
        }
        
        msg = ujson.dumps(message).encode('utf-8')
        send_object(msg, websocket) # Send to the TCP server.
        
        print("Sent 'create' message to server")

        # Receive data. This should just be an ACK, as the TCP server will 'ACK' our create() calls.
        received = str(websocket.recv(1024), "utf-8")

        try:
            print("Starting client thread1")
            t1 = Thread(target=client_task, args=(str(1),), daemon=True)
            t1.start()
        except Exception as ex:
            print("[ERROR] Failed to start client thread1.")
            print(ex)

        t1.join()

        try:
            print("Starting client thread2")
            t2 = Thread(target=client_task, args=(str(2),), daemon=True)
            t2.start()
        except Exception as ex:
            print("[ERROR] Failed to start client thread2.")
            print(ex)
        
        t2.join()

if __name__ == "__main__":
    client_main()
    
"""
Changes:
1. Above CP send synchronize pass kwargs parm [ID = taskID]
2. tcp loop action: create: change **keyword_arguments to keyword_arguments, I think
3. tcp loop action: synchronize: same kwargs logic as create()
4. method synchronize() in class Synchronizer:
    a. add state parm: def synchronize(self, method_name, state, **kwargs):
    b. get rid of:
        cb = kwargs["cb"]
        first = kwargs["first"]
    c. Change program_counter = kwargs["program_counter"] to
        program_counter = state._pc
        state.pc = 10
"""