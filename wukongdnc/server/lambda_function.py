import json
import uuid
from threading import Thread
import cloudpickle 
import base64 
import socket
import uuid 

from ..constants import TCP_SERVER_IP

from .counting_semaphore import CountingSemaphore
from .state import State
from .util import make_json_serializable, decode_and_deserialize

"""
Lambda Client
-------------

This is the version of the client code that I've been running within an AWS Lambda function.

I've updated it to keep it consistent with the changes we've been making.
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
    print("Will be sending a message of size %d bytes." % len(obj))
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

def bounded_buffer_task(taskID, function_name, websocket):
    state = State(ID = function_name)
    #with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
    #    websocket.connect(TCP_SERVER_IP)
    msg_id1 = str(uuid.uuid4())
    print(taskID + " calling synchronize_async PC: " + str(state._ID) + ". Message ID=" +msg_id1)

    state.keyword_arguments = {
        "ID": taskID,
        "value": "hello, world!"        # Used by the bounded buffer test.
    }
    state._pc = 2
    message1 = {
        "op": "synchronize_async", 
        "name": "b", 
        "method_name": "deposit",       # Bounded buffer test.
        "state": make_json_serializable(state),
        "id": msg_id1
    }
    print("Calling 'synchronize_async' - 'deposit' on the server. MessageID=" + msg_id1)
    msg1 = json.dumps(message1).encode('utf-8')
    send_object(msg1, websocket)
    print(taskID + " called synchronize_async, PC: " + str(state._ID))

    msg_id2 = str(uuid.uuid4())
    message2 = {
        "op": "synchronize_sync", 
        "name": "b", 
        "method_name": "withdraw",       # Bounded buffer test.
        "state": make_json_serializable(state),
        "id": msg_id2
    }

    print("Calling 'synchronize_sync' - 'withdraw' on the server. MessageID=" + msg_id2)
    msg2 = json.dumps(message2).encode('utf-8')
    send_object(msg2, websocket)
    print(taskID + " called synchronize_sync, PC: " + str(state._ID))        

    data = recv_object(websocket)               # Should just be a serialized state object.
    state_from_server = cloudpickle.loads(data) # `state_from_server` is of type State

    return_value = state_from_server.return_value
    state = state_from_server
    print(str(return_value)) 
    print("=== FINISHED ===")
    websocket.shutdown(socket.SHUT_RDWR)
    websocket.close()            

def try_wait_b_task(taskID, function_name):
    state = State(ID = function_name)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
        websocket.connect(TCP_SERVER_IP)
        msg_id = str(uuid.uuid4())
        print(taskID + " calling synchronize PC: " + str(state._ID) + ". Message ID=" +msg_id)

        state.keyword_arguments = {
            "ID": taskID
        }
        state._pc = 2
        message = {
            "op": "synchronize_sync", 
            "name": "b", 
            "method_name": "try_wait_b", 
            "state": make_json_serializable(state),
            "id": msg_id
        }
        print("Calling 'synchronize' on the server.")
        msg = json.dumps(message).encode('utf-8')
        send_object(msg, websocket)
        print(taskID + " called synchronize PC: " + str(state._ID))

        data = recv_object(websocket)               # Should just be a serialized state object.
        state_from_server = cloudpickle.loads(data) # `state_from_server` is of type State
        blocking = state_from_server.blocking

        if blocking:
            print("Blocking is true. Terminating.")
            websocket.shutdown(socket.SHUT_RDWR)
            websocket.close()
            return 
        else:
            return_value = state_from_server.return_value
            state = state_from_server
            print(str(return_value)) 
            print("=== FINISHED ===")
            websocket.shutdown(socket.SHUT_RDWR)
            websocket.close()            

def init_state(state):
    """
    Initialize state.
    """
    pass 

def lambda_handler(event, context):
    """
    This is the function that runs when the AWS Lambda starts.

    This is like our main method.
    Arguments:
    ----------
        context (AWS Context object):
            See https://docs.aws.amazon.com/lambda/latest/dg/python-context.html

        event (dict):
            Invocation payload passed by whoever/whatever invoked us.
    """
    print("event: " + str(event))
    state = decode_and_deserialize(event["state"])
    do_create = event["do_create"]
    task_id = state.task_id 
    print("Task %s has started executing." % task_id)

    if not state.restart:
        init_state(state) # Initialize the state variables one time.
    else:
        print("Restart is true. Exiting now.")
        print("state._pc = %d" % state._pc)
        return 

    function_name = context.function_name
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
        print("Connecting to " + str(TCP_SERVER_IP))
        websocket.connect(TCP_SERVER_IP)
        print("Succcessfully connected!")
        msg_id = str(uuid.uuid4())

        if do_create:
            print("Sending 'create' message to server. Message ID=" + msg_id)

            #state.keyword_arguments = {"n": 2}
            state.keyword_arguments = {"n": 1}
            message = {
                "op": "create", 
                #"type": "Barrier", 
                "type": "BoundedBuffer", 
                "name": "b", 
                "function_name": function_name,
                "state": make_json_serializable(state),
                "id": msg_id
            }
            
            msg = json.dumps(message).encode('utf-8')
            send_object(msg, websocket)
            
            print("Sent 'create' message to server")

            # Receive data. This should just be an ACK, as the TCP server will 'ACK' our create() calls.
            ack = recv_object(websocket)

            # Just call this directly.
            #try_wait_b_task(str(1), function_name)
            bounded_buffer_task(str(1), function_name, websocket)
        else:
            print("Skipping call to create.")
            #try_wait_b_task(str(2), function_name)
            bounded_buffer_task(str(1), function_name, websocket)

        # try:
        #     print("Starting client thread1")
        #     t1 = Thread(target=try_wait_b_task, args=(str(1),function_name,), daemon=True)
        #     t1.start()
        # except Exception as ex:
        #     print("[ERROR] Failed to start client thread1.")
        #     print(ex)

        # t1.join()

        # try:
        #     print("Starting client thread2")
        #     t2 = Thread(target=try_wait_b_task, args=(str(2), function_name,), daemon=True)
        #     t2.start()
        # except Exception as ex:
        #     print("[ERROR] Failed to start client thread2.")
        #     print(ex)
        
        # t2.join()
        websocket.shutdown(socket.SHUT_RDWR)
        websocket.close() 
        
    return {
        'statusCode': 200,
        'body': json.dumps("Hello, world!")
    }
