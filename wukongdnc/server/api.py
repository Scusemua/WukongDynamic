#import sys
#import threading
import uuid 
import socket 
import cloudpickle
#import base64
import json 
#import redis 
import threading
#import time

#from threading import Thread 

from .util import make_json_serializable
from .state import State 
from ..dag.DAG_executor_State import DAG_executor_State
#from ..constants import TCP_SERVER_IP

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
"""
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.propagate = False
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
    #logger.debug("send_object: Will be sending a message of size %d bytes." % len(obj))
    
    thread_name = threading.current_thread().name  
    # First, we send the number of bytes that we're going to send.
    logger.error(thread_name + ": send_object: len obj: " + str(len(obj)))
    # send_object: len obj: 278522 needs 3 bytes
    #time.sleep(0.6)
    websocket.sendall(len(obj).to_bytes(4, byteorder='big'))
    logger.error("length of: len(obj).to_bytes(4, byteorder='big'): " + str(len(len(obj).to_bytes(4, byteorder='big'))))
    #time.sleep(0.6)
    # Next, we send the serialized object itself. 
    logger.error(thread_name + ": send_object: len obj: " + str(len(obj)))
    websocket.sendall(obj)
    logger.error(thread_name + ": sent object:") 

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

    thread_name = threading.current_thread().name
    logger.error(thread_name + ": do receive") 
    data = bytearray()
    while len(data) < 4:
        new_data = websocket.recv(4 - len(data)).strip()
        logger.error(thread_name + ": recv_object received") 

        if not new_data:
            logger.warn("Stopped reading incoming message size from socket early. Have read " + str(len(data)) + " bytes of a total expected 4 bytes.")
            break 

        #logger.debug("recv_object: starting read %d bytes from TCP server." % len(new_data))
        data.extend(new_data)
    
    # Convert the bytes representing the size of the incoming serialized object to an integer.
    incoming_size = int.from_bytes(data, 'big')
    logger.error(thread_name + ": recv_object: Will receive another message of size %d bytes" % incoming_size)
    data = bytearray()
    logger.error(thread_name + ": created second data object, incoming_size: " + str(incoming_size))
    while len(data) < incoming_size:
        # Finally, we read the serialized object itself.
        logger.error(thread_name + ": start recv_object rcv") 
        new_data = websocket.recv(incoming_size - len(data)).strip()
        logger.error(thread_name + ": recv_object received") 

        if not new_data:
            break 

        #logger.debug("recv_object: starting read %d bytes from TCP server." % len(new_data))
        data.extend(new_data)
        #logger.debug("recv_object: end-of read %d/%d bytes from TCP server." % (len(data), incoming_size))

        logger.error(thread_name + ": returning from recv_object rcv") 

    return data 

def synchronize_sync(websocket, op, name, method_name, state):
    """
    Synchronize on the remote TCP server.

    Arguments:
    ----------
        websocket (socket.socket):
            Socket connection to the TCP server.
            TODO: We pass this in, but in the function body, we connect to the server.
                    In that case, we don't need to pass a websocket. We'll just create one.
                    We should only bother with passing it as an argument if its already connected.
        
        op (str):
            The operation being performed. 
        
        method_name (str):
            The name of the synchronization method we'd be calling on the server.

        name (str):
            The name (which serves as an identifier) of the object we're using for synchronization.
        
        state (state.State):
            Our current state.
    """
    # see the note below about closing the websocket or not
    msg_id = str(uuid.uuid4())
    message = {
        "op": op, 
        "name": name,
        "method_name": method_name,
        "state": make_json_serializable(state),
        "id": msg_id
    }
    thread_name = threading.current_thread().name
    logger.debug(thread_name + ": synchronize_sync: Fan-in ID %s calling send_object %s. Message ID=%s" % (name, op, msg_id))
    msg = json.dumps(message).encode('utf-8')
    logger.debug(thread_name + ": synchronize_sync: msg to send: " + str(msg))
    send_object(msg, websocket)
    logger.debug(thread_name + ": synchronize_sync: sent object successful, calling receive object.")
    data = recv_object(websocket)               # Should just be a serialized state object.
    logger.debug(thread_name + ": synchronize_sync: receive object succesful.")
    #logger.debug("Received %d byte return value from server: %s" % (len(data), str(data)))

    logger.debug(thread_name + ": synchronize_sync: data is " + str(data))
    logger.debug(thread_name+ ":synchronize_sync: cloudpickle.loads(data)")

    state_from_server = cloudpickle.loads(data) # `state_from_server` is of type State
    logger.debug(thread_name+ ": synchronize_sync: successful")
    #logger.debug("Fan-in ID %s received return value from server in synchronize_sync: %s" % (name, str(state_from_server.return_value)))

    return state_from_server

def synchronize_async(websocket, op, name, method_name, state):
    """
    Synchronize on the remote TCP server.

    Arguments:
    ----------
        websocket (socket.socket):
            Socket connection to the TCP server.
            TODO: We pass this in, but in the function body, we connect to the server.
                    In that case, we don't need to pass a websocket. We'll just create one.
                    We should only bother with passing it as an argument if its already connected.
        
        op (str):
            The operation being performed. 
        
        method_name (str):
            The name of the synchronization method we'd be calling on the server.

        name (str):
            The name (which serves as an identifier) of the object we're using for synchronization.
        
        state (state.State):
            Our current state.
    """
    # see the note below about closing the websocket or not
    msg_id = str(uuid.uuid4())
    message = {
        "op": op, 
        "name": name,
        "method_name": method_name,
        "state": make_json_serializable(state),
        "id": msg_id
    }
    thread_name = threading.current_thread().name
 
    #logger.debug("synchronize_async: Calling %s. Message ID=%s" % (op, msg_id))
    msg = json.dumps(message).encode('utf-8')
    logger.debug(thread_name + ": synchronize_async: send_object: length msg:" + str(len(msg)))
    send_object(msg, websocket)
    logger.debug("synchronize_async: thread " + thread_name + " send_object successful")

def synchronize_async_terminate(websocket: socket.socket, op: str, name: str, method_name: str, state: State):
    """
    Synchronize on the remote TCP server.

    Arguments:
    ----------
        websocket (socket.socket):
            Socket connection to the TCP server.
            TODO: We pass this in, but in the function body, we connect to the server.
                    In that case, we don't need to pass a websocket. We'll just create one.
                    We should only bother with passing it as an argument if its already connected.
        
        op (str):
            The operation being performed. 
        
        method_name (str):
            The name of the synchronization method we'd be calling on the server.

        name (str):
            The name (which serves as an identifier) of the object we're using for synchronization.
        
        state (state.State):
            Our current state.
    
    Return:
    -------
        state.State: Return the state object that was passed in. The `blocking` field will have been set to True.
    """
    # see the note below about closing the websocket or not
    msg_id = str(uuid.uuid4())
    state.blocking = True
    message = {
        "op": op, 
        "name": name,
        "method_name": method_name,
        "state": make_json_serializable(state),
        "id": msg_id
    }
    logger.debug("synchronize_async_terminate: Calling %s. Message ID=%s" % (op, msg_id))
    msg = json.dumps(message).encode('utf-8')
    send_object(msg, websocket)

    return state

def create(websocket, op, type, name, state):
    """
    Create a remote object on the TCP server.

    Arguments:
    ----------
        websocket (socket.socket):
            Socket connection to the TCP server.
            TODO: We pass this in, but in the function body, we connect to the server.
                    In that case, we don't need to pass a websocket. We'll just create one.
                    We should only bother with passing it as an argument if its already connected.
        
        op (str):
            The operation being performed, which is "create"
        
        type (str):
            The type of the object to be created.

        name (str):
            The name (which serves as an identifier) of the synchronization object to be created
        
        state (state.State):
            Our current state.
    """
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:
    #    logger.debug("Connecting to " + str(TCP_SERVER_IP))
    #    websocket.connect(TCP_SERVER_IP)
    #    logger.debug("Successfully connected!")

    # msg_id for debugging
    msg_id = str(uuid.uuid4())
    logger.debug("create: Sending 'create' message to server. Op='%s', type='%s', name='%s', id='%s', state=%s" % (op, type, name, msg_id, state))

    # we set state.keyword_arguments before call to create()
    message = {
        "op": op,
        "type": type,
        "name": name,
        "state": make_json_serializable(state),
        "id": msg_id
    }

    msg = json.dumps(message).encode('utf-8')
    send_object(msg, websocket)
    logger.debug("create: Sent 'create' message to server")

    # Receive data. This should just be an ACK, as the TCP server will 'ACK' our create() calls.
    ack_ignored = recv_object(websocket)

"""
# Not used: No need yet to create only a wwork queue without also creating fanins and faninNBs.
# We piggyback call to create work queue on create_all_fanins_and_faninNBs_and_possibly_work_queue
def create_work_queue_on_server(websocket, work_queue_message):

    #Create work_queue on tcp_server

    #Arguments:
    #----------
    #    websocket (socket.socket):
    #        Socket connection to the TCP server.
    #        TODO: We pass this in, but in the function body, we connect to the server.
    #                In that case, we don't need to pass a websocket. We'll just create one.
    #                We should only bother with passing it as an argument if its already connected.
        
    #    work_queue_message: tcp_server command

    msg_id = str(uuid.uuid4())
    logger.debug("api: create_work_queue_on_server: Sending 'create_work_queue' message to server. Op='%s', type='%s', name='%s', id='%s', state=%s" % (work_queue_message['op'], work_queue_message['type'], work_queue_message['name'], work_queue_message['state'], work_queue_message['msg_id']))

    msg = json.dumps(work_queue_message).encode('utf-8')
    send_object(msg, websocket)
    logger.debug("create_work_queue: Sent 'create_work_queue' message to server")

    # Receive data. This should just be an ACK, as the TCP server will 'ACK' our create() calls.
    ack = recv_object(websocket)
    #state_from_server = cloudpickle.loads(data) # `state_from_server` is of type State
    return ack
"""

def create_all_fanins_and_faninNBs_and_possibly_work_queue(websocket, op, type, name, state):
    """
    Create all fanins and faninNBs for DAG_executor on the TCP server.

    Arguments:
    ----------
        websocket (socket.socket):
            Socket connection to the TCP server.
            TODO: We pass this in, but in the function body, we connect to the server.
                    In that case, we don't need to pass a websocket. We'll just create one.
                    We should only bother with passing it as an argument if its already connected.
        
        op (str):
            The operation being performed, which is "create_all_fanins_and_faninNBs"
        
        type (str):
            The type of the object to be created.

        name (str):
            A tuple "messages" of fanin "create" messages and faninNB "create" messages
        
        state (state.State):
            Our current state.
    """
    logger.error("foo")
    msg_id = str(uuid.uuid4())
    logger.error("api: create_all_fanins_and_faninNBs_and_possibly_work_queue: Sending 'create_all_fanins_and_faninNBs' message to server. Op='%s', type='%s', id='%s', state=%s" % (op, type, msg_id, state))
    logger.error("length name: " + str(len(name)))
    # we set state.keyword_arguments before call to create()
    message = {
        "op": op,
        "type": type,
        "name": name,
        "state": make_json_serializable(state),
        "id": msg_id
    }
    logger.error("api: create_all_fanins_and_faninNBs_and_possibly_work_queue: op:" + op + " type: " + type + " state: " + str(state) + " id: " + str(id))
    """
    message0 = name[0] 
    message1 = name[1]
    m00 = message0[0]
    logger.debug("api: m01.op:" + m01.op
    m01 = message0[1]
    m02 = message0[2]
    m03 = message0[3]
    m10 = message1[0]
    m11 = message1[1]
    m12 = message1[2]
    m13 = message1[3]
    """

    msg = json.dumps(message).encode('utf-8')
    logger.error("calling send object")
    send_object(msg, websocket)
    logger.error("create_all_fanins_and_faninNBs_and_possibly_work_queue: Sent 'create_all_fanins_and_faninNBs' message to server")

    # Receive data. This should just be an ACK, as the TCP server will 'ACK' our create() calls.
    ack = recv_object(websocket)
    #state_from_server = cloudpickle.loads(data) # `state_from_server` is of type State
    return ack

def synchronize_process_faninNBs_batch(websocket, op, type, name, DAG_exec_state):
    """
    process all fanins and faninNBs for DAG_executor on the TCP server.

    Arguments:
    ----------
        websocket (socket.socket):
            Socket connection to the TCP server.
            TODO: We pass this in, but in the function body, we connect to the server.
                    In that case, we don't need to pass a websocket. We'll just create one.
                    We should only bother with passing it as an argument if its already connected.
        
        op (str):
            The operation being performed, which is "synchronize_process_faninNBs_batch"
        
        type (str):
            The type of the object to be processed

        name (str):
            Name of the operation to be performed
        
        state (state.State):
            Our current state.
    """

    thread_name = threading.current_thread().name

#rhc: async_call
    async_call = DAG_exec_state.keyword_arguments['async_call']
 
    msg_id = str(uuid.uuid4())
    logger.debug(thread_name + ": synchronize_process_faninNBs_batch: Sending synchronize_process_faninNBs_batch message to server. Op='%s', type='%s', id='%s', state=%s" % (op, type, msg_id, DAG_exec_state))

    # we set state.keyword_arguments before call to create()
    message = {
        "op": op,
        "type": type,
        "name": name,
        "state": make_json_serializable(DAG_exec_state),
        "id": msg_id
    }

    msg = json.dumps(message).encode('utf-8')
    send_object(msg, websocket)

    logger.debug(thread_name + ": synchronize_process_faninNBs_batch: Sent 'synchronize_process_faninNBs_batch' message to server")

    if not async_call:
        # synch call so get return value from tcp server
        logger.debug(thread_name + ": synchronize_process_faninNBs_batch: synchronous call so get return value.")
        data = recv_object(websocket)
        logger.debug(thread_name + ": synchronize_process_faninNBs_batch: data is " + str(data))
        state = cloudpickle.loads(data) # `state_from_server` is of type State
        logger.debug(thread_name + ": synchronize_process_faninNBs_batch: successfully unpickled")
    else: 
        # don't wait for return value from tcp server since we did not request to receive work.
        # set return_value = 0 to indicate no work is coming back
        logger.debug(thread_name + ": synchronize_process_faninNBs_batch: asynchronous call so return state with return_value 0.")
        state = DAG_executor_State(function_name = "DAG_executor", function_instance_ID = str(uuid.uuid4()))
        state.return_value = 0
        state.blocking = False

    return state



def close_all(websocket):
    """
    Call CLOSE_ALL on the TCP server.
    """
    msg_id = str(uuid.uuid4())
    message = {
        "op": "close_all",
        "id": msg_id
    }
    msg = json.dumps(message).encode('utf-8')
    send_object(msg, websocket)
    logger.debug("close_all: Sent 'close_all' message to server")    
    ack_ignored = recv_object(websocket)
