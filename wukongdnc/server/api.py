import sys
import threading
import uuid 
import socket 
import cloudpickle
import base64
import json 
import redis 

from threading import Thread 

from .util import make_json_serializable
from .state import State 
from ..constants import TCP_SERVER_IP

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

logger.propagate = False

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
    logger.debug("Will be sending a message of size %d bytes." % len(obj))
    
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
    logger.debug("Will receive another message of size %d bytes" % incoming_size)
    data = bytearray()
    
    while len(data) < incoming_size:
        # Finally, we read the serialized object itself.
        new_data = websocket.recv(incoming_size - len(data)).strip()

        if not new_data:
            break 

        logger.debug("Read %d bytes from TCP server." % len(new_data))
        data.extend(new_data)
        logger.debug("Have read %d/%d bytes from TCP server." % (len(data), incoming_size))
    
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
    logger.debug("Fan-in ID %s calling %s. Message ID=%s" % (name, op, msg_id))
    msg = json.dumps(message).encode('utf-8')
    send_object(msg, websocket)
    data = recv_object(websocket)               # Should just be a serialized state object.
    logger.debug("Received %d bytes from server: %s" % (len(data), str(data)))
    state_from_server = cloudpickle.loads(data) # `state_from_server` is of type State

    logger.debug("Fan-in ID %s received return value from server in synchronize_sync: %s" % (name, str(state_from_server.return_value)))

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
    logger.debug("Calling %s. Message ID=%s" % (op, msg_id))
    msg = json.dumps(message).encode('utf-8')
    send_object(msg, websocket)

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
    logger.debug("Calling %s. Message ID=%s" % (op, msg_id))
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
    logger.debug("Sending 'create' message to server. Op='%s', type='%s', name='%s', id='%s', state=%s" % (op, type, name, msg_id, state))

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
    logger.debug("Sent 'create' message to server")

    # Receive data. This should just be an ACK, as the TCP server will 'ACK' our create() calls.
    ack = recv_object(websocket)

def create_all_fanins_and_faninNBs(websocket, op, type, name, state):
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

    msg_id = str(uuid.uuid4())
    logger.debug("Sending 'create_all_fanins_and_faninNBs' message to server. Op='%s', type='%s', id='%s', state=%s" % (op, type, msg_id, state))

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
    logger.debug("Sent 'create_all_fanins_and_faninNBs' message to server")

    # Receive data. This should just be an ACK, as the TCP server will 'ACK' our create() calls.
    ack = recv_object(websocket)

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
    logger.debug("Sent 'close_all' message to server")    
    ack = recv_object(websocket)
