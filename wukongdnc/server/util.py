import cloudpickle 
import socketserver
import base64 

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

def make_json_serializable(obj):
    """
    Serialize and encode an object.
    """
    return base64.b64encode(cloudpickle.dumps(obj)).decode('utf-8')

def decode_and_deserialize(obj):
    """
    Decode and deserialize an object.
    """
    return cloudpickle.loads(base64.b64decode(obj))

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