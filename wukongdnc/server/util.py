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