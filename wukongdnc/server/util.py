import cloudpickle 
#import socketserver
import base64 

import logging 
logger = logging.getLogger(__name__)
"""
logger.setLevel(logging.ERROR)

formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)
"""

def isTry_and_getMethodName(name):
    """
    Check if the given name (which is the name of a method) begins with try. 
    """
    if name.startswith("try_"):
        return name[4:], True
    return name, False

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

def decode_base64(original_data, altchars=b'+/'):
    """Decode base64, padding being optional.

    :param data: Base64 data as an ASCII byte string
    :returns: The decoded byte string.
    """
    original_data += b'==='
    return base64.b64decode(original_data, altchars)

def isSelect(name):
    """
    Check if the given name (which is the name of a method) ends with "_Select". 
    """
    if name.endswith("_Select"):
        logger.info("isSelect(" + name + "): true")
        return True
    logger.info("isSelect(" + name + "): false")
    return False