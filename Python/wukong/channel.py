import multiprocessing
import sys

import logging
from logging import handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

fh = handlers.RotatingFileHandler("divide_and_conquer.log", maxBytes=(1048576*5), backupCount=7)
fh.setFormatter(formatter)
logger.addHandler(fh)

class UniChannel(object):
    """
        Unidirectional Channel 
            send ---------------> rcv
            Note: Working directly with references to sent message. If this is not desired, someone should make
            a copy of the message, either the sender or the receiver or the ultimate destination of the message,
            in our case, the memoization map.    
    """
    def __init__(self, id : str):
        self.id = id 
        self.queue = multiprocessing.Queue(maxsize = 1)
    
    def send(self, result):
        self.queue.put(result)
    
    def rcv(self):
        return self.queue.get()

class BiChannel(object):
    """
        Bidirectional Channel - one side does send1 and receive1 and the other side does send2 and receive2
            send1 ---------------> rcv2
            rcv1 <--------------- send2
    """

    def __init__(self, id : str):
        self.queue1 = multiprocessing.Queue(maxsize = 1)
        self.queue2 = multiprocessing.Queue(maxsize = 1)
        self.id = id
    
    def send1(self, msg, timeout = None):
        """
        Blocking. Attempt to put a message into queue1.

        Key-Word Arguments:
        -------------------
            msg (ResultType):
                The object to place into the queue.
                    
            timeout (int or float):
                Number of seconds to wait before raising the Full exception.
        """        
        self.queue1.put(msg, timeout = timeout)
    
    def send2(self, msg, timeout = None):
        """
        Blocking. Attempt to put a message into queue2.

        Key-Word Arguments:
        -------------------
            msg (ResultType):
                The object to place into the queue.
            
            timeout (int or float):
                Number of seconds to wait before raising the Full exception.
        """
        self.queue2.put(msg, timeout = timeout)
    
    def rcv1(self, timeout = None):
        """
        Attempt to get a message from queue2.

        Key-Word Arguments:
        -------------------
            timeout (int or float):
                Number of seconds to wait before raising the Empty exception.
        """        
        return self.queue2.get(timeout = timeout)

    def rcv2(self, timeout = None):
        """
        Attempt to get a message from queue1.

        Key-Word Arguments:
        -------------------
            timeout (int or float):
                Number of seconds to wait before raising the Empty exception.
        """
        return self.queue1.get(timeout = timeout)