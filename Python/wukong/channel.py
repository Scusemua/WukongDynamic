import multiprocessing

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
    
    def send1(self, result):
        """
        Blocking.
        """        
        self.queue1.put(result)
    
    def send2(self, result):
        """
        Blocking.
        """
        self.queue2.put(result)
    
    def rcv1(self):
        return self.queue1.get()

    def rcv2(self):
        return self.queue2.get()        