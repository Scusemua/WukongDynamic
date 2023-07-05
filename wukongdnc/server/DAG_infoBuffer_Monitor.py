from .monitor_su import MonitorSU
#from monitor_su import MonitorSU
import _thread
import time

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s]: %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)


# Controls workers when using incremental DAG generation
class DAG_infoBuffer_Monitor(MonitorSU):
    def __init__(self, monitor_name = "DAG_infoBuffer_Monitor"):
        super(DAG_infoBuffer_Monitor, self).__init__(monitor_name=monitor_name)
        # For testing, if we havn't called init() then version number will be 1
        self.current_version_DAG_info = None
        self.current_version_number_DAG_info = 1
        self._next_version=super().get_condition_variable(condition_name="_next_version")

    #def init(self, **kwargs):
    def init(self,**kwargs):
        # initialize with a DAG_info object. This will be version 1 of the DAG
        self.current_version_DAG_info = kwargs['current_version_DAG_info']
        self.current_version_number_DAG_info = self.current_version_DAG_info.get_version_number()
        # if use kwargs, it looks like:
        # self._capacity = kwargs["n"]
        # logger.info(kwargs)

    def deposit(self,**kwargs):
        # deposit a new DAG_info object. It's version number will be one more
        # than the current DAG_info object.
        # Wake up any workes waiting for the next version of the DAG. Workers
        # that finish the current version i of the DAG will wait for the next 
        # version i+1. Note: a worker may be finishing an earlier versio of 
        # the DAG. When they request a nwe version, it may be the current
        # version or an older version that they are requesting, We will give
        # them the current version, which may be newer than the version they
        # requsted. This is fine. We assume tha tthe DAG grows incrementally
        # and we add new states but do not delete old states from the DAG.
        try:
            super(DAG_infoBuffer_Monitor, self).enter_monitor(method_name="deposit")
        except Exception as ex:
            logger.error("[ERROR] Failed super(DAG_infoBuffer, self)")
            logger.error("[ERROR] self: " + str(self.__class__.__name__))
            logger.debug(ex)
            return 0

        logger.debug(" deposit() entered monitor, len(self._new_version) ="+str(len(self._next_version)))
        self.current_version_DAG_info = kwargs['new_current_version_DAG_info']
        self.current_version_number_DAG_info = self.current_version_DAG_info.get_version_number()
        #logger.debug("DAG_info to deposit: " + str(self.current_version_DAG_info))
        restart = False
        self._next_version.signal_c_and_exit_monitor()
        return 0, restart

    def withdraw(self, **kwargs):
        # request a new version of the DAG. A worker that finishes version 
        # i will request i+1. Noet that i+1 may <= current_version. If so
        # return the current version. If not, then the worker is requesting
        # the next version of the DAG, which hasn't been generated yet.
        super().enter_monitor(method_name = "withdraw")
        requested_current_version_number = kwargs['requested_current_version_number']
        logger.debug("withdraw() entered monitor, requested_current_version_number = "
            + str(requested_current_version_number) + " len(self._next_version) = " + str(len(self._next_version)))
        DAG_info = None
        restart = False
        if requested_current_version_number <= self.current_version_number_DAG_info:
            DAG_info = self.current_version_DAG_info
            logger.debug(" withdraw got " + str(DAG_info.get_value())
                + " with version number " + str(DAG_info.get_version_number()))
            super().exit_monitor()
            return DAG_info, restart
        else:
            logger.debug("withdraw waiting for version " + str(requested_current_version_number))
            self._next_version.wait_c()
            DAG_info = self.current_version_DAG_info
            # cascaded wakeup, i.e., if there are more than one worker waiting,
            # the deposit() will wakeup the first worker with its
            # signal_c_and_exit_monitor(). The firsy waitng worker will wakeup
            # the second worker here with signal_c_and_exit_monitor(); the 
            # secnd workers will wakeup the third worker wit signal_c_and_exit_monitor()
            # etc. Note that all calls to signal are using signal_c_and_exit_monitor()
            # So no worker that signals another worker will wait to reenter the monitor;
            # instead, the signalling worker will just return, The last waiting worker will 
            # call signal_c_and_exit_monitor() and since no workers are waiting on 
            # the condition or to reenter the monitor, this will have no effect 
            # other than to release mutual exclusion. Note: Any calls to deposit()
            # for verson i+1 after a previous call to deposit() for version i
            # will have to wait for all the workers who were waiting for version i 
            # when the first deposit() occurred to return with version i, before the 
            # call to deposit with version i+1 can start. These workers will just
            # make a later call to withdraw to get version i+1, but we don't
            # expect deposits to occur so soon after each other.
            #
            # Note: We could use an SC monitor in which case the second deposit
            # might be allowed to enter the monitor before waiting workers, in 
            # which case the workers would get verson i+1, which is not bad.
            #
            self._next_version.signal_c_and_exit_monitor()
            logger.debug(" withdraw got " + str(DAG_info.get_value())
                + " with version number " + str(DAG_info.get_version_number()))
            return DAG_info, restart

class Dummy_DAG_info:
    def __init__(self,value,version_number):
        self.value = value
        self.version_number = version_number

    def get_value(self):
        return self.value
    def get_version_number(self):
        return self.version_number

#Local tests
def taskD(b : DAG_infoBuffer_Monitor):
    time.sleep(3)
    DAG_info = Dummy_DAG_info("DAG_info2",2)
    keyword_arguments = {}
    keyword_arguments['new_current_version_DAG_info'] = DAG_info
    logger.debug("taskD Calling withdraw")
    b.deposit(**keyword_arguments)
    logger.debug("Successfully called deposit version 2")

def taskW1(b : DAG_infoBuffer_Monitor):
    logger.debug("taskW1 Calling withdraw")
    keyword_arguments = {}
    keyword_arguments['requested_current_version_number'] = 1
    DAG_info, restart = b.withdraw(**keyword_arguments)
    logger.debug("Successfully called withdraw, ret is " 
        + str(DAG_info.get_value()) + "," + str(DAG_info.get_version_number())
        + " restart " + str(restart))

def taskW2(b : DAG_infoBuffer_Monitor):
    logger.debug("taskW2 Calling withdraw")
    keyword_arguments = {}
    keyword_arguments['requested_current_version_number'] = 2
    DAG_info, restart = b.withdraw(**keyword_arguments)
    logger.debug("Successfully called withdraw, ret is " 
        + str(DAG_info.get_value()) + "," + str(DAG_info.get_version_number())
        + " restart " + str(restart))

def main(): 
    b = DAG_infoBuffer_Monitor(monitor_name="DAG_infoBuffer_Monitor")
    DAG_info = Dummy_DAG_info("DAG_info1",1)
    keyword_arguments = {}
    keyword_arguments['current_version_DAG_info'] = DAG_info
    b.init(**keyword_arguments)
    try:
        logger.debug("Starting D thread")
        _thread.start_new_thread(taskD, (b,))
    except Exception as ex:
        logger.debug("[ERROR] Failed to start first thread.")
        logger.debug(ex)

    try:
        logger.debug("Starting taskW1 thread")
        _thread.start_new_thread(taskW1, (b,))
    except Exception as ex:
        logger.debug("[ERROR] Failed to start taskW1 thread.")
        logger.debug(ex)

    try:
        logger.debug("Starting first taskW2 thread")
        _thread.start_new_thread(taskW2, (b,))
    except Exception as ex:
        logger.debug("[ERROR] Failed to start first taskW2 thread.")
        logger.debug(ex)

    try:
        logger.debug("Starting second taskW2 thread")
        _thread.start_new_thread(taskW2, (b,))
    except Exception as ex:
        logger.debug("[ERROR] Failed to start second taskW2 thread.")
        logger.debug(ex)

    time.sleep(4)
    logger.debug("Done sleeping")

if __name__=="__main__":
     main()
