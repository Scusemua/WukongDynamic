from DivideAndConquer.Python.memoization.memoization_controller import NullResult
from channel import BiChannel
from threading import Thread

from memoization_controller import BiChannelForMemoization, ChannelMap, ChannelMapLock, MemoizationRecords
from util import MemoizationMessageType

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

class MemoizationThread(Thread):
    def __init__(self):
        self.active = True 

    def disableThread(self):
        """
        Sets the `active` instance variable to False.
        """
        self.active = False 

    def run(self):
        while self.active:
            # TODO: This is normally in a try-catch with an interrupted exception (in the Java version).
            msg = BiChannelForMemoization.rcv2() 

            if (msg.messageType == MemoizationMessageType.PAIR):
                with ChannelMapLock:
                    queuePair = ChannelMap[msg.problemOrResultID]
                    queuePair.send(NullResult)
            elif (msg.messageType == MemoizationMessageType.ADDPAIRINGNAME):
                with ChannelMapLock:
                    queuePair = ChannelMap[msg.senderID]
                    queuePair.send(NullResult)
            elif (msg.messageType == MemoizationMessageType.REMOVEPAIRINGNAME):
                with ChannelMapLock:
                    queuePair = ChannelMap[msg.problemOrResultID]
                    queuePair.send(NullResult)
            elif (msg.messageType == MemoizationMessageType.PROMISEVALUE):
                r1 = MemoizationRecords[msg.memoizationLabel]
                logger.debug("MemoizationThread: r1: " + str(r2))

                with ChannelMapLock:
                    queuePromise = ChannelMap[msg.problemOrResultID]
                    queuePromise.send(NullResult)
            elif (msg.messageType == MemoizationMessageType.DELIVEREDVALUE):
                r2 = MemoizationRecords[msg.memoizationLabel]
                logger.debug("MemoizationThread: r2: " + str(r2))
            else:
                pass 