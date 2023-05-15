import logging
import logging.handlers

# "Because you'll want to define the logging configurations for listener and workers, the
# listener and worker process functions take a configurer parameter which is a callable
# for configuring logging for that process. These functions are also passed the queue,
# which they use for communication.
#
# In practice, you can configure the listener however you want, but note that in this
# simple example, the listener does not apply level or filter logic to received records.
# In practice, you would probably want to do this logic in the worker processes, to avoid
# sending events which would be filtered out between processes.""
#
# The size of the rotated files is made small so you can see the results easily.
def listener_configurer():

    root = logging.getLogger()
    #h = logging.handlers.RotatingFileHandler('mptest.log', 'a', 300, 10)
    h = logging.handlers.RotatingFileHandler('mptestNew.log', 'w',50000)
    #h = logging.handlers.FileHandler('mp.log', 'a')
    #h = logging.FileHandler('mp.log', 'a')
    f = logging.Formatter('[%(asctime)s] [%(module)s] [%(processName)s] [%(threadName)s] [%(name)s] [%(levelname)s]: %(message)s')
    #Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')
    h.setFormatter(f)
    root.addHandler(h)
    
    # In theory, this code will set the logging level for ALL loggers.
    # https://stackoverflow.com/q/54036637
    # get Pylint error about root logger - just turning error off since:
    # "This is because Pylint is not smart enough to grasp that you'll get MyLogger 
    #  instances on calling getLogger."
    # https://stackoverflow.com/questions/20965287/instance-of-rootlogger-has-no-trace-member-but-some-types-could-not-be-infe

""" 
    # pylint: disable=E1103
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    # pylint: enable=E1103

    for logger in loggers:
        logger.setLevel(logging.DEBUG) # Specify the log level here.
"""

"""
class logging.handlers.RotatingFileHandler(filename, mode='a', maxBytes=0, backupCount=0, encoding=None, 
delay=False, errors=None)
Returns a new instance of the RotatingFileHandler class. The specified file is opened and used as 
the stream for logging. If mode is not specified, 'a' is used. If encoding is not None, it is used to 
open the file with that encoding. If delay is true, then file opening is deferred until the first call 
to emit(). By default, the file grows indefinitely. If errors is provided, it determines how encoding 
errors are handled.

You can use the maxBytes and backupCount values to allow the file to rollover at a predetermined size. 
When the size is about to be exceeded, the file is closed and a new file is silently opened for output. 
Rollover occurs whenever the current log file is nearly maxBytes in length; but if either of maxBytes 
or backupCount is zero, rollover never occurs, so you generally want to set backupCount to at least 1, 
and have a non-zero maxBytes. When backupCount is non-zero, the system will save old log files by 
appending the extensions ‘.1’, ‘.2’ etc., to the filename. For example, with a backupCount of 5 and a 
base file name of app.log, you would get app.log, app.log.1, app.log.2, up to app.log.5. The file being 
written to is always app.log. When this file is filled, it is closed and renamed to app.log.1, and if 
files app.log.1, app.log.2, etc. exist, then they are renamed to app.log.2, app.log.3 etc. respectively.
"""

# This is the listener process top-level loop: wait for logging events
# (LogRecords)on the queue and handle them, quit when you get a None for a
# LogRecord.
def listener_process(queue, configurer):
    configurer()
    while True:
        try:
            record = queue.get()
            if record is None:  # We send this as a sentinel to tell the listener to quit.
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        except Exception:
            import sys, traceback
            print('Problem:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

# Arrays used for random selections in this demo

#LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING,
#          logging.ERROR, logging.CRITICAL]

LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING,
          logging.ERROR, logging.CRITICAL]

LOGGERS = ['a.b.c', 'd.e.f']

MESSAGES = [
    'Random message #1',
    'Random message #2',
    'Random message #3',
]

# Note that on Windows you can't rely on fork semantics, so each process
# will run the logging configuration code when it starts.
def worker_configurer(queue):
    h = logging.handlers.QueueHandler(queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(logging.DEBUG)

# This is the worker process top-level loop, which just logs ten events with
# random intervening delays before terminating.
# The print messages are just so you know it's doing something!

from random import choice, random
import time

def worker_process(queue, configurer):
    configurer(queue)
    name = multiprocessing.current_process().name
    print('Worker started: %s' % name)
    for i in range(10):
        time.sleep(random())
        logger = logging.getLogger(choice(LOGGERS))
        level = choice(LEVELS)
        message = choice(MESSAGES)
        logger.log(level, message)
    print('Worker finished: %s' % name)

import multiprocessing

def main():
    queue = multiprocessing.Queue(-1)
    listener = multiprocessing.Process(target=listener_process,
                                       args=(queue, listener_configurer))
    listener.start()
    workers = []
    for i in range(10):
        worker = multiprocessing.Process(target=worker_process,
                                         args=(queue, worker_configurer))
        workers.append(worker)
        worker.start()
    for w in workers:
        w.join()
    queue.put_nowait(None)
    listener.join()

if __name__ == '__main__':
    main()
