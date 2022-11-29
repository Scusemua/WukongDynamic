"""
****************************************************
Failure: *******************************************

This is an example of a failure that occurs:

2022-10-10 10:05:59,614] [Thread-2] DEBUG: [HANDLER] call receive_object
[2022-10-10 10:05:59,614] [Thread-2] DEBUG: receive_object: Do self.rfile.read(4)
[2022-10-10 10:06:00,120] [Thread-3] DEBUG: receive_object self.rfile.read(4) successful
[2022-10-10 10:06:00,120] [Thread-3] DEBUG: recv_object int.from_bytes successful
[2022-10-10 10:06:00,120] [Thread-3] DEBUG: recv_object: Will receive another message of size 406 bytes
[2022-10-10 10:06:00,734] [Thread-3] DEBUG: recv_object: have read 406/406 bytes from remote client.
[2022-10-10 10:06:00,734] [Thread-3] DEBUG: [HANDLER] receive_object successful
[2022-10-10 10:06:00,739] [Thread-3] DEBUG: tcp_server: synchronize_sync: data is bytearray(b'\x00\x00\x01\x96{"op": "synchronize_sync", "name": "process_work_queue", "method_name": "withdraw", "state": "gAWVugAAAAAAAACMIHd1a29uZ2RuYy5kYWcuREFHX2V4ZWN1dG9yX1N0YXRllIwSREFHX2V4ZWN1dG9yX1N0YXRllJOUKYGUfZQojA1mdW5jdGlvbl9uYW1llE6MFGZ1bmN0aW9uX2luc3RhbmNlX0lElE6MB3Jlc3RhcnSUiYwCcGOUSwCMEWtleXdvcmRfYXJndW1lbnRzlH2UjAxyZXR1cm5fdmFsdWWUTowIYmxvY2tpbmeUiYwFc3RhdGWUSwB1Yi4=", "id": "834c6caa-92ca-4ab1-9a80-57ee92a6d0')
[2022-10-10 10:06:00,745] [Thread-3] ERROR: 'utf-32-be' codec can't decode bytes in position 4-7: code point not in range(0x110000)
[2022-10-10 10:06:00,745] [Thread-3] DEBUG: Error in tcp_handler
[2022-10-10 10:06:00,745] [Thread-3] DEBUG: 'utf-32-be' codec can't decode bytes in position 4-7: code point not in range(0x110000)
[2022-10-10 10:06:00,745] [Thread-3] ERROR: Traceback (most recent call last):
  File "C:\Users\benrc\Desktop\Executor\DAG\WukongDynamic\wukongdnc\server\tcp_server.py", line 55, in handle
    json_message = json.loads(data)
  File "C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.8_3.8.2800.0_x64__qbz5n2kfra8p0\lib\json\__init__.py", line 343, in loads
    s = s.decode(detect_encoding(s), 'surrogatepass')
  File "C:\Program Files\WindowsApps\PythonSoftwareFoundation.Python.3.8_3.8.2800.0_x64__qbz5n2kfra8p0\lib\encodings\utf_32_be.py", line 11, in decode
    return codecs.utf_32_be_decode(input, errors, True)
UnicodeDecodeError: 'utf-32-be' codec can't decode bytes in position 4-7: code point not in range(0x110000)

Other failures can occur, such as a message sent by the tcp_servr may not get receivd by the client.
In this case, the program just hangs. Sometimes there is an unpickle error:

File "C:\Users\benrc\Desktop\Executor\DAG\WukongDynamic\wukongdnc\server\api.py", line 144, in synchronize_sync
  state_from_server = cloudpickle.loads(data) # `state_from_server` is of type State
  _pickle.UnpicklingError: invalid load key, '\x01'.

The send/receve code is in api.py (send_object and recv_object) and tcp_server.py (at the end) 
in directory server.



********************************************************
Configuation: ******************************************
The failure occurs when the DAG_executor_driver create a child process and that 
process creates two threads to execute the DAG. There are no failures if the child process
creates a single thread. There are no failures if the DAG_executor_driver creates the two
threads. There are no failures if the DAG_executor_driver creates multiple processes and they
don't create any threads. In all of these configurations the thread code is the same. 

So it seems that the two threads created by the child process are stepping on each other. Maybe
they are sharing something (file descriptors?) in the DAG_executor_driver process, which is the 
parent process of the child process that creates the threads? Again, if the DAG_executor_driver 
(parent process) creates the threads there are no failures.



******************************************************
Code: ************************************************
The DAG_executor_driver creates the child process(es):

    if use_multithreaded_multiprocessing:
        # Config: A6
        # keep list of threads/processes in pool so we can join() them
        multithreaded_multiprocessing_process_list = []
        num_processes_created_for_multithreaded_multiprocessing = 0
        #num_processes_created_for_multithreaded_multiprocessing = create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,counter,process_work_queue,data_dict,log_queue,worker_configurer)
        num_processes_created_for_multithreaded_multiprocessing = create_multithreaded_multiprocessing_processes(num_processes_created_for_multithreaded_multiprocessing,multithreaded_multiprocessing_process_list,counter,log_queue,worker_configurer)

where create_multithreaded_multiprocessing_processes excutes in a loop this code to crate the child process:

        try:
            payload = {
            }
            process_name = "proc"+str(num_processes_created_for_multithreaded_multiprocessing + 1)
            proc = Process(target=create_and_run_threads_for_multiT_multiP, name=(process_name), args=(process_name,payload,counter,log_queue,worker_configurer,))
            proc.start()
            multithreaded_multiprocessing_process_list.append(proc)
            num_processes_created_for_multithreaded_multiprocessing += 1  

which executes create_and_run_threads_for_multiT_multiP to create the child process' threads in a loop:

       try:
            DAG_exec_state = None
            payload = {
                "DAG_executor_state": DAG_exec_state
            }
            thread_name = process_name+"_thread"+str(num_threads_created_for_multiP+1)
            thread = threading.Thread(target=DAG_executor.DAG_executor_processes, name=(thread_name), args=(payload,counter,logger,worker_configurer,))
            thread_list.append(thread)
            num_threads_created_for_multiP += 1 

Then when all threads have been created:

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()	

After this the DAG_excutor_driver does:

            for proc in multithreaded_multiprocessing_process_list:
                proc.join()

Each thread executes: DAG_executor.DAG_executor_processes() in DAG_executor.py line 1451:

This calls DAG_executor_work_loop() line 908.

which does at the beginning:

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as websocket:

        # Config: A1, A4_remote, A5, A6
        if not store_fanins_faninNBs_locally:
            logger.debug("DAG_executor " + thread_name + " connecting to TCP Server at %s." % str(TCP_SERVER_IP))
            websocket.connect(TCP_SERVER_IP)

Each thread connects successfully.

The tcp_server prints this for requests received from the DAG_executor_driver

    [2022-11-29 08:37:20,306] [Thread-4] INFO: [HANDLER] Recieved one request from 127.0.0.1
    [2022-11-29 08:37:20,306] [Thread-4] INFO: [HANDLER] Recieved one request from 54619

and this for requests received from the two threads:

    [2022-11-29 08:37:21,883] [Thread-5] INFO: [HANDLER] Recieved one request from 127.0.0.1
    [2022-11-29 08:37:21,901] [Thread-5] INFO: [HANDLER] Recieved one request from 54622

    [2022-11-29 08:37:21,941] [Thread-6] INFO: [HANDLER] Recieved one request from 127.0.0.1
    [2022-11-29 08:37:21,951] [Thread-6] INFO: [HANDLER] Recieved one request from 54623

So the socket connections seem to be separate on the server side.

Wonder if the client-side threads are stepping on each other somehow.


********************************************************
Running Program: ***************************************

From the README file:

    Next, to run the local experiment itself, execute the following commands (n order):
    'python -m wukongdnc.server.tcp_server'
    `python -m wukongdnc.dag.DAG_executor_driver`

"""
