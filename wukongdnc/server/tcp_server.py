#from re import A
import json
import traceback
import socketserver


from .synchronizer import Synchronizer
from .util import decode_and_deserialize
#from ..dag.DAG_executor_State import DAG_executor_State
from .util import decode_and_deserialize, isTry_and_getMethodName, isSelect #, make_json_serializable

# Set up logging.
import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

class TCPHandler(socketserver.StreamRequestHandler):
    def handle(self):

        #TCP handler for incoming requests from AWS Lambda functions.
        
        while True:
            logger.info("[HANDLER] Recieved one request from {}".format(self.client_address[0]))

            self.action_handlers = {
                "create": self.create_obj,
                "setup": self.setup_server,
                "synchronize_async": self.synchronize_async,
                "synchronize_sync": self.synchronize_sync,
                "close_all": self.close_all,
                # These are DAG execution operations
                "create_all_fanins_and_faninNBs_and_possibly_work_queue": self.create_all_fanins_and_faninNBs_and_possibly_work_queue,
                "synchronize_process_faninNBs_batch": self.synchronize_process_faninNBs_batch
            }
            #logger.info("Thread Name:{}".format(threading.current_thread().name))

            try:
                data = self.recv_object()

                if data is None:
                    logger.warning("recv_object() returned None. Exiting handler now.")
                    return 

                json_message = json.loads(data)
                message_id = json_message["id"]
                logger.debug("[HANDLER] Received message (size=%d bytes) from client %s with ID=%s" % (len(data), self.client_address[0], message_id))
                action = json_message.get("op", None)
                self.action_handlers[action](message = json_message)
            except ConnectionResetError as ex:
                logger.error(ex)
                logger.error(traceback.format_exc())
                return 
            except Exception as ex:
                logger.error(ex)
                logger.error(traceback.format_exc())

    def _get_synchronizer_name(self, type_name = None, name = None):
        """
        Return the key of a synchronizer object. 

        The key is a string of the form <type>-<name>.
        """
        return str(name) # return str(type_name + "_" + name)

    def create_obj(self,message = None):
        """
        Called by a remote Lambda to create an object here on the TCP server.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.debug("[HANDLER] server.create() called.")
        type_arg = message["type"]
        name = message["name"]
        state = decode_and_deserialize(message["state"])

        synchronizer = Synchronizer()

        synchronizer.create(type_arg, name, **state.keyword_arguments)
        
        synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = name)
        logger.debug("Caching new Synchronizer of type '%s' with name '%s'" % (type_arg, synchronizer_name))
        tcp_server.synchronizers[synchronizer_name] = synchronizer # Store Synchronizer object.

        resp = {
            "op": "ack",
            "op_performed": "create"
        }
        #############################
        # Write ACK back to client. #
        #############################
        logger.info("Sending ACK to client %s for CREATE operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.info("Sent ACK of size %d bytes to client %s for CREATE operation." % (len(resp_encoded), self.client_address[0]))  

    def create_all_fanins_and_faninNBs_and_possibly_work_queue(self, message = None):
        """
        Called by a remote Lambda to create an object here on the TCP server.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        
        where:
            message = {
                "op": "create_all_fanins_and_faninNBs",
                "type": "DAG_executor_fanin_or_faninNB",
                "name": messages,						# Q: Fix this? usually it's a synch object name (string)
                "state": make_json_serializable(dummy_state),
                "id": msg_id
            }
        """  
        logger.debug("[HANDLER] server.create_all_fanins_and_faninNBs() called.")
        messages = message['name']
        fanin_messages = messages[0]
        faninNB_messages = messages[1]
        logger.info(str(fanin_messages))
        logger.info(str(faninNB_messages))

        for msg in fanin_messages:
            self.create_one_of_all_objs(msg)
        logger.info("created fanins")

        for msg in faninNB_messages:
            self.create_one_of_all_objs(msg)
        logger.info("created faninNBs")


        create_work_queue = (len(messages)>2)
        logger.info("create_work_queue: " + str(create_work_queue) + " len: " + str(len(messages)))
        if create_work_queue:
            msg = messages[2]
            self.create_one_of_all_objs(msg)


        resp = {
            "op": "ack",
            "op_performed": "create_all_fanins_and_faninNBs"
        }
        #############################
        # Write ACK back to client. #
        #############################
        logger.info("Sending ACK to client %s for create_all_fanins_and_faninNBs operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.info("Sent ACK of size %d bytes to client %s for create_all_fanins_and_faninNBs operation." % (len(resp_encoded), self.client_address[0]))

    def create_one_of_all_objs(self,message = None):
        """
        Called by create_all_fanins_and_faninNBs to create an object here on the TCP server. No
        ack is sent to a client. create_all_fanins_and_faninNBs will send the ack.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.debug("[HANDLER] server.create() called.")
        type_arg = message["type"]
        name = message["name"]
        state = decode_and_deserialize(message["state"])

        synchronizer = Synchronizer()

        synchronizer.create(type_arg, name, **state.keyword_arguments)

        synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = name)
        logger.debug("Caching new Synchronizer of type '%s' with name '%s'" % (type_arg, synchronizer_name))
        tcp_server.synchronizers[synchronizer_name] = synchronizer # Store Synchronizer object.

        # Do not send ack to client - this is just one of possibly many of the creates from create_all_fanins_and_faninNBs
 
    def synchronize_process_faninNBs_batch(self, message = None):
        """
        Synchronous process all faninNBs for a given state during DAG execution.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """

        logger.debug("[HANDLER] server.synchronize_process_faninNBs_batch() called.")
        type_arg = message["type"]
        name = message["name"]
        DAG_exec_state = decode_and_deserialize(message["state"])
        faninNBs = DAG_exec_state.keyword_arguments['faninNBs']
        faninNB_sizes = DAG_exec_state.keyword_arguments['faninNB_sizes']
        result = DAG_exec_state.keyword_arguments['result']
        calling_task_name = DAG_exec_state.keyword_arguments['calling_task_name'] 
        DAG_states = DAG_exec_state.keyword_arguments['DAG_states'] 
        worker_needs_input = DAG_exec_state.keyword_arguments['worker_needs_input'] 

        got_work = False
        for name in zip(faninNBs,faninNB_sizes):
            start_state_fanin_task  = DAG_states[name]
            pass

            logger.debug("[HANDLER] server.synchronize_sync() called.")
            obj_name = name
            method_name = message['method_name']
            DAG_exec_state = decode_and_deserialize(message["state"])

            synchronizer_name = self._get_synchronizer_name(type_name = None, name = obj_name)

            logger.debug("tcp_server: synchronize_process_faninNBs_batch: Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
            synchronizer = tcp_server.synchronizers[synchronizer_name]

            if (synchronizer is None):
                raise ValueError("synchronize_process_faninNBs_batch: Could not find existing Synchronizer with name '%s'" % synchronizer_name)

            base_name, isTryMethod = isTry_and_getMethodName(method_name)
            is_select = isSelect(type_arg) # is_select = isSelect(type_arg)
    
            logger.debug("tcp_server: synchronize_process_faninNBs_batch: method_name: " + method_name + ", base_name: " + base_name + ", isTryMethod: " + str(isTryMethod))
            logger.debug("tcp_server: synchronize_process_faninNBs_batch: self._synchronizer_class_name: : " + type_arg + ", is_select: " + str(is_select))

            logger.debug("tcp_server: synchronize_process_faninNBs_batch: calling synchronizer.synchronize.")
            return_value = synchronizer.synchronize(base_name, DAG_exec_state, **DAG_exec_state.keyword_arguments)

            # look at return value and worker_needs_input, etc
            if worker_needs_input:
                if not got_work:
                    if return_value != 0:
                        got_work = True
                        DAG_exec_state.return_value = return_value
                        DAG_exec_state.blocking = False 
                        logger.debug("tcp_server: synchronize_process_faninNBs_batch: %s sending return_value %s back for method %s." % (synchronizer_name, str(return_value), method_name))
                        logger.debug("tcp_server: synchronize_process_faninNBs_batch: %s sending state %s back for method %s." % (synchronizer_name, str(DAG_exec_state), method_name))

#ToDo/Note: Need start state of returned work

        """ where faninNB_remotely_batch is:
        DAG_exec_state = synchronize_process_faninNBs_batch(websocket, "synchronize_process_faninNBs_batch", "FaninNB", "fan_in", DAG_exec_state)
        return DAG_exec_state


        and tcp_server does:
            if (synchronizer is None):
                raise ValueError("synchronize_sync: Could not find existing Synchronizer with name '%s'" % synchronizer_name)

            # This tcp_server passing self so synchronizer can access tcp_server's send_serialized_object
            # return_value = synchronizer.synchronize_sync(tcp_server, obj_name, method_name, type_arg, state, synchronizer_name)
            return_value = synchronizer.synchronize_sync(tcp_server, obj_name, method_name, state, synchronizer_name, self)
    
        and, ASSUMING this is not a try-op, synchronizer.synchronize_synch does:

            if is_select:
                    self.lock_synchronizer()
                
                    if is_select:
                        # create result_buffer, create execute() reference, call execute(), result_buffer.withdraw(), 
                        # return excute's result, with no restart (by definition of synchronous non-try-op)
                        # (Send result to client below.)
                        wait_for_return = True
                        self.synchronizeSelect(base_name, state, wait_for_return, **state.keyword_arguments)
                    else:
                        return_value = self.synchronize(base_name, state, **state.keyword_arguments)
                    state.return_value = return_value
                    state.blocking = False 
                    logger.debug("synchronizerXXX: synchronize_sync: %s sending return_value %s back for method %s." % (synchronizer_name, str(return_value), method_name))
                    logger.debug("synchronizerYYY: synchronize_sync: %s sending state %s back for method %s." % (synchronizer_name, str(state), method_name))
                    tcp_handler.send_serialized_object(cloudpickle.dumps(state))  
                return 0

                and self.synchronize does:

                    synchronizer_method = getattr(self._synchClass, method_name)
                    returnValue, restart = synchronizer_method(self._synchronizer, **kwargs) 
                    if restart:
                        pass
                    return returnValue

                    So returnValue goes back to:

                        state.return_value = return_value
                        state.blocking = False 

                    which gets sent back to client:

                        tcp_handler.send_serialized_object(cloudpickle.dumps(state))  

                    as the result of their synchronize_sync. 

                    which we get as dummy_DAG_exec_state below.

        1. It does not make sense to batch try-ops, or to execute a batch of synchronous
            ops that may block and that have return values. faninNB fanins are non-blocking
            and either (1) we ignore return value since we are not the last caller to fanin
            or (2) we are last caller and we may use retrun value (if we need work), where
            there can only be one last caller so we are not returning multple return values
            for multiple operations.
        2. This means that we do not need the generality of doing a batch of synchronize-sync
            operations that could be try-ops, or could block, or could each require a value
            to be returned. 
        3. Thus we are doing a batch of fanins that each require a call to self.synchronize().
            The value returned by self.synchronize() for a fanin will either be a 
            dict_of_results, when the is the last caller for this fanin, or 0, when this is not
            the last call for the fanin.
        4. We can check the return value:
            If it is 0 then there is nothing to do. 
            If it is a dict_of_results and we (the client worker caller) do not need work, then 
                we can deosit this work in the work queue as tuple (start_state_of_fanin_task,
                dict_of_results)
            If it is a dict_of_results and we (the client worker caller) need work, then 
                we check whether we have already seen a dict of results for a previous fanin:
                if we have previosly seen a dict_of_results then we've already saved the 
                dict_of_results for return to the client worker caller so we can deposit
                this work in the work queue
            otherwise we save the dict_of_results for return to the client worker caller.
                Note that this is returned as DAG_exec_state.return_value, which is 
                checked by the caller upon return (see below).

        5. The caller does: 

            if dummy_DAG_exec_state.return_value == 0:
                # either we wanted work (worker_needs_input = true) but didn't get any 
                # or we didn't want any work
                return 0
            else:
                if using_workers:
                    # this caller can be a thread of a process.
                    dict_of_results = dummy_DAG_exec_state.return_value
                    if not worker_needs_input:
                        # work should have been enqueued in work_queue by tco_server.
                        logger.error("[Error: process_fannNBs: Internal error: got work but not worker_needs_input."])

                    # put results in our data_dict since we will use them next
                    # Note: We will be writog over our result from the task we 
                    #  did that inputs into this faninNB.
                    for key, value in dict_of_results.items():
                        data_dict[key] = value
                    # keep work and do it next
                    worker_needs_input = False
                    DAG_exec_state.state = start_state_fanin_task
                    logger.debug("process_faninNBs: set worker_needs_input to False.")
                else:
                    if not worker_needs_input:
                        # create a thread to do the work? this thread is not a worker
                        # so there is no work_queue; this thread is simulating Lambda
                        # scheme which means thread is created by faninNB on tcp_server?
                        # No: faninNB only creates therads if faninNBs are stored locally
                        # and there is no worker. Implicitly, here faninNBs are stored
                        # remotely. We are usng a single thread and storing faninNBs remotely
                        # so here create a new thread to do the work.



        """

    def synchronize_sync(self, message = None):
        """
        Synchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """

        logger.debug("[HANDLER] server.synchronize_sync() called.")
        obj_name = message['name']
        method_name = message['method_name']
        # type_arg = message["type"]
        state = decode_and_deserialize(message["state"])
        # synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = obj_name)
        synchronizer_name = self._get_synchronizer_name(type_name = None, name = obj_name)

        logger.debug("tcp_server: synchronize_sync: Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
        synchronizer = tcp_server.synchronizers[synchronizer_name]

        if (synchronizer is None):
            raise ValueError("synchronize_sync: Could not find existing Synchronizer with name '%s'" % synchronizer_name)

        # This tcp_server passing self so synchronizer can access tcp_server's send_serialized_object
        # return_value = synchronizer.synchronize_sync(tcp_server, obj_name, method_name, type_arg, state, synchronizer_name)
        return_value = synchronizer.synchronize_sync(tcp_server, obj_name, method_name, state, synchronizer_name, self)

        logger.debug("tcp_server called synchronizer.synchronize_sync")

        return return_value

    def synchronize_async(self, message = None):
        """
        Asynchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.debug("[HANDLER] server.synchronize_async() called.")
        obj_name = message['name']
        method_name = message['method_name']
        # type_arg = message["type"]        
        state = decode_and_deserialize(message["state"])

        synchronizer_name = self._get_synchronizer_name(type_name = None, name = obj_name)
        logger.debug("tcp_server: synchronize_async: Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
        synchronizer = tcp_server.synchronizers[synchronizer_name]

        if (synchronizer is None):
            raise ValueError("synchronize_async: Could not find existing Synchronizer with name '%s'" % synchronizer_name)

        logger.debug("tcp_server: synchronize_async: Successfully found synchronizer")

        # return_value = synchronizer.synchronize_async(obj_name, method_name, type_arg, state, synchronizer_name)
        return_value = synchronizer.synchronize_async(obj_name, method_name, state, synchronizer_name)

        logger.debug("tcp_server called synchronizer.synchronize_async")

        return return_value    

    def recv_object(self):
        """
        Receive an object from a remote entity via the given websocket.

        The TCP server uses a "streaming" API that is implemented using file handles (or rather the API looks like we're just using file handles).
        """
        try:
            # Read the size of the incoming serialized object.
            incoming_size = self.rfile.read(2) 
        except ConnectionAbortedError as ex:
            logger.error("Established connection aborted while reading incoming size.")
            logger.error(repr(ex))
            return None 

        # Convert bytes of size to integer.
        incoming_size = int.from_bytes(incoming_size, 'big')

        if incoming_size == 0:
            logger.debug("Incoming size is 0. Client is expected to have disconnected.")
            return None 
        
        if incoming_size < 0:
            logger.error("Incoming size < 0: " + incoming_size + ". An error might have occurred...")
            return None 

        logger.info("Will receive another message of size %d bytes" % incoming_size)

        data = bytearray()
        try:
            while len(data) < incoming_size:
                # Read serialized object (now that we know how big it'll be).
                new_data = self.rfile.read(incoming_size - len(data)).strip()

                if not new_data:
                    break 

                data.extend(new_data)
                logger.debug("Have read %d/%d bytes from remote client." % (len(data), incoming_size))
        except ConnectionAbortedError as ex:
            logger.error("Established connection aborted while reading data.")
            logger.error(repr(ex))
            return None 
        
        return data 

    def send_serialized_object(self, obj):
        """
        Send an ALREADY SERIALIZED object to the connected client.

        Serialize the object before calling this function via:
            obj = cloudpickle.dumps(obj)

        Arguments:
        ----------
            obj (bytes):
                The already-serialized object that we are sending to a remote entity (presumably an AWS Lambda executor).
        """
        logger.debug("Sending payload of size %d bytes to remote client now..." % len(obj))
        self.wfile.write(len(obj).to_bytes(2, byteorder='big'))     # Tell the client how many bytes we're sending.
        self.wfile.write(obj)                                       # Then send the object.
        logger.debug("Sent %d bytes to remote client." % len(obj))

    def close_all(self, message = None):
        """
        Clear all known synchronizers.
        """
        logger.debug("Received close_all request.")

        tcp_server.synchronizers = {}

        #############################
        # Write ACK back to client. #
        #############################
        resp = {
            "op": "ack",
            "op_performed": "close_all"
        }        
        logger.info("Sending ACK to client %s for 'close_all' operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.info("Sent ACK of size %d bytes to client %s for 'close_all' operation." % (len(resp_encoded), self.client_address[0]))          

    def close_obj(self, message = None):
        """
        Called by a remote Lambda to delete an object here on the TCP server.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """
        type_arg = message["type"]
        name = message["name"]
        #state = decode_and_deserialize(message["state"])

        logger.debug("Received close_obj request for object with name '%s' and type %s" % (name, type_arg))

        #############################
        # Write ACK back to client. #
        #############################
        resp = {
            "op": "ack",
            "op_performed": "close_obj"
        }        
        logger.info("Sending ACK to client %s for CLOSE_OBJ operation." % self.client_address[0])
        resp_encoded = json.dumps(resp).encode('utf-8')
        self.send_serialized_object(resp_encoded)
        logger.info("Sent ACK of size %d bytes to client %s for CLOSE_OBJ operation." % (len(resp_encoded), self.client_address[0]))        

    def setup_server(self, message = None):
        logger.debug("server.setup() called.")
        pass 
        
class TCPServer(object):
    def __init__(self):
        self.synchronizers =  {}    # dict of name to Synchronizer
        self.server_threads = []    # list      - not used
        self.clients =        []    # list      - not used
        self.server_address = ("0.0.0.0",25565)
        self.tcp_server = socketserver.ThreadingTCPServer(self.server_address, TCPHandler)
    
    def start(self):
        logger.info("Starting TCP server.")
        try:
            self.tcp_server.serve_forever()
        except Exception as ex:
            logger.error("Exception encountered:" + repr(ex))

if __name__ == "__main__":
    # Create a Server Instance
    tcp_server = TCPServer()
    tcp_server.start()
