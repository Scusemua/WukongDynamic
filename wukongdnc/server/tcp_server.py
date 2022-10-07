#from re import A
import json
import traceback
import socketserver
import cloudpickle


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
            logger.info("[HANDLER] Recieved one request from {}".format(self.client_address[1]))

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
        logger.debug("[HANDLER] server.create_all_fanins_and_faninNBs_and_possibly_work_queue() called.")
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

        logger.info("[HANDLER] server.synchronize_process_faninNBs_batch() called.")

        # name of the type is always "DAG_executor_FanInNB"
        type_arg = message["type"]
        # Name of the method callled on "DAG_executor_FanInNB" is always "fanin"
        method_name = message["name"]

        DAG_exec_state = decode_and_deserialize(message["state"])
        faninNBs = DAG_exec_state.keyword_arguments['faninNBs']
        #faninNB_sizes = DAG_exec_state.keyword_arguments['faninNB_sizes']
        # FYI:
        #result = DAG_exec_state.keyword_arguments['result']
        # For debuggng
        calling_task_name = DAG_exec_state.keyword_arguments['calling_task_name'] 
        DAG_states_of_faninNBs = DAG_exec_state.keyword_arguments['DAG_states_of_faninNBs'] 
        worker_needs_input = DAG_exec_state.keyword_arguments['worker_needs_input']
        work_queue_name = DAG_exec_state.keyword_arguments['work_queue_name']
        work_queue_type = DAG_exec_state.keyword_arguments['work_queue_type']
        work_queue_method = DAG_exec_state.keyword_arguments['work_queue_method']
        list_of_work_queue_fanout_values = DAG_exec_state.keyword_arguments['list_of_work_queue_fanout_values']

        logger.info("tcp_server: synchronize_process_faninNBs_batch: calling_task_name: " + calling_task_name + ": worker_needs_input: " + str(worker_needs_input)
            + " faninNBs size: " +  str(len(faninNBs)))

        # True if the client needs work and we got some work for the client, which are the
        # results of a faninNB.
        got_work = False
        list_of_work = []

        # List list_of_work_queue_fanout_values may be empty: if a state has no fanouts this list is empty. 
        # If a state has 1 fanout it will be a become task and there will be no moer fanouts.
        # If there are no fanouts, then worker_needs_work will be True and this list will be empty.
        # otherwise, the worker will have a become task so worker_needs_input will be false (and this
        # list may or may not be empty depending on whether there are any more fanouts.)
        if len(list_of_work_queue_fanout_values) > 0:
            # work_queue.deposit_all(list_of_work_queue_fanout_values)
            synchronizer = tcp_server.synchronizers[work_queue_name]
            synchClass = synchronizer._synchClass
            try:
                synchronizer_method = getattr(synchClass, work_queue_method)
            except Exception as ex:
                logger.error("tcp_server: synchronize_process_faninNBs_batch: deposit fanout work: Failed to find method '%s' on object '%s'." % (work_queue_method, work_queue_type))
                raise ex

            # To call "deposit" instead of "deposit_all", change the work_queue_method above before you
            # generate synchronizer_method and here iterate over the list.
            # work_queue_method = "deposit"
            #for work_tuple in list_of_work:
                #work_queue_method_keyword_arguments = {}
                #work_queue_method_keyword_arguments['value'] = work_tuple
                #returnValue, restart = synchronizer_method(synchronizer._synchronizer, **work_queue_method_keyword_arguments) 

            work_queue_method_keyword_arguments = {}
            work_queue_method_keyword_arguments['list_of_values'] = list_of_work_queue_fanout_values
            # call work_queue (bounded buffer) deposit_all(list_of_work_queue_fanout_values)
            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": deposit all fanout work.")
            returnValue, restart = synchronizer_method(synchronizer._synchronizer, **work_queue_method_keyword_arguments) 
            # deposit_all return value is 0 and restart is False
        else:
            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": no fanout work to deposit")

        for name in faninNBs:
            start_state_fanin_task  = DAG_states_of_faninNBs[name]

            synchronizer_name = self._get_synchronizer_name(type_name = None, name = name)
            logger.debug("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
            synchronizer = tcp_server.synchronizers[synchronizer_name]

            if (synchronizer is None):
                raise ValueError("synchronize_process_faninNBs_batch: Could not find existing Synchronizer with name '%s'" % synchronizer_name)

            base_name, isTryMethod = isTry_and_getMethodName(method_name)
            is_select = isSelect(type_arg) # is_select = isSelect(type_arg)
    
            logger.debug("tcp_server: synchronize_process_faninNBs_batch: method_name: " + method_name + ", base_name: " + base_name + ", isTryMethod: " + str(isTryMethod))
            logger.debug("tcp_server: synchronize_process_faninNBs_batch: synchronizer_class_name: : " + type_arg + ", is_select: " + str(is_select))

            # These are per FaninNB
            DAG_exec_state.keyword_arguments['fanin_task_name'] = name
            DAG_exec_state.keyword_arguments['start_state_fanin_task'] = start_state_fanin_task

            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": calling synchronizer.synchronize.")
            return_value = synchronizer.synchronize(base_name, DAG_exec_state, **DAG_exec_state.keyword_arguments)
            """
            Note: It does not make sense to batch try-ops, or to execute a batch of synchronous
                ops that may block and that have return values. faninNB fan_ins are non-blocking
                and either (1) we ignore the return value since we were not the last caller to fan_in
                or (2) we are last caller for a faninNB fan_in so we save the work (for only one faninNB) 
                which is the statr state and the dict. of results for the fanin task, and return this work
                to the caller. So we batch faninNB fan_ins but we are not returning multple return values
                for multiple fan_in operations. (Again, we return either no work or the work (fanin results)
                from one of the fan_ins for which we were the last caller).
                This means that we do not need the generality of doing a batch of synchronize-sync
                operations that could be try-ops, or could block, or could each require a value
                to be returned. Thus we call synchronizer.synchronize() for each fan_in. Note that
                synchronize_sync calls synchronizer.synchronize to do the fan_in. Note: We do not call
                sychronize_sync and let it call synchronizer.synchronize since synchronize_sync sends the 
                return value of synchronizer.synchronize back to the client and we do not want that to 
                happen. So we call synchronizer.synchronize and process the return value (see if it is 
                work that can be sent to the client (if the client needs work))
            """
            if return_value != 0:
                # return value is a dictionary of results for the fanin task
                work_tuple = (start_state_fanin_task,return_value)
                if worker_needs_input:
                    # Changing local worker_needs_input; it's still True on client caller, of course
                    worker_needs_input = False
                    got_work = True
                    DAG_exec_state.return_value = work_tuple
                    DAG_exec_state.blocking = False 
                    logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": send work: %s sending name %s and return_value %s back for method %s." % (synchronizer_name, name, str(return_value), method_name))
                    logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": send work: %s sending state %s back for method %s." % (synchronizer_name, str(DAG_exec_state), method_name))
                    # Note: We send work back now, as soon as we get it, to free up the waitign client
                    # instead of waiting until the end. This delays the processing of FaninNBs and depositing
                    # any work in the work_queue. Possibly: create a thread to do this.                 
                    self.send_serialized_object(cloudpickle.dumps(DAG_exec_state))
                else:
                    # Client doesn't need work or we already got some work for the client, so add this work
                    # to the work_queue)
                    list_of_work.append(work_tuple)
                    logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": not sending work: %s sending name %s and return_value %s back for method %s." % (synchronizer_name, name, str(return_value), method_name))
                    logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": not sending work: %s sending state %s back for method %s." % (synchronizer_name, str(DAG_exec_state), method_name))
            # else we were not the last caller of fanin, so we deposited our result, which will be given to
            # the last caller.
 
        if len(list_of_work) > 0:            
            synchronizer = tcp_server.synchronizers[work_queue_name]
            synchClass = synchronizer._synchClass

            #rhc
            #work_queue_method = "deposit"

            try:
                synchronizer_method = getattr(synchClass, work_queue_method)
            except Exception as ex:
                logger.error("tcp_server: synchronize_process_faninNBs_batch: deposit fanin work: Failed to find method '%s' on object '%s'." % (work_queue_method, work_queue_type))
                raise ex

            # To call "deposit" instead of "deposit_all", change the work_queue_method above before you
            # generate synchronizer_method and here iterate over the list.
            # work_queue_method = "deposit"
            #for work_tuple in list_of_work:
                #work_queue_method_keyword_arguments = {}
                #work_queue_method_keyword_arguments['value'] = work_tuple
                #returnValue, restart = synchronizer_method(synchronizer._synchronizer, **work_queue_method_keyword_arguments) 

            work_queue_method_keyword_arguments = {}
            work_queue_method_keyword_arguments['list_of_values'] = list_of_work
            # call work_queue (bounded buffer) deposit_all(list_of_work)
            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": deposit_all FanInNB work, list_of_work size: " + str(len(list_of_work)))
            returnValue, restart = synchronizer_method(synchronizer._synchronizer, **work_queue_method_keyword_arguments) 
            # deposit_all return value is 0 and restart is False

            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": work_queue_method: " + str(work_queue_method) + ", restart " + str(restart))
            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": " + str(work_queue_method) + ", returnValue " + str(returnValue))
            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": " + str(work_queue_method) + ", successfully called work_queue method. ")

        if not got_work:
            # if if worker_needs_input is sent from client as False, then got_work is initially False and never set to True
            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": no work to return, returning DAG_exec_state.return_value = 0.")           
            DAG_exec_state.return_value = 0
            DAG_exec_state.blocking = False
            # Note: if we decide not to send work back immediately to the waitign clent (see above),
            # then we can comment this send out, uncomment the else and he log mssage in the else part, 
            # and uncomment the send at the end. That send will either send the DAG_exec_state return 
            # value 0 we just set or the DAG_xec_state above with the return value containing work.
            self.send_serialized_object(cloudpickle.dumps(DAG_exec_state))
        #else:
            # we return the DAG_exec_state.return_value set to work_tuple above
            #logger.debug("tcp_server: synchronize_process_faninNBs_batch: returning work in DAG_exec_state.") 

        #logger.debug("tcp_server: synchronize_process_faninNBs_batch: returning DAG_state %s." % (str(DAG_exec_state)))           
        #self.send_serialized_object(cloudpickle.dumps(DAG_exec_state))

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
            incoming_size = self.rfile.read(4) 
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
        self.wfile.write(len(obj).to_bytes(4, byteorder='big'))     # Tell the client how many bytes we're sending.
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
