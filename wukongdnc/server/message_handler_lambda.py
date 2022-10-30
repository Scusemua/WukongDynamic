import traceback

from .synchronizer_lambda import Synchronizer
from .util import decode_and_deserialize #, make_json_serializable,  isTry_and_getMethodName, isSelect 

from ..dag.DAG_executor_constants import run_all_tasks_locally
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

class MessageHandler(object):
    synchronizers =  {} 
        
    def handle(self,json_message):

        #Lambda message  handler for incoming requests from AWS Lambda functions.
        
        while True:
            # Q: Should we pass self.client_address[0] to LambdaBB aand LambdaSem? This is just the client's IP: 127.0.0.1?
            # logger.info("[MessageHandler] Recieved one request from {}".format(self.client_address[0]))

            self.action_handlers = {
                "create": self.create_obj,
                "setup": self.setup_server,
                "synchronize_async": self.synchronize_async,
                "synchronize_sync": self.synchronize_sync,
                "close_all": self.close_all,
                # These are DAG execution operations
                "create_all_fanins_and_faninNBs_and_possibly_work_queue": self.create_all_fanins_and_faninNBs_and_possibly_work_queue,
                "synchronize_process_faninNBs_batch": self.synchronize_process_faninNBs_batch,
                "create_work_queue": self.create_work_queue
            }
            #logger.info("Thread Name:{}".format(threading.current_thread().name))

            try:    
                message_id = json_message["id"]
                action = json_message.get("op", None)
                logger.debug("[MessageHandler] Handling message from client with ID=%s, operation=%s" % (message_id, action))
                
                return_value = self.action_handlers[action](message = json_message)
            except ConnectionResetError as ex:
                logger.error(ex)
                logger.error(traceback.format_exc())
                return_value = {
                    "msg": "ConnectionResetError encountered while executing the Lambda function.",
                    "error_msg": str(ex)
                }
            except Exception as ex:
                logger.error(ex)
                logger.error(traceback.format_exc())
                return_value = {
                    "msg": str(type(ex)) + " encountered while executing the Lambda function.",
                    "error_msg": str(ex)
                }
                
            # this is return value of Lambda, sent back to tcp_server method that invoked Lambda - create, 
            # synchronize_async, synchronize_async.
            return return_value
                
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
        logger.debug("[MESSAGEHANDLER] create() called.")
        type_arg = message["type"]
        obj_name = message["name"]
        state = decode_and_deserialize(message["state"])

        synchronizer = Synchronizer()
        synchronizer.create(type_arg, obj_name, **state.keyword_arguments)
        synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = obj_name)
        logger.debug("MessageHandler create caching new Synchronizer of type '%s' with name '%s'" % (type_arg, synchronizer_name))
        MessageHandler.synchronizers[synchronizer_name] = synchronizer # Store Synchronizer object.

        # return to handle()
        # tcp_server.create will ignore this return value and send a response to client indicating create is complete.
        return 0

    def create_one_of_all_objs(self,message = None):
        """
        Called by create_all_fanins_and_faninNBs_and_possibly_work_queue and create_work_queue to 
        create an object here on the TCP server. No ack is sent to a client. 
        create_all_fanins_and_faninNBs_and_possibly_work_queue and create_work_queue will
        return to tco_server_lambda, which will send the ack.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.debug("[MESSAGEHANDLER] server.create_one_of_all_objs() called.")
        type_arg = message["type"]
        name = message["name"]
        state = decode_and_deserialize(message["state"])

        synchronizer = Synchronizer()
        synchronizer.create(type_arg, name, **state.keyword_arguments)
        synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = name)
        logger.debug("Caching new Synchronizer of type '%s' with name '%s'" % (type_arg, synchronizer_name))
        MessageHandler.synchronizers[synchronizer_name] = synchronizer # Store Synchronizer object.

        # Do not send ack to client - this is just one of possibly many of the creates from create_all_fanins_and_faninNBs

    def create_all_fanins_and_faninNBs_and_possibly_work_queue(self, message = None):
        """
        Called by a remote Lambda to create fanins, faninNBs, and pssibly work queue.
        Number of fanins/faninNBs may be 0.

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
        logger.debug("[MESSAGEHANDLER] server.create_all_fanins_and_faninNBs_and_possibly_work_queue() called.")
        messages = message['name']
        fanin_messages = messages[0]
        faninNB_messages = messages[1]
        logger.info(str(fanin_messages))
        logger.info(str(faninNB_messages))

        for msg in fanin_messages:
            self.create_one_of_all_objs(msg)
        if len(fanin_messages) > 0:
            logger.info("created fanins")

        for msg in faninNB_messages:
            self.create_one_of_all_objs(msg)
        if len(faninNB_messages) > 0:
            logger.info("created faninNBs")

        # we always create the fanin and faninNBs. We possibly create the work queue. If we send
        # a message for create work queue, in addition to the lst of messages for create
        # fanins and create faninNBs, we create a work queue too.
        create_the_work_queue = (len(messages)>2)
        if create_the_work_queue:
            logger.info("create_the_work_queue: " + str(create_the_work_queue) + " len: " + str(len(messages)))
            msg = messages[2]
            self.create_one_of_all_objs(msg)

        # return to handle()
        # tcp_server.create will ignore this return value and send a response to client indicating create is complete.
        return 0

    def synchronize_process_faninNBs_batch(self, message = None):
        """
        Synchronous process all faninNBs for a given state during DAG execution.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """

        logger.info("[MESSAGEHANDLER] server.synchronize_process_faninNBs_batch() called.")

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
        # Note: if using lambdas, then we are not usingn workers (for now) so worker_needs_input must be false
        worker_needs_input = DAG_exec_state.keyword_arguments['worker_needs_input']
        work_queue_name = DAG_exec_state.keyword_arguments['work_queue_name']
        work_queue_type = DAG_exec_state.keyword_arguments['work_queue_type']
        work_queue_method = DAG_exec_state.keyword_arguments['work_queue_method']
        list_of_fanout_values = DAG_exec_state.keyword_arguments['list_of_work_queue_fanout_values']

        logger.info("tcp_server: synchronize_process_faninNBs_batch: calling_task_name: " + calling_task_name + ": worker_needs_input: " + str(worker_needs_input)
            + " faninNBs size: " +  str(len(faninNBs)))

        # assert:
        if worker_needs_input:
            if not run_all_tasks_locally:
                logger.error("[Error: Internal Error: synchronize_process_faninNBs_batch: worker needs input but using lambdas.")
        
        # Note: If we are using lambdas, then we are not using workers (for now) so worker_needs_input
        # must be false. Also, we are currently not piggybacking the fanouts so there should be no 
        # fanouts to process.

        # True if the client needs work and we got some work for the client, which are the
        # results of a faninNB.
        got_work = False
        list_of_work = []

        # List list_of_work_queue_fanout_values may be empty: if a state has no fanouts this list is empty. 
        # If a state has 1 fanout it will be a become task and there will be no moer fanouts.
        # If there are no fanouts, then worker_needs_work will be True and this list will be empty.
        # otherwise, the worker will have a become task so worker_needs_input will be false (and this
        # list may or may not be empty depending on whether there are any more fanouts.)
        if len(list_of_fanout_values) > 0:
            # if run_all_tasks_locally then we are not using lambdas so add fanouts as work in the 
            # work queue.
            # If we are using lambdas, then we can use the parallel invoker to invoke the fanout lambdas
            if run_all_tasks_locally:
                # work_queue.deposit_all(list_of_work_queue_fanout_values)
                synchronizer = MessageHandler.synchronizers[work_queue_name]
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
                work_queue_method_keyword_arguments['list_of_values'] = list_of_fanout_values
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
            synchronizer = MessageHandler.synchronizers[synchronizer_name]

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
            returned_work = None
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
                    #
                    # Note: this scheme is changed when we are running in a lambda, as we are here.
                    # The work must be returned back to tcp_server_lambda, whcih will send the work to
                    # the client. So save the work and return it at the end.
                    returned_work = DAG_exec_state          
                    #self.send_serialized_object(cloudpickle.dumps(DAG_exec_state))
                else:
                    # Client doesn't need work or we already got some work for the client, so add this work
                    # to the work_queue)
                    list_of_work.append(work_tuple)
                    logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": not sending work: %s sending name %s and return_value %s back for method %s." % (synchronizer_name, name, str(return_value), method_name))
                    logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": not sending work: %s sending state %s back for method %s." % (synchronizer_name, str(DAG_exec_state), method_name))
            # else we were not the last caller of fanin, so we deposited our result, which will be given to
            # the last caller.
 
        if len(list_of_work) > 0:   
            # There is work in the form of faninNB tasks for which we were the last fan_in caller; thia
            # work gets enqueued in the work queue        
            synchronizer = MessageHandler.synchronizers[work_queue_name]
            synchClass = synchronizer._synchClass

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
            # if we didn't need work or we did need work but we did not get any above, 
            # then we return 0 to indicate that we didn't get work. 
            # if worker_needs_input is sent from client as False, then got_work is initially False and never set to True
            logger.info("tcp_server: synchronize_process_faninNBs_batch: " + calling_task_name + ": no work to return, returning DAG_exec_state.return_value = 0.")           
            DAG_exec_state.return_value = 0
            DAG_exec_state.blocking = False
            # Note: if we decide not to send work back immediately to the waitign clent (see above),
            # then we can comment this send out, uncomment the else and he log mssage in the else part, 
            # and uncomment the send at the end. That send will either send the DAG_exec_state return 
            # value 0 we just set or the DAG_xec_state above with the return value containing work.
            #
            # Note: this scheme is changed when we are running in a lambda, as we are here.
            # The work must be returned back to tcp_server_lambda, whcih will send the work to
            # the client. So save the work and return it at the end.    
            returned_work = DAG_exec_state 
            #self.send_serialized_object(cloudpickle.dumps(DAG_exec_state))
        #else:
            # we got work above so we already returned the DAG_exec_state.return_value set to work_tuple 
            # via self.send_serialized_object(work)
            #logger.debug("tcp_server: synchronize_process_faninNBs_batch: returning work in DAG_exec_state.") 

        #logger.debug("tcp_server: synchronize_process_faninNBs_batch: returning DAG_state %s." % (str(DAG_exec_state)))           
        #self.send_serialized_object(cloudpickle.dumps(DAG_exec_state))

        logger.debug("MessageHandler finished synchronize_process_faninNBs_batch")
        
        # this is a DAG_executor_State with  DAG_exec_state.return_value = work_tuple
        # or DAG_exec_state.return_value = 0
        return returned_work

    # Not used and not tested. Currently create work queue in 
    # create_all_fanins_and_faninNBs_and_possibly_work_queue. 
    def create_work_queue(self, message = None):
        # used to create only a work queue. This is the case when we are creating the fanins and faninNBs
        # on the fly, i.e., not at the beginning of execution.
        self.create_one_of_all_objs(message)

        # return to handle()
        # tcp_server.create will ignore this return value and send a response to client indicating create is complete.
        return 0

    def synchronize_sync(self, message = None):
        """
        Synchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """
        
        logger.debug("[MESSAGEHANDLER] synchronize_sync() called.")
        obj_name = message['name']
        method_name = message['method_name']
        state = decode_and_deserialize(message["state"])
        
        # not using synchronizer class name in object name for now, i.e., use "bb" instead of "BoundedBuffer_bb"
        # type_arg = message["type"]
        # synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = obj_name)
        synchronizer_name = self._get_synchronizer_name(type_name = None, name = obj_name)
        
        logger.debug("MessageHandler: synchronize_sync: Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
        synchronizer = MessageHandler.synchronizers[synchronizer_name]
        
        if (synchronizer is None):
            raise ValueError("MessageHandler: synchronize_sync: Could not find existing Synchronizer with name '%s'" % synchronizer_name)
         
        # return_value = synchronizer.synchronize_sync(tcp_server, obj_name, method_name, type_arg, state, synchronizer_name)
        # return_value = synchronizer.synchronize_sync(tcp_server, obj_name, method_name, state, synchronizer_name, self)
        # MessageHandler not passing itself since synchronizer does not use send_serialized_object to send results 
        # to tcp_server - Lambda returns values synchronously.
        #return_value = synchronizer.synchronize_sync(obj_name, method_name, state, synchronizer_name, self)
        return_value = synchronizer.synchronize_sync(obj_name, method_name, state, synchronizer_name)
        
        logger.debug("MessageHandler called synchronizer.synchronize_sync")
        
        return return_value

    def synchronize_async(self, message = None):
        """
        Asynchronous synchronization.

        Key-word arguments:
        -------------------
            message (dict):
                The payload from the AWS Lambda function.
        """        
        logger.debug("[MESSAGEHANDLER] synchronize_async() called.")
        obj_name = message['name']
        method_name = message['method_name']       
        state = decode_and_deserialize(message["state"])

        # not using synchronizer class name in object name for now, i.e., use "bb" instead of "BoundedBuffer_bb"
        # type_arg = message["type"]
        # synchronizer_name = self._get_synchronizer_name(type_name = type_arg, name = obj_name)    
        synchronizer_name = self._get_synchronizer_name(type_name = None, name = obj_name)
        logger.debug("MessageHandler: synchronize_async: Trying to retrieve existing Synchronizer '%s'" % synchronizer_name)
        synchronizer = MessageHandler.synchronizers[synchronizer_name]
        
        if (synchronizer is None):
            raise ValueError("MessageHandler: synchronize_async: Could not find existing Synchronizer with name '%s'" % synchronizer_name)
        
        logger.debug("MessageHandler: synchronize_async: Successfully found synchronizer")

        # return_value = synchronizer.synchronize_async(obj_name, method_name, type_arg, state, synchronizer_name)
        return_value = synchronizer.synchronize_async(obj_name, method_name, state, synchronizer_name)
        
        logger.debug("MessageHandler called synchronizer.synchronize_async")
        
        return return_value    

    def close_all(self, message = None):
        """
        Clear all known synchronizers.
        """
        logger.debug("MessageHandler: close_all: Received close_all request.")

        MessageHandler.synchronizers = {}

        # return to handle()
        # tcp_server.close_all will ignore this return value and send a response to client indicating create is complete.
        return 0    
    
    # Currently does not close any object - relying on close_all
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

        # return to handle()
        # tcp_server.close_obj will ignore this return value and send a response to client indicating create is complete.
        return 0
        
    def setup_server(self, message = None):
        logger.debug("server.setup() called.")
        pass 