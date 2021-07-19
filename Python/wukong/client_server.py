from .channel import UniChannel, BiChannel
import yaml 
import importlib

# with open("wukong-divide-and-conquer.yaml") as f:
#     config = yaml.load(f, Loader = yaml.FullLoader)
#     config = config 
#     sources_config = config["sources"]
#     memoization_config = config["memoization"]
    
#     source_path = sources_config["source-path"]
#     source_module = sources_config["source-module"]
#     spec = importlib.util.spec_from_file_location(source_module, source_path)
#     user_module = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(user_module) 

class ServerlessNetworkingClientServer(object):
    def __init__(self, connections : BiChannel, client_channel : UniChannel):
        self.connections = connections 
        self.client_channel = client_channel       
    
    def send1(self, msg):
        self.connections.send1(msg)
        
    def rcv1(self):
        return self.client_channel.rcv()