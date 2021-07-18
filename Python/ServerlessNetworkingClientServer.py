from channel import UniChannel, BiChannel

class ServerlessNetworkingClientServer(object):
    def __init__(self, connections : BiChannel, client_channel : UniChannel):
        self.connections = connections 
        self.client_channel = client_channel
    
    def send1(self, msg):
        self.connections.send1(msg)
        
    def rcv1(self):
        return self.client_channel.rcv()