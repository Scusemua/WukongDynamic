from channel import UniChannel, BiChannel

class ServerlessNetworkingClientServer(object):
    def __init__(self, connections : BiChannel, client_channel : UniChannel):
        self.connections = connections 
        self.client_channel = client_channel