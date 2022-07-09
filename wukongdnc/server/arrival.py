class Arrival(object):
    def __init__(self, entry_name, synchronizer, synchronizer_method, result_buffer, timestamp, state, **kwargs):
        self._entry_name = entry_name
        self._synchronizer = synchronizer
        self._synchronizer_method = synchronizer_method
        self._result_buffer = result_buffer
        self._timestamp = timestamp
        self._kwargs = kwargs
        self._state = state    #place holder
