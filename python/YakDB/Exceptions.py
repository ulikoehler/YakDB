#!/usr/bin/env python
class ParameterException(Exception):
    """
    An exception that relates to  any caller-supplied parameter
    not being the correct type/value
    """
    def __init__(self, message):
        Exception.__init__(self, message)

class ZeroDBProtocolException(Exception):
    """
    An internal exception in the ZeroDB protocol.
    Should not occur if the API is used correctly
    """
    def __init__(self, message):
        Exception.__init__(self, message)
