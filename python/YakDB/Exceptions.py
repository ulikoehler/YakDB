#!/usr/bin/env python
# -*- coding: utf8 -*-
class ParameterException(Exception):
    """
    An exception that relates to  any caller-supplied parameter
    not being the correct type/value
    """
    def __init__(self, message):
        Exception.__init__(self, message)

class YakDBProtocolException(Exception):
    """
    An internal exception in the ZeroDB protocol.
    Should not occur if the API is used correctly
    """
    def __init__(self, message):
        Exception.__init__(self, message)

class ConnectionStateException(Exception):
    """
    This exception is used if the connection is not in the correct state
    to execute the operation.
    """
    def __init__(self, message):
        Exception.__init__(self, message)
