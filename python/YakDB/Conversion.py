#!/usr/bin/env python
import platform
if platform.python_implementation() == "PyPy":
    import zmqpy as zmq
else:
    import zmq
import struct
from Exceptions import ParameterException

class ZMQBinaryUtil:
    """
    A class that provides utilities to send/receive little-endian binary data over a ZMQ socket.
    Provides static methods only
    """
    def __init__(self): raise Exception("Why the hell wouldn't you read the docs before randomly creating objects?")
    @staticmethod
    def sendBinary32(socket, value, more=True):
        """
        Send a given int as 32-bit little-endian unsigned integer over self.socket.
        @param value The integral value, or None to send empty frame.
        @param more If this is set to True (default), ZMQ_SNDMORE is set, else it is unset
        """
        if value is None:
            socket.send("", (zmq.SNDMORE if more else 0))
            return
        if type(value) is not int:
            raise Exception("Can't format object of non-integer type as binary integer")
        socket.send(struct.pack('<I', value), (zmq.SNDMORE if more else 0))
    @staticmethod
    def sendBinary64(socket, value, more=True):
        """
        Send a given int as 64-bit little-endian unsigned integer over self.socket.
        @param value The integral value, or None to send empty frame.
        @param more If this is set to True (default), ZMQ_SNDMORE is set, else it is unset
        """
        if value is None:
            socket.send("", (zmq.SNDMORE if more else 0))
            return
        if type(value) is not int:
            raise Exception("Can't format object of non-integer type as binary integer")
        socket.send(struct.pack('<q', value), (zmq.SNDMORE if more else 0))
    @staticmethod
    def convertToBinary(value):
        """
        Given a string, float or int value, convert it to binary and return the converted value.
        Ints are converted to 32-bit little-endian signed integers (uint32_t).
        Floats are converted to 64-bit little-endian IEEE754 values (double).
        Numeric values are assumed to be signed.
        For other types, raises an exception
        """
        if type(value) is int:
            return struct.pack('<i', value)
        elif type(value) is float:
            return struct.pack('<d', value)
        elif type(value) is str:
            return value
        else:
            raise ParameterException("Value '%s' is neither int nor float nor str-type -- not mappable to binary. Please use a binary string for custom types." % value)

