#!/usr/bin/env python
# -*- coding: utf8 -*-
import platform
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
    def convertToBinary(value, convertIterables=True):
        """
        Given a string, float or int value, convert it to binary and return the converted value.
        Ints are converted to 32-bit little-endian signed integers (uint32_t).
        Floats are converted to 64-bit little-endian IEEE754 values (double).
        Numeric values are assumed to be signed.
        
        Given a list of these types, returns a list of the converted values,
        if convertIterables is not set to False.
        
        For other types, raises an exception
        """
        if type(value) is str:
            return value
        elif type(value) is int:
            return struct.pack('<i', value)
        elif type(value) is float:
            return struct.pack('<d', value)
        elif type(value) is unicode:
            return value.encode("utf-8")
        #Listlike types
        elif isinstance(value, collections.Iterable):
            #str and unicode are also iterable, but we already handled that
            if not convertIterables:
                raise ParameterException("Value '%s' contains nested iterable or iterable conversion is disabled, can't convert!" % str(value))
            return [ZMQBinaryUtil.convertToBinary(val, convertIterables=False)
                    for val in value]
        elif value is None:
            raise ParameterException("Can't convert None values, please check the value does not contains any None values")
        else:
            raise ParameterException("Value '%s' of type %s is not mappable to binary. Please use a binary string for custom types." % (value, str(type(value))))
    @staticmethod
    def convertToBinaryList(value, convertIterables=True):
        """
        Same as ZMQBinaryUtil.convertToBinary(), but always returns lists.
        """
        conv = convertToBinary(value, convertIterables)
        if type(conv) != list:
            conv = [conv]
        return conv
