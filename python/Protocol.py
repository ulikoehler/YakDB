#!/usr/bin/env python
import zmq
import struct

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
