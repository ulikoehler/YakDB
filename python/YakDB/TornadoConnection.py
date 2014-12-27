#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
Asynchronous YakDB connection that works with the tornado IO loop.
Automatically installs itself as tornado IO loop.

Currently in development. Supports only a small subset of available requests.
"""
from YakDB.ConnectionBase import YakDBConnectionBase
from YakDB.Conversion import ZMQBinaryUtil
from YakDB.Exceptions import YakDBProtocolException
import struct
import sys
import zmq
from zmq.eventloop import ioloop
ioloop.install()
from zmq.eventloop.zmqstream import ZMQStream

class TornadoConnection(YakDBConnectionBase):
    """
    A tornado IOLoop-based connection variant that uses a DEALER connection
    with YakDB request IDs to support parallel requests without having to wait.
    
    This class does not have special documentation. See YakDB.Connection for details
    on function parameters. The purpose of this behaviour is to avoid having to update
    duplicate content.
    In addition to the YakDB.Connection arguments, simply supply a callback function.
    It will be called with the result of the operation (or without parameter if the
    operation has no result).

    Also note that exception-based error reporting in this class
    does not allow to backtrace the caller because the receipt handler
    is called from the IO loop. This might be fixed in the future.
    """
    def __init__(self, endpoints, context=None):
        YakDBConnectionBase.__init__(self, context=context)
        self.useDealerMode()
        self.connect(endpoints)
        self.requests = {} #Maps request ID to callback
        self.nextRequestId = 0
        self.stream = ZMQStream(self.socket)
        self.stream.on_recv(self.__recvCallback)
    def scan(self, tableNo, callback, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, skip=0, invert=False, mapData=False):
        requestId = self.__newRequest(callback, {"mapData": mapData})
        msgParts = [""] + YakDBConnectionBase.buildScanRequest(self, tableNo, startKey, endKey, limit, keyFilter, valueFilter, skip, invert, requestId=requestId)
        self.stream.send_multipart(msgParts)
    def read(self, tableNo, keys, callback, mapKeys=False):
        requestId = self.__newRequest(callback, {"keys": (keys if mapKeys else None), "mapKeys": mapKeys})
        msgParts = [""] + YakDBConnectionBase.buildReadRequest(self, tableNo, keys, requestId)
        self.stream.send_multipart(msgParts)
    def __newRequest(self, callback, params={}):
        """Setup mapping for a new request. Returns the new request ID."""
        self.nextRequestId += 1
        self.requests[self.nextRequestId] = (callback, params)
        return struct.pack("<I", self.nextRequestId)
    def __recvCallback(self, msg):
        #DEALER response contains empty delimiter!
        if len(msg[0]) != 0:
            print >>sys.stderr, "Received malformed message: ", msg
            return
        msg = msg[1:]
        #Currently we don't check the response type
        YakDBConnectionBase._checkHeaderFrame(msg)
        #Struct unpack yields 1-element tuple!
        headerFrame = msg[0]
        assert(len(headerFrame) == 8) #4 bytes response + 4 bytes request ID
        requestId = struct.unpack("<I", YakDBConnectionBase._extractRequestId(headerFrame))[0]
        callback, params = self.requests[requestId]
        #Postprocess, depending on request type.
        responseType = headerFrame[2]
        dataFrames = msg[1:]
        if responseType == "\x13": #Scan
            if params["mapData"]:
                data = YakDBConnectionBase(dataFrames)
            else:
                data = YakDBConnectionBase._mapScanToTupleList(dataFrames)
        elif responseType == "\x10": #Read
            if params["mapKeys"]:
                data = YakDBConnectionBase._mapReadKeyValues(params["keys"], dataFrames)
            else:
                data = dataFrames
        else:
            raise YakDBProtocolException("Received correct response, but cannot handle response code %d" % ord(responseType))
        #Cleanup
        del self.requests[requestId]
        #Call original callback
        callback(data)
