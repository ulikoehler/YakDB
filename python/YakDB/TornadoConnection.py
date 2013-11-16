#!/usr/bin/env python
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
import platform
import zmq
from zmq.eventloop import ioloop
ioloop.install()
from zmq.eventloop.zmqstream import ZMQStream

class TornadoConnection(YakDBConnectionBase):
    """
    A tornado IOLoop-based connection variant that uses a DEALER connection
    with YakDB request IDs to support parallel requests without having to wait.
    
    This class does not support auto-mapping of scan/read results.
    It always returns the raw value (therefore being more efficient):
        - List of values for read requests
        - List of tuples for write requests

    Also note that exception-based error reporting in this class
    does not allow to backtrace the caller because the receipt handler
    is called from the IO loop. This might be fixed in the future.
    """
    def __init__(self, endpoints, context=None):
        YakDBConnectionBase.__init__(self, context=context)
        self.useDealerMode()
        self.connect(endpoints)
        print "Connecting to ", endpoints
        self.requests = {} #Maps request ID to callback
        self.nextRequestId = 0
        self.stream = ZMQStream(self.socket)
        self.stream.on_recv(self.__recvCallback)
    def scan(self, tableNo, callback, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, invert=False):
        requestId = self.__newRequest(callback)
        msgParts = YakDBConnectionBase.buildScanRequest(self, tableNo, startKey, endKey, limit, keyFilter, valueFilter, invert, requestId=requestId)
        self.stream.send_multipart(msgParts)
        print "Scanning"
    def read(self, tableNo, keys, callback, mapKeys=False):
        msgParts = YakDBConnectionBase.buildReadRequest(self, tableNo, keys)
        self.stream.send_multipart(msgParts)
    def __newRequest(self, callback, params={}):
        """Setup mapping for a new request. Returns the new request ID."""
        self.nextRequestId += 1
        self.requests[self.nextRequestId] = (callback, params)
        return struct.pack("<I", self.nextRequestId)
    def __recvCallback(self, msg):
        #Currently we don't check the response type
        YakDBConnectionBase._checkHeaderFrame(msg)
        #Struct unpack yields 1-element tuple!
        requestId = struct.unpack("<I", YakDBConnectionBase._extractRequestId(msg[0]))[0]
        callback, params = self.requests[requestId]
        #Postprocess, depending on request type.
        headerFrame = msg[0]
        responseType = headerFrame[2]
        if responseType == "\x13": #Scan
            data = YakDBConnectionBase._mapScanToTupleList(msg[1:])
        elif responseType == "\x10": #Read
            data = YakDBConnectionBase._mapReadKeyValues(params["keys"], msg[1:])
        else:
            raise YakDBProtocolException("Received correct response, but cannot handle response code %d" % ord(responseType))
        #Call original callback
        callback(data)

class SerialTornadoConnection(YakDBConnectionBase):
    """
    An instance of this class represents a connection to a YakDB database.
    This thin wrapper uses asynchronicity together with the Tornado IO loop.
    
    This class provides a reentrant wrapper that may be called only from a single
    thread but from multiple IO-loop-based eventlets in that thread.
    """
    def __initStream(self, callback, options={}):
        """
        Initialize a new stream over the current connection.
        """
        stream = ZMQStream(self.socket)
        stream.callback = callback
        stream.options = options
        return stream
    def scan(self, tableNo, callback, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, invert=False, mapData=False):
        """
        Asynchronous reentrant scan. Scans an entire range at once.
        The scan stops at the table end, endKey (exclusive) or when
        *limit* keys have been read, whatever occurs first

        See self.read() documentation for an explanation of how
        non-str values are mapped.

        @param tableNo The table number to scan in
        @param callback A function(param, scanResult) which is called with the custom defined parameter
        @param startKey The first key to scan, inclusive, or None or "" (both equivalent) to start at the beginning
        @param endKey The last key to scan, exclusive, or None or "" (both equivalent) to end at the end of table
        @param limit The maximum number of keys to read, or None, if no limit shall be imposed
        @param keyFilter If this is non-None, the server filters for keys containing (exactly) this substring
        @param valueFilter If this is non-None, the server filters for values containing (exactly) this substring
        @param invert Set this to true to invert the scan direction
        @return A dictionary of the returned key/value pairs
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        self._checkParameterType(limit, int, "limit",  allowNone=True)
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Use stream object to store callback data
        stream = self.__initStream(callback, {"mapData":mapData})
        #Send header frame
        msgParts = ["\x31\x01\x13" + ("\x01" if invert else "\x00")]
        #Create the table number frame
        msgParts.append(struct.pack('<I', tableNo))
        #Send limit frame
        msgParts.append("" if limit is None else struct.pack('<q', limit))
        #Send range. "" --> empty frame --> start/end of table
        msgParts += TornadoConnection._rangeToFrames(startKey, endKey)
        #Send key filter parameters
        msgParts.append("" if keyFilter is None else keyFilter)
        #Send value filter parameters
        msgParts.append("" if keyFilter is None else valueFilter)
        #Send & callback after sending all frames finished
        stream.on_recv_stream(TornadoConnection.__onScanRecvFinish)
        stream.send_multipart(msgParts)
    @staticmethod
    def __onScanRecvFinish(stream, response):
        TornadoConnection._checkHeaderFrame(response, '\x13') #Remap the returned key/value pairs to a dict
        dataParts = response[1:]
        #Build return data
        if stream.options["mapData"]:
            data = YakDBConnectionBase._mapScanToDict(dataParts)
        else:
            data = YakDBConnectionBase._mapScanToTupleList(dataParts)
        #Call callback
        stream.callback(data)
    def read(self, tableNo, keys, callback, mapKeys=False):
        """
        Read one or multiples values, identified by their keys, from a table.

        @param tableNo The table number to read from
        @param keys A list, tuple or single value.
                        Must only contain strings, ints or floats.
                        integral types are automatically mapped to signed 32-bit little-endian binary,
                        floating point types are mapped to signed little-endian 64-bit IEEE754 values.
                        If you'd like to use another binary representation, use a binary string instead.
        @param mapKeys If this is set to true, a mapping from the original keys to 
                        values is performed, the return value is a dictionary key->value
                        rather than a value list. Mapping keys introduces additional overhead.
        @return A list of values, correspondent to the key order (or a dict, depends on mapKeys parameter)
        """
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Use stream object to store callback data
        stream = self.__initStream(callback, {"mapKeys": mapKeys})
        #Check parameters and create binary-string only key list
        self.__class__._checkParameterType(tableNo, int, "tableNo")
        convertedKeys = ZMQBinaryUtil.convertToBinaryList(keys)
        #Send header frame
        msgParts = ["\x31\x01\x10"]
        #Send the table number frame
        msgParts.append(struct.pack('<I', tableNo))
        #Send key list
        #This is a bit simpler than the normal read() because we don't have to deal with SNDMORE
        msgParts += convertedKeys
        #Send & Wait for reply
        stream.on_recv_stream(TornadoConnection.__onReadRecvFinish)
        stream.send_multipart(msgParts)
    @staticmethod
    def __onReadRecvFinish(stream, response):
        TornadoConnection._checkHeaderFrame(response, '\x10') #Remap the returned key/value pairs to a dict
        dataParts = response[1:]
        #Build return data
        if not stream.options["mapKeys"]:
            data= response[1:]
        else: #Perform key-value mapping
            res = {}
            #For mapping we need to ensure 'keys' is array-ish
            if type(keys) is not list and type(keys) is not tuple:
                keys = [keys]
            #Do the key-value mapping
            values = response[1:]
            for i in range(len(values)):
                res[keys[i]] = values[i]
            data = res
        #Call callback
        stream.callback(data)
    def put(self, tableBlocksizeNo, valueDict, callback, partsync=False, fullsync=False):
        """
        Write a dictionary of key-value pairs to the connected servers.
        
        This request can be used in REQ/REP, PUSH/PULL and PUB/SUB mode.

        @param tableNo The numeric, unsigned table number to write to
        @param callback A function(param) which is called with the result when finished
        @param valueDict A dictionary containing the key/value pairs to be written.
                        Must not contain None keys or values.
                        integral types are automatically mapped to signed 32-bit little-endian binary,
                        floating point types are mapped to signed little-endian 64-bit IEEE754 values.
                        If you'd like to use another binary representation, use a binary string instead!
        @param partsync If set to true, subsequent reads are guaranteed to return the written values
        @param fullsync If set to true, written data is synced to disk after being written.
        @return True on success, else an appropriate exception will be raised
        """
        #Check parameters
        self.__class__._checkParameterType(tableNo, int, "tableNo")
        self.__class__._checkParameterType(valueDict, dict, "valueDict")
        #Before sending any frames, check the value dictionary for validity
        #Else, the socket could be left in an inconsistent state
        if len(valueDict) == 0:
            raise ParameterException("Dictionary to be written did not contain any valid data!")
        Connection.__checkDictionaryForNone(valueDict)
        #Use stream object to store callback data
        stream = self.__initStream(callback)
        #Send header frame
        msgParts = [__getWriteHeader("\x20", partsync, fullsync)]
        #Send the table number
        msgParts.append(struct.pack('<I', tableNo))
        #Send key/value pairs
        for key, value in valueDict.iteritems():
            msgParts.append(ZMQBinaryUtil.convertToBinary(key))
            msgParts.append(ZMQBinaryUtil.convertToBinary(value))
        #If this is a req/rep connection, receive a reply
        if self.mode is zmq.REQ:
            #Send, call callback when received
            stream.on_recv_stream(TornadoConnection.__onPutRecvFinish)
        else:
            #Send, call callback when sent
            stream.on_send_stream(TornadoConnection.__onPutSendFinish)
        stream.send_multipart(msgParts)
    @staticmethod
    def __onPutRecvFinish(stream, response):
        TornadoConnection._checkHeaderFrame(response, '\x20')
        stream.callback()
    @staticmethod
    def __onPutSendFinish(stream, msg, status):
        stream.callback()
