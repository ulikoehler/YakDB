#!/usr/bin/env python
"""
Asynchronous YakDB connection that works with the tornado IO loop.
Automatically installs itself as tornado IO loop.

Currently in development. Supports only a small subset of available requests.
"""
import YakDB.Connection
import struct
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream
ioloop.install()

class TornadoConnection(YakDB.Connection.Connection):
    """
    An instance of this class represents a connection to a YakDB database.
    This thin wrapper uses asynchronicity together with the Tornado IO loop.
    
    This class provides a reentrant wrapper that may be called only from a single
    thread but from multiple IO-loop-based eventlets in that thread.
    """
    def __initStream(self, callback, expectedRetCode, options={}):
        """
        Initialize a new stream over the current connection.
        """
        stream = ZMQStream(self.socket)
        stream.callback = callback
        stream.expectedRetCode = expectedRetCode
        return stream
    @staticmethod
    def _rangeToFrames(startKey, endKey):
        """
        Convert a start and an end key to a list of two frames
        """
        if startKey is not None: startKey = ZMQBinaryUtil.convertToBinary(startKey)
        else: startKey = ""
        if endKey is not None: endKey = ZMQBinaryUtil.convertToBinary(endKey)
        else: endKey = ""
        return [startKey, endKey]
    def scan(self, tableNo, callback, callbackParam=None, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, invert=False, mapData=True):
        """
        Asynchronous reentrant scan. Scans an entire range at once.
        The scan stops at the table end, endKey (exclusive) or when
        *limit* keys have been read, whatever occurs first

        See self.read() documentation for an explanation of how
        non-str values are mapped.

        @param tableNo The table number to scan in
        @param callback A function(param, scanResult) which is called with the custom defined parameter
        @param callbackParam The first parameter for the callback function. Any value or object is allowed.
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
        stream = self.__initStream(callback, '\x13')
        #Send header frame
        msgParts = ["\x31\x01\x13" + ("\x01" if invert else "\x00")]
        #Create the table number frame
        msgParts.append(struct.pack('<I', tableNo))
        #Send limit frame
        msgParts.append("" if limit is None else struct.pack('<q', limit))
        #Send range. "" --> empty frame --> start/end of table
        msgParts.append(struct.pack('<q', limit))
        msgParts += TornadoConnection.__rangeToFrames(startKey, endKey)
        #Send key filter parameters
        msgParts.append("" if keyFilter is None else keyFilter)
        #Send value filter parameters
        msgParts.append("" if keyFilter is None else valueFilter)
        #Send & callback after sending all frames finished
        stream.send_multipart(msgParts)
        stream.on_recv_multipart(__onScanRecvFinish)
    def __onScanRecvFinish(msg, stream):
        self._checkHeaderFrame(msgParts,  '\x13') #Remap the returned key/value pairs to a dict
        dataParts = msgParts[1:]
        mappedData = {}
        for i in range(0,len(dataParts),2):
            mappedData[dataParts[i]] = dataParts[i+1]
        stream.callback(stream.callbackParma)
    def __onSendFinish():
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True, )
    def put(self, tableNo, valueDict, callback, callbackParam=None, partsync=False, fullsync=False):
        """
        Write a dictionary of key-value pairs to the connected servers.
        
        This request can be used in REQ/REP, PUSH/PULL and PUB/SUB mode.

        @param tableNo The numeric, unsigned table number to write to
        @param callback A function(param, scanResult) which is called with the custom defined parameter
        @param callbackParam The first parameter for the callback function. Any value or object is allowed.
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
        for key in valueDict.iterkeys():
            value = valueDict[key]
            #None keys or values are not supported, they can't be mapped to binary!
            # Use empty strings if neccessary.
            if key is None:
                raise ParameterException("'None' keys are not supported!")
            if value is None:
                raise ParameterException("'None' values are not supported!")
        #Use stream object to store callback data
        stream = self.__initStream(callback, '\x20')
        #Send header frame
        flags = 0
        if partsync: flags |= 1
        if fullsync: flags |= 2
        headerStr = "\x31\x01\x20" + chr(flags)
        msgParts = [headerStr]
        #Send the table number
        msgParts.append(struct.pack('<I', tableNo))
        #Send key/value pairs
        nextToSend = None #Needed because the last value shall be sent w/out SNDMORE
        for key in valueDict.iterkeys():
            #Send the value from the last loop iteration
            if nextToSend is not None: msgParts.append(nextToSend)
            #Map key to binary data if neccessary
            value = ZMQBinaryUtil.convertToBinary(valueDict[key])
            #Send the key and enqueue the value
            msgParts.append(key)
            nextToSend = value
        #If nextToSend is None now, the dict didn't contain valid data
        #Send the last value without SNDMORE
        msgParts.append(nextToSend)
        #If this is a req/rep connection, receive a reply
        if self.mode is zmq.REQ:
            msgParts = self.socket.recv_multipart(copy=True)
            self.__class__._checkHeaderFrame(msgParts,  '\x20')
    #TODO
            
        