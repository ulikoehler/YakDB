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
    def __initStream(self):
        """
        Initialize a new stream over the current connection.
        """
        return ZMQStream(self.socket)
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
    def scan(self, tableNo, callback, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None):
        """
        Asynchronous reentrant scan. Scans an entire range at once.
        The scan stops at the table end, endKey (exclusive) or when
        *limit* keys have been read, whatever occurs first

        See self.read() documentation for an explanation of how
        non-str values are mapped.

        @param tableNo The table number to scan in
        @param startKey The first key to scan, inclusive, or None or "" (both equivalent) to start at the beginning
        @param endKey The last key to scan, exclusive, or None or "" (both equivalent) to end at the end of table
        @param limit The maximum number of keys to read, or None, if no limit shall be imposed
        @param keyFilter If this is non-None, the server filters for keys containing (exactly) this substring
        @param valueFilter If this is non-None, the server filters for values containing (exactly) this substring
        @return A dictionary of the returned key/value pairs
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        self._checkParameterType(limit, int, "limit",  allowNone=True)
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Use stream object to store callback data
        stream = self.__initStream()
        stream.callback = callback
        stream.expectedRet
        #Send header frame
        msgParts = ["\x31\x01\x13"]
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
        #Callback after sending all frames finished
    #TODO
    def __onRecvFinish():
        self._checkHeaderFrame(msgParts,  '\x13') #Remap the returned key/value pairs to a dict
        dataParts = msgParts[1:]
        mappedData = {}
        for i in range(0,len(dataParts),2):
            mappedData[dataParts[i]] = dataParts[i+1]
        return mappedData
    def __onSendFinish():
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True, )
            
        