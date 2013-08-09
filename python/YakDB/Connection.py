#!/usr/bin/env python
# -*- coding: utf8 -*-
import struct
#Local imports
from Conversion import ZMQBinaryUtil
from Exceptions import *
import DataProcessor
#Use ZMQPy inside PyPy
import platform
if platform.python_implementation() == "PyPy":
    import zmqpy as zmq
else:
    import zmq


class Connection:
    def __init__(self, context=None):
        if context is None:
            self.context = zmq.Context()
            self.cleanupContextOnDestruct = True
        else:
            self.context = context
            self.cleanupContextOnDestruct = False
        self.socket = None
        self.numConnections = 0
    def __del__(self):
        """
        Cleanup ZMQ resources.
        Auto-destroys context if the context was created in the constructor.
        """
        if self.socket is not None:
            self.socket.close()
        if self.cleanupContextOnDestruct:
            self.context.destroy()
    def useRequestReplyMode(self):
        """Sets the current ZeroDB connection into Request/reply mode (default)"""
        self.socket = self.context.socket(zmq.REQ)
        self.mode = zmq.REQ
    def usePushMode(self):
        """Sets the current ZeroDB connection into Push/pull mode (default)"""
        self.socket = self.context.socket(zmq.PUSH)
        self.mode = zmq.PUSH
    def usePubMode(self):
        """Sets the current ZeroDB connection into publish/subscribe mode"""
        self.socket = self.context.socket(zmq.PUB)
        self.mode = zmq.PUB
    def connect(self, endpoint):
        """Connect to a ZeroDB server"""
        #Use request/reply as default
        if self.socket == None:
            self.useRequestReplyMode()
        self.socket.connect(endpoint)
        self.numConnections += 1
    def _checkSingleConnection(self):
        """
        Check if the current instance is connected to
        exactly one server. If not, raise an exception.
        """
        if self.numConnections > 1:
            raise ConnectionStateException("This operation can only be executed with exactly one connection, but currently %d connections are active" % self.numConnections)
    def _checkRequestReply(self):
        """
        Check if the current instance is correctly setup for REQ/REP, else raise an exception
        """
        self._checkConnection()
        if self.mode is not zmq.REQ:
            raise Exception("Only request/reply connections support server info msgs")
    def _checkConnection(self):
        """Check if the current instance is correctly connected, else raise an exception"""
        if self.socket is None:
            raise Exception("Please connect to server before using serverInfo (use ZeroDBConnection.connect()!")
        if self.numConnections <= 0:
            raise Exception("Current ZeroDBConnection is setup, but not connected. Please connect before usage!")
    def _sendBinary32(self, value, more=True): ZMQBinaryUtil.sendBinary32(self.socket,  value,  more)
    def _sendBinary64(self, value, more=True): ZMQBinaryUtil.sendBinary64(self.socket,  value,  more)
    def _sendRange(self, fromKey,  toKey,  more=False):
        """
        Send a dual-frame range over the socket.
        If any of the keys is None or empty, a zero-sized frame is sent
        @param more If this is set to true, not only the range start frame but also the range end frame is sent
            with the ZMQ_SNDMORE flag
        """
        if fromKey is not None: fromKey = ZMQBinaryUtil.convertToBinary(fromKey)
        else: fromKey = ""
        if toKey is not None: toKey = ZMQBinaryUtil.convertToBinary(toKey)
        else: toKey = ""
        self.socket.send(fromKey, zmq.SNDMORE)
        self.socket.send(toKey,  (zmq.SNDMORE if more else 0))
    def _checkHeaderFrame(self,  msgParts,  expectedResponseType):
        """
        Given a list of received message parts, checks the first message part.
        Checks performed:
            - Magic byte (expects 0x31)
            - Version byte (expects 0x01)
            - Response code (expects 0x00, else it assumes that the next frame, is the error msg)
        This function throws an exception on check failure and exits normally on success
        """
        if len(msgParts) == 0:
            raise YakDBProtocolException("Received empty reply message")
        if len(msgParts[0]) < 4:
            raise YakDBProtocolException("Header frame has size of %d, but expected 4" % len(msgParts[0]))
        if msgParts[0][2] != expectedResponseType:
            raise YakDBProtocolException("Response code received from server is "
                        "%d instead of %d" % (ord(msgParts[0][2]),  ord(expectedResponseType)))
        if msgParts[0][3] != '\x00':
            errorMsg = msgParts[1] if len(msgParts) >= 2 else "<Unknown>"
            raise YakDBProtocolException(
                "Response status code is %d instead of 0x00 (ACK), error message: %s"
                % (ord(msgParts[0][3]),  errorMsg))
    def serverInfo(self):
        """
        Send a server info request to the client
        @return The server version string
        """
        self._checkRequestReply()
        self._checkSingleConnection()
        self.socket.send("\x31\x01\x00")
        #Check reply message
        replyParts = self.socket.recv_multipart(copy=True)
        if len(replyParts) != 2:
            raise Exception("Expected to receive 2-part message from the server, but got %d" % len(replyParts))
        responseHeader = replyParts[0]
        if not responseHeader.startswith("\x31\x01\x00"):
            raise Exception("Response header frame contains invalid header: %d %d %d" % (ord(responseHeader[0]), ord(responseHeader[1]), ord(responseHeader[2])))
        #Return the server version string
        return replyParts[1]
    def _checkParameterType(self, value, expectedType, name,  allowNone=False):
        """
        Raises a ParameterException if the given value does not have the given type
        @param allowNone If this is set to true, a 'None' value is also allowed and doesn't throw
        """
        if allowNone and value is None:
            return
        if type(value) is not expectedType:
            raise ParameterException("Parameter '%s' is not a %s but a %s!" % (name, str(expectedType), str(type(value))))
    def put(self, tableNo, valueDict, partsync=False, fullsync=False):
        """
        Write a dictionary of key-value pairs to the connected servers

        @param tableNo The numeric, unsigned table number to write to
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
        self._checkParameterType(tableNo, int, "tableNo")
        self._checkParameterType(valueDict, dict, "valueDict")
        #Check if this connection instance is setup correctly
        self._checkRequestReply()
        #Before sending any frames, check the value dictionary for validity
        #Else, the socket could be left in an inconsistent state
        if len(valueDict) == 0:
            raise ParameterException("Dictionary to be written did not contain any valid data!")
        for key in valueDict.iterkeys():
            value = valueDict[key]
            #None keys or values are not supported, they can't be mapped to binary!
            # Use empty strings if neccessary.
            if key is None:
                raise ParameterException("'None' keys are not supported by ZeroDB!")
            if value is None:
                raise ParameterException("'None' values are not supported by ZeroDB!")
        #Send header frame
        flags = 0
        if partsync: flags |= 1
        if fullsync: flags |= 2
        headerStr = "\x31\x01\x20" + chr(flags)
        self.socket.send(headerStr, zmq.SNDMORE)
        #Send the table number
        self._sendBinary32(tableNo)
        #Send key/value pairs
        nextToSend = None #Needed because the last value shall be sent w/out SNDMORE
        for key in valueDict.iterkeys():
            #Send the value from the last loop iteration
            if nextToSend is not None: self.socket.send(nextToSend, zmq.SNDMORE)
            #Map key to binary data if neccessary
            value = ZMQBinaryUtil.convertToBinary(valueDict[key])
            #Send the key and enqueue the value
            self.socket.send(key, zmq.SNDMORE)
            nextToSend = value
        #If nextToSend is None now, the dict didn't contain valid data
        #Send the last value without SNDMORE
        self.socket.send(nextToSend)
        #If this is a req/rep connection, receive a reply
        if self.mode is zmq.REQ:
            msgParts = self.socket.recv_multipart(copy=True)
            self._checkHeaderFrame(msgParts,  '\x20')
    def delete(self, tableNo, keys):
        """
        Delete one or multiples values, identified by their keys, from a table.

        @param tableNo The table number to delete in
        @param keys A list, tuple or single value.
                        Must only contain strings, ints or floats.
                        integral types are automatically mapped to signed 32-bit little-endian binary,
                        floating point types are mapped to signed little-endian 64-bit IEEE754 values.
                        If you'd like to use another binary representation, use a binary string instead.
        """
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        convertedKeys = []
        if type(keys) is list or type(keys) is tuple:
            for value in keys:
                if value is None:
                    raise ParameterException("Key list contains 'None' value, not mappable to binary")
                convertedKeys.append(ZMQBinaryUtil.convertToBinary(value))
        elif (type(keys) is str) or (type(keys) is int) or (type(keys) is float):
            #We only have a single value
            convertedKeys.append(ZMQBinaryUtil.convertToBinary(keys))
        #Send header frame
        self.socket.send("\x31\x01\x21", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        #Send key list
        nextToSend = None #Needed because the last value shall be sent w/out SNDMORE
        for key in convertedKeys:
            #Send the value from the last loop iteration
            if nextToSend is not None: self.socket.send(nextToSend, zmq.SNDMORE)
            #Send the key and enqueue the value
            nextToSend = key
        #Send last key, without SNDMORE flag
        self.socket.send(nextToSend)
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x21')
    def read(self, tableNo, keys, mapKeys=False):
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
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        convertedKeys = []
        if type(keys) is list or type(keys) is tuple:
            for value in keys:
                if value is None:
                    raise ParameterException("Key list contains 'None' value, not mappable to binary")
                convertedKeys.append(ZMQBinaryUtil.convertToBinary(value))
        elif (type(keys) is str) or (type(keys) is int) or (type(keys) is float):
            #We only have a single value
            convertedKeys.append(ZMQBinaryUtil.convertToBinary(keys))
        #Send header frame
        self.socket.send("\x31\x01\x10", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        #Send key list
        nextToSend = None #Needed because the last value shall be sent w/out SNDMORE
        for key in convertedKeys:
            #Send the value from the last loop iteration
            if nextToSend is not None: self.socket.send(nextToSend, zmq.SNDMORE)
            #Send the key and enqueue the value
            nextToSend = key
        #Send last key, without SNDMORE flag
        self.socket.send(nextToSend)
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x10')        #Return the data frames
        if not mapKeys:
            return msgParts[1:]
        else: #Perform key-value mapping
            res = {}
            #For mapping we need to ensure 'keys' is array-ish
            if type(keys) is not list and type(keys) is not tuple:
                keys = [keys]
            #Do the key-value mapping
            values = msgParts[1:]
            for i in range(len(values)):
                res[keys[i]] = values[i]
            return res
    def scan(self, tableNo, fromKey=None, toKey=None):
        """
        Synchronous scan. Scans an entire range at once.

        See self.read() documentation for an explanation of how
        non-str values are mapped.

        @param tableNo The table number to scan in
        @param fromKey The first key to scan, inclusive, or None or "" (both equivalent) to start at the beginning
        @param toKey The last key to scan, exclusive, or None or "" (both equivalent) to end at the end of table
        @return A dictionary of the returned key/value pairs
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send("\x31\x01\x13", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        #Send range. "" --> empty frame --> start/end of table
        self._sendRange(fromKey,  toKey)
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x13') #Remap the returned key/value pairs to a dict
        dataParts = msgParts[1:]
        mappedData = {}
        for i in range(0,len(dataParts),2):
            mappedData[dataParts[i]] = dataParts[i+1]
        return mappedData
    def scanWithLimit(self, tableNo, fromK
        self._checkSingleConnection()ey, limit):
        """
        Synchronous limited scan.
        Returns up to a given limit of key-value pairs, starting
        at the given start key

        See self.read() documentation for an explanation of how
        non-str values are mapped.

        @param tableNo The table number to scan in
        @param fromKey The first key to scan, inclusive, or None or "" (both equivalent) to start at the beginning
        @param limit The maximum number of keys to return
        @return A dictionary of the returned key/value pairs
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        self._checkParameterType(limit, int, "limit")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send("\x31\x01\x14", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        #Send range. "" --> empty frame --> start/end of tabe
        if fromKey is not None: fromKey = ZMQBinaryUtil.convertToBinary(fromKey)
        if fromKey is None: fromKey = ""
        self.socket.send(fromKey, zmq.SNDMORE)
        self._sendBinary64(limit, more=False)
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x14')
        #Remap the returned key/value pairs to a dict
        dataParts = msgParts[1:]
        mappedData = {}
        for i in range(0,len(dataParts),2):
            mappedData[dataParts[i]] = dataParts[i+1]
        return mappedData
    def deleteRange(self, tableNo, fromKey, toKey):
        """
        Deletes a range of keys in the database

        @param tableNo The table number to scan in
        @param fromKey The first key to scan, inclusive, or None or "" (both equivalent) to start at the beginning
        @param toKey The last key to scan, exclusive, or None or "" (both equivalent) to end at the end of table
        @return A dictionary of the returned key/value pairs
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send("\x31\x01\x22", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo, more=True)
        #Send range. "" --> empty frame --> start/end of tabe
        self._sendRange(fromKey,  toKey)
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x22')
    def deleteRangeLimited(self, tableNo, fromKey, limit):
        """
        Deletes a range of keys in the database. This is a version of deleteRange() that allows
        you to specify a maximum number of keys to scan, instead of an end key

        @param tableNo The table number to scan in
        @param fromKey The first key to scan, inclusive, or None or "" (both equivalent) to start at the beginning
        @param toKey The last key to scan, exclusive, or None or "" (both equivalent) to end at the end of table
        @return A dictionary of the returned key/value pairs
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        self._checkParameterType(limit, int, "limit")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send("\x31\x01\x23", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        #Send range. "" --> empty frame --> start/end of tabe
        if fromKey is not None: fromKey = ZMQBinaryUtil.convertToBinary(fromKey)
        else: fromKey = ""
        self.socket.send(fromKey, zmq.SNDM
        self._checkSingleConnection()ORE)
        self._sendBinary64(limit, more=False)
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x23')
    def count(self, tableNo, fromKey, toKey):
        """
        Count a range of

        See self.read() documentation for an explanation of how
        non-str values are mapped.

        @param tableNo The table number to scan in
        @param fromKey The first key to scan, inclusive, or None or "" (both equivalent) to start at the beginning
        @param toKey The last key to scan, exclusive, or None or "" (both equivalent) to end at the end of table
        @return The count, as integer
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send("\x31\x01\x11", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        #Send range. "" --> empty frame --> start/end of table
        self._sendRange(fromKey,  toKey)
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x11')
        #Deserialize
        binaryCount = msgParts[1]
        count = struct.unpack("<Q", binaryCount)[0]
        return count
    def exists(self, tableNo, keys):
        """
        Chec one or multiples values, identified by their keys, for existence in a given table.

        @param tableNo The table number to read from
        @param keys A list, tuple or single value.
                        Must only contain strings, ints or floats.
                        integral types are automatically mapped to signed 32-bit little-endian binary,
                        floating point types are mapped to signed little-endian 64-bit IEEE754 values.
                        If you'd like to use another binary representation, use a binary string instead.
        @return A list of values, correspondent to the key order
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        convertedKeys = []
        if type(keys) is list or type(keys) is tuple:
            for value in keys:
                if value is None:
                    raise ParameterException("Key list contains 'None' value, not mappable to binary")
                convertedKeys.append(ZMQBinaryUtil.convertToBinary(value))
        elif (type(keys) is str) or (type(keys) is int) or (type(keys) is float):
            #We only have a single value
            convertedKeys.append(ZMQBinaryUtil.convertToBinary(keys))
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send("\x31\x01\x12", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        #Send key list
        nextToSend = None #Needed because the last value shall be sent w/out SNDMORE
        for key in convertedKeys:
            #Send the value from the last loop iteration
            if nextToSend is not None: self.socket.send(nextToSend, zmq.SNDMORE)
            #Send the key and enqueue the value
            nextToSend = key
        #Send last key, without SNDMORE flag
        self.socket.send(nextToSend)
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x12')
        #Return the data frames after mapping them to bools
        processedValues = []
        for msgPart in msgParts[1:]:
            processedValues.append(False if msgPart == "\x00" else True)
        return processedValues
    def openTable(self, tableNo, compression=True, lruCacheSize=None, writeBufferSize=None, tableBlocksize=None, bloomFilterBitsPerKey=None):
        """
        Open a table.

        This is usually not neccessary, because tables are opened on-the-fly
        when they are accessed. Opening tables is slow, however,
        so this decreases latency and counteracts the possibility
        of work piling up for threads that are waiting for a table to be opened.

        Additionally, this method of opening tables allows setting
        table parameters whereas on-the-fly-open always assumes defaults

        @param tableNo The table number to open
        @param compression Set this to false to disable blocklevel snappy compression
        @param lruCacheSize The LRU cache size in bytes, or None to assume default
        @param tableBlocksize The table block size in bytes, or None to assume default
        @param writeBufferSize The table write buffer size, or None to assume defaults
        @parameter bloomFilterBitsPerKey If this is set to none, no bloom filter is used, else a bloom filter with the given number of bits per key is used.
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        self._checkParameterType(lruCacheSize,  int,  "lruCacheSize",  allowNone=True)
        self._checkParameterType(tableBlocksize,  int,  "tableBlocksize",  allowNone=True)
        self._checkParameterType(writeBufferSize,  int,  "writeBufferSize",  allowNone=True)
        self._checkParameterType(bloomFilterBitsPerKey,  int,  "bloomFilterBitsPerKey",  allowNone=True)
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        headerFrame = "\x31\x01\x01" + ("\x00" if compression else "\x01")
        self.socket.send(headerFrame, zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        #Send LRU, blocksize and write buffer size
        self._sendBinary64(lruCacheSize)
        self._sendBinary64(tableBlocksize)
        self._sendBinary64(writeBufferSize)
        self._sendBinary64(bloomFilterBitsPerKey, more=False)
        #Receive and extract response code
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x01')
    def truncateTable(self, tableNo):
        """
        Close & truncate a table.
        @param tableNo The table number to truncate
        @return
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send("\x31\x01\x04", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo, 0) #No SNDMORE flag
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x04')
    def closeTable(self, tableNo):
        """
        Close a table.
        Usually not neccessary, unless you want to save memory
        @param tableNo The table number to truncate
        @return
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send("\x31\x01\x02", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo, more=False) #No SNDMORE flag
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x02')
    def compactRange(self, tableNo, fromKey=None, toKey=None):
        """
        Compact a range in a table.
        This operation might take a long time and might not be useful at all
        under certain circumstances
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send("\x31\x01\x03", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo, more=true)
        self._sendRange(fromKey,  toKey)
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x03')
    def initializePassiveDataJob(self, tableNo, fromKey=None, toKey=None,  chunksize=None):
        """
        Initialize a job on the server that waits for client requests.
        @param tableNo The table number to scan in
        @param fromKey The first key to scan, inclusive, or None or "" (both equivalent) to start at the beginning
        @param toKey The last key to scan, exclusive, or None or "" (both equivalent) to end at the end of table
        @param chunksize How many key/value pairs will be returned for a single request. None --> Serverside default
        @return A PassiveDataJob instance, exposing requestDataBlock()
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        self._checkParameterType(chunksize, int, "chunksize",  allowNone=True)
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send("\x31\x01\x42",  zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        self._sendBinary32(chunksize)
        #Send range to be scanned
        self._sendRange(fromKey,  toKey)
        #Receive response
        msgParts = self.socket.recv_multipart(copy=True)
        self._checkHeaderFrame(msgParts,  '\x42')
        if len(msgParts) < 2:
            raise YakDBProtocolException("CSPTMIR response does not contain APID frame")
        #Get the APID and create a new job instance
        apid = struct.unpack('<q', msgParts[1])[0]
        return DataProcessor.ClientSidePassiveJob(self,  apid)
    def _requestJobDataChunk(self,  apid):
        """
        Requests a data chunk for a given asynchronous Job.
        May only be used for client-side passive jobs.
        @param apid The Asynchronous Process ID
        """
        self._checkParameterType(apid, int, "apid")
        self._checkRequestReply()
        #Send header frame
        self.socket.send("\x31\x01\x50", zmq.SNDMORE)
        #Send APID
        self._sendBinary64(apid,  more=False)
        #Receive response chunk
        msgParts = self.socket.recv_multipart(copy=True)
        #A response code of 0x01 or 0x02 also indicates success
        if len(msgParts) >= 1 and len(msgParts[0]) > 2:
            hdrList = list(msgParts[0]) #Strings are immutable!
            hdrList[3] = '\x00' #Otherwise _checkHeaderFrame would fail
            msgParts[0] = b"".join(hdrList)
        self._checkHeaderFrame(msgParts,  '\x50')
        #We silently ignore the partial data / no data flags from the header,
        # because we can simply deduce them from the data frames.
        dataParts = msgParts[1:]
        mappedData = {}
        for i in range(0,len(dataParts),2):
            mappedData[dataParts[i]] = dataParts[i+1]
        return mappedData