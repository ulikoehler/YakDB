#!/usr/bin/env python3
# -*- coding: utf8 -*-
import struct
#Local imports
from YakDB.Conversion import ZMQBinaryUtil
from YakDB.Exceptions import ParameterException, YakDBProtocolException
from YakDB.DataProcessor import ClientSidePassiveJob
from YakDB.ConnectionBase import YakDBConnectionBase
import zmq

class Connection(YakDBConnectionBase):
    """
    An instance of this class represents a connection to a YakDB database.
    """
    def serverInfo(self, requestId=b""):
        """
        Send a server info request to the server and return the version string
        @return The server version string
        """
        self._checkRequestReply()
        self._checkSingleConnection()
        self.socket.send(b"\x31\x01\x00" + requestId)
        #Check reply message
        replyParts = self.socket.recv_multipart(copy=True)
        if len(replyParts) != 2:
            raise Exception("Expected to receive 2-part message from the server, but got %d"
                            % len(replyParts))
        responseHeader = replyParts[0]
        if not responseHeader.startswith(b"\x31\x01\x00"):
            raise Exception("Response header frame contains invalid header: %d %d %d"
                            % (responseHeader[0], responseHeader[1], responseHeader[2]))
        #Return the server version string
        return replyParts[1]
    def tableInfo(self, tableNo=1):
        """
        Get table info for a single table number
        @return A dictionary of table information
        """
        self._checkRequestReply()
        self._checkSingleConnection()
        #Send request
        self.socket.send(b"\x31\x01\x06", zmq.SNDMORE)
        self._sendBinary32(tableNo, more=False)
        #Check reply message
        replyParts = self.socket.recv_multipart(copy=True)
        YakDBConnectionBase._checkHeaderFrame(replyParts, b'\x06')
        return YakDBConnectionBase._mapScanToDict(replyParts[1:])
    def put(self, tableNo, valueDict, partsync=False, fullsync=False, requestId=b""):
        """
        Write a dictionary of key-value pairs to the connected servers.

        This request can be used in REQ/REP, PUSH/PULL and PUB/SUB mode.

        If the value dictionary is empty, no operation is performed.

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
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
        YakDBConnectionBase._checkParameterType(valueDict, dict, "valueDict")
        #Before sending any frames, check the value dictionary for validity
        #Else, the socket could be left in an inconsistent state
        if len(valueDict) == 0:
            return
        YakDBConnectionBase._checkDictionaryForNone(valueDict)
        #Send header frame
        self.socket.send(YakDBConnectionBase._getWriteHeader(b"\x20", partsync, fullsync, requestId), zmq.SNDMORE)
        #Send the table number
        self._sendBinary32(tableNo)
        #Send key/value pairs
        nextToSend = None #Needed because the last value shall be sent w/out SNDMORE
        for key, value in valueDict.items():
            #Send the value from the last loop iteration
            if nextToSend is not None: self.socket.send(nextToSend, zmq.SNDMORE)
            #Map key & value to binary data if neccessary
            key = ZMQBinaryUtil.convertToBinary(key)
            value = ZMQBinaryUtil.convertToBinary(value)
            #Send the key and enqueue the value
            self.socket.send(key, zmq.SNDMORE)
            nextToSend = value
        #If nextToSend is None now, the dict didn't contain valid data
        #Send the last value without SNDMORE
        self.socket.send(nextToSend)
        #If this is a req/rep connection, receive a reply
        if self.mode is zmq.REQ:
            msgParts = self.socket.recv_multipart(copy=True)
            YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x20')
    def delete(self, tableNo, keys, partsync=False, fullsync=False, requestId=b""):
        """
        Delete one or multiples values, identified by their keys, from a table.

        This request may be used in REQ/REP and PUSH/PULL mode.

        @param tableNo The table number to delete in
        @param keys A list, tuple or single value.
                        Must only contain strings, ints or floats.
                        integral types are automatically mapped to signed 32-bit little-endian binary,
                        floating point types are mapped to signed little-endian 64-bit IEEE754 values.
                        If you'd like to use another binary representation, use a binary string instead.
        """
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        #Check parameters and create binary-string only key list
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
        convertedKeys = ZMQBinaryUtil.convertToBinary(keys)
        #Send header frame
        self.socket.send(YakDBConnectionBase._getWriteHeader(b"\x21", partsync, fullsync, requestId), zmq.SNDMORE)
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
        if self.mode is zmq.REQ:
            msgParts = self.socket.recv_multipart(copy=True)
            YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x21')
    def read(self, tableNo, keys, mapKeys=False, requestId=b""):
        """
        Read one or multiples values, identified by their keys, from a table.

        @param tableNo The table number to read from
        @param keys A list, tuple or single value.
                        Must only contain strings/bytes, ints or floats.
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
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
        convertedKeys = ZMQBinaryUtil.convertToBinaryList(keys)
        #Send header frame
        self.socket.send(b"\x31\x01\x10" + requestId, zmq.SNDMORE)
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
        YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x10')
        #Return data frames
        values = msgParts[1:]
        if mapKeys:
            values = YakDBConnectionBase._mapReadKeyValues(keys, values)
        return values
    def scan(self, tableNo, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, skip=0, invert=False, mapData=False, requestId=b""):
        """
        Synchronous scan. Scans an entire range at once.
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
        @param skip The number of records to skip at the beginning. Filter mismatches do not count.
        @param invert Set this to True to invert the scan direction
        @param mapData If this is set to False, a list of tuples is returned instead of a directory
        @return A dictionary of the returned key/value pairs
        """
        #Check parameters and create binary-string only key list
        YakDBConnectionBase._checkParameterType(skip, int, "skip")
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
        YakDBConnectionBase._checkParameterType(limit, int, "limit",  allowNone=True)
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send(b"\x31\x01\x13" + (b"\x01" if invert else b"\x00") + requestId, zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        #Send limit frame
        self._sendBinary64(limit)
        #Send range. "" --> empty frame --> start/end of table
        self._sendRange(startKey,  endKey, more=True)
        #Send key filter parameters
        self.socket.send(b"" if keyFilter is None else keyFilter, zmq.SNDMORE)
        #Send value filter parameters
        self.socket.send(b"" if valueFilter is None else valueFilter, zmq.SNDMORE)
        #Send skip number
        self._sendBinary64(skip, more=False)
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True)
        YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x13') #Remap the returned key/value pairs to a dict
        dataParts = msgParts[1:]
        #Return appropriate data format
        if mapData:
            return YakDBConnectionBase._mapScanToDict(dataParts)
        else:
            return YakDBConnectionBase._mapScanToTupleList(dataParts)

    def list(self, tableNo, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, skip=0, invert=False, mapData=False, requestId=b""):
        """
        Synchronous list. Fully equivalent to scan, but only returns keys.
        See scan documentation for further reference.
        """
        #Check parameters and create binary-string only key list
        YakDBConnectionBase._checkParameterType(skip, int, "skip")
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
        YakDBConnectionBase._checkParameterType(limit, int, "limit",  allowNone=True)
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send(b"\x31\x01\x14" + (b"\x01" if invert else b"\x00") + requestId, zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        #Send limit frame
        self._sendBinary64(limit)
        #Send range. "" --> empty frame --> start/end of table
        self._sendRange(startKey,  endKey, more=True)
        #Send key filter parameters
        self.socket.send(b"" if keyFilter is None else keyFilter, zmq.SNDMORE)
        #Send value filter parameters
        self.socket.send(b"" if valueFilter is None else valueFilter, zmq.SNDMORE)
        #Send skip number
        self._sendBinary64(skip, more=False)
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True)
        YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x14') #Remap the returned key/value pairs to a dict
        dataParts = msgParts[1:]
        return dataParts
    def deleteRange(self, tableNo, startKey, endKey, limit=None):
        """
        Deletes a range of keys in the database
        The deletion stops at the table end, endKey (exclusive) or when
        *limit* keys have been read, whatever occurs first

        @param tableNo The table number to scan in
        @param startKey The first key to scan, inclusive, or None or "" (both equivalent) to start at the beginning
        @param endKey The last key to scan, exclusive, or None or "" (both equivalent) to end at the end of table
        @param limit The maximum number of keys to delete, or None, if no limit shall be imposed
        @return A dictionary of the returned key/value pairs
        """
        #Check parameters and create binary-string only key list
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send(b"\x31\x01\x22", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo, more=True)
        #Send range. "" --> empty frame --> start/end of tabe
        self._sendRange(startKey,  endKey)
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True)
        YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x22')
    def count(self, tableNo, startKey, endKey):
        """
        self._checkSingleConnection()
        Count a range of

        See self.read() documentation for an explanation of how
        non-str values are mapped.

        @param tableNo The table number to scan in
        @param startKey The first key to scan, inclusive, or None or "" (both equivalent) to start at the beginning
        @param endKey The last key to scan, exclusive, or None or "" (both equivalent) to end at the end of table
        @return The count, as integer
        """
        #Check parameters and create binary-string only key list
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send(b"\x31\x01\x11", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        #Send range. "" --> empty frame --> start/end of table
        self._sendRange(startKey,  endKey)
        #Wait for reply
        msgParts = self.socket.recv_multipart(copy=True)
        YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x11')
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
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
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
        self.socket.send(b"\x31\x01\x12", zmq.SNDMORE)
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
        YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x12')
        #Return the data frames after mapping them to bools
        processedValues = []
        for msgPart in msgParts[1:]:
            processedValues.append(False if msgPart == b"\x00" else True)
        return processedValues
    def openTable(self, tableNo, lruCacheSize=None, writeBufferSize=None, tableBlocksize=None, bloomFilterBitsPerKey=None, compression="SNAPPY", mergeOperator="REPLACE"):
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
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
        YakDBConnectionBase._checkParameterType(lruCacheSize, int, "lruCacheSize", allowNone=True)
        YakDBConnectionBase._checkParameterType(tableBlocksize, int, "tableBlocksize", allowNone=True)
        YakDBConnectionBase._checkParameterType(writeBufferSize, int, "writeBufferSize", allowNone=True)
        YakDBConnectionBase._checkParameterType(bloomFilterBitsPerKey, int, "bloomFilterBitsPerKey", allowNone=True)
        YakDBConnectionBase._checkParameterType(compression, str, "compression", allowNone=False)
        YakDBConnectionBase._checkParameterType(mergeOperator, str, "mergeOperator", allowNone=False)
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        headerFrame = b"\x31\x01\x01" + (b"\x00" if compression else b"\x01")
        self.socket.send(headerFrame, zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        #Send parameter map
        if lruCacheSize is not None:
            self.__sendDecimalParam(b"LRUCacheSize", lruCacheSize)
        if tableBlocksize is not None:
            self.__sendDecimalParam(b"Blocksize", tableBlocksize)
        if writeBufferSize  is not None:
            self.__sendDecimalParam(b"WriteBufferSize", writeBufferSize)
        if bloomFilterBitsPerKey is not None:
            self.__sendDecimalParam(b"BloomFilterBitsPerKey", bloomFilterBitsPerKey)
        self._sendBytesParam(b"MergeOperator", mergeOperator)
        self._sendBytesParam(b"CompressionMode", compression, flags=0)
        #Receive and etract response code
        msgParts = self.socket.recv_multipart(copy=True)
        YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x01')
    def truncate(self, tableNo):
        """
        Close & truncate a table.
        self._checkSingleConnection()
        @param tableNo The table number to truncate
        @return
        """
        #Check parameters and create binary-string only key list
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send(b"\x31\x01\x04", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo, 0) #No SNDMORE flag
        msgParts = self.socket.recv_multipart(copy=True)
        YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x04')
    def closeTable(self, tableNo):
        """
        Close a table.
        Usually not neccessary, unless you want to save memory
        @param tableNo The table number to truncate
        @return
        """
        #Check parameters and create binary-string only key list
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send(b"\x31\x01\x02", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo, more=False) #No SNDMORE flag
        msgParts = self.socket.recv_multipart(copy=True)
        YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x02')
    def stopServer(self):
        """
        Stop the YakDB server (by sending a stop request). Use with caution.
        """
        self.socket.send(b"\x31\x01\x05")
        if self.mode is zmq.REQ:
            response = self.socket.recv_multipart(copy=True)
            YakDBConnectionBase._checkHeaderFrame(response, b'\x05')
            #Check responseCode
            responseCode = response[0][3]
            if responseCode != 0:
                raise YakDBProtocolException("Server stop code was %d instead of 0 (ACK)" % responseCode)
    def compactRange(self, *args, **kwargs):
        """Alias for compact"""
        return self.compact(*args, **kwargs)
    def compact(self, tableNo, startKey=None, endKey=None):
        """
        Compact a range in a table.
        This operation might take a long time and might not be useful at all
        under certain circumstances
        """
        #Check parameters and create binary-string only key list
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send(b"\x31\x01\x03", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo, more=True)
        self._sendRange(startKey,  endKey)
        msgParts = self.socket.recv_multipart(copy=True)
        YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x03')
    def initializePassiveDataJob(self, tableNo, startKey=None, endKey=None, scanLimit=None, chunksize=None):
        """
        Initialize a job on the server that waits for client requests.
        @param tableNo The table number to scan in
        @param startKey The first key to scan, inclusive, or None or "" (both equivalent) to start at the beginning
        @param endKey The last key to scan, exclusive, or None or "" (both equivalent) to end at the end of table
        @param scanLimit The maximum number of keys to scan, or None (--> no limit)
       @param chunksize How many key/value pairs will be returned for a single request. None --> Serverside default
        @return A PassiveDataJob instance, exposing requestDataBlock()
        """
        #Check parameters and create binary-string only key list
        YakDBConnectionBase._checkParameterType(tableNo, int, "tableNo")
        YakDBConnectionBase._checkParameterType(chunksize, int, "chunksize",  allowNone=True)
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        self.socket.send(b"\x31\x01\x42", zmq.SNDMORE)
        #Send the table number frame
        self._sendBinary32(tableNo)
        self._sendBinary32(chunksize)
        self._sendBinary64(scanLimit)
        #Send range to be scanned
        self._sendRange(startKey,  endKey)
        #Receive response
        msgParts = self.socket.recv_multipart(copy=True)
        YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x42')
        if len(msgParts) < 2:
            raise YakDBProtocolException("CSPTMIR response does not contain APID frame")
        #Get the APID and create a new job instance
        apid = struct.unpack('<q', msgParts[1])[0]
        return ClientSidePassiveJob(self,  apid)
    def _requestJobDataChunk(self,  apid):
        """
        Requests a data chunk for a given asynchronous Job.
        May only be used for client-side passive jobs.
        Retunrs
        @param apid The Asynchronous Process ID
        """
        YakDBConnectionBase._checkParameterType(apid, int, "apid")
        self._checkRequestReply()
        #Send header frame
        self.socket.send(b"\x31\x01\x50", zmq.SNDMORE)
        #Send APID
        self._sendBinary64(apid, more=False)
        #Receive response chunk
        msgParts = self.socket.recv_multipart(copy=True)
        #A response code of 0x01 or 0x02 also indicates success
        if len(msgParts) >= 1 and len(msgParts[0]) > 2:
            hdrList = list(msgParts[0]) #Strings are immutable!
            hdrList[3] = 0 #Otherwise _checkHeaderFrame would fail
            msgParts[0] = bytes(hdrList)
        YakDBConnectionBase._checkHeaderFrame(msgParts, b'\x50')
        #We silently ignore the partial data / no data flags from the header,
        # because we can simply deduce them from the data frames.
        dataParts = msgParts[1:]
        mappedData = []
        for i in range(0, len(dataParts), 2):
            mappedData.append((dataParts[i], dataParts[i+1]))
        return mappedData
