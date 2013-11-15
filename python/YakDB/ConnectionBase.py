#!/usr/bin/env python
# -*- coding: utf8 -*-
from YakDB.Conversion import ZMQBinaryUtil
from YakDB.Exceptions import ParameterException, YakDBProtocolException, ConnectionStateException
import collections
import random
import zmq
import struct

class YakDBConnectionBase(object):
    """
    Base class and algorithm provider for YakDB connections
    """
    def __init__(self, endpoints=None, context=None):
        """
        Create a new YakDB connection instance.
        @param endpoints An endpoint string or list of endpoints to connect to.
        New endpoints can be added dynamically.
        """
        if context is None:
            self.context = zmq.Context()
            self.cleanupContextOnDestruct = True
        else:
            self.context = context
            self.cleanupContextOnDestruct = False
        self.socket = None
        self.numConnections = 0
        #Connect to the endpoints, if any
        if endpoints is None:
            pass
        elif type(endpoints) is str:
            self.connect(endpoints)
        elif isinstance(connection, collections.Iterable):
            [self.connect(endpoint) for endpoint in endpoints]
        else: #Unknown parameter type
            raise ParameterException("Endpoints parameter is a '%s' but expected an iterable or a string!" % (str(type(endpoints))))
    def __del__(self):
        """
        Cleanup ZMQ resources.
        Auto-destroys context if the context was created in the constructor.
        """
        if self.socket is not None:
            self.socket.close()
        if self.cleanupContextOnDestruct:
            self.context.destroy()
    @staticmethod
    def _extractRequestId(responseHeader, requestExpectedSize=4):
        """
        From a response header frame, extracts the request ID sent back by the server
        
        >>> YakDBConnectionBase._extractRequestId("\\x31\\x01\\x10\\x00\\xBE\\xEF")
        '\\xbe\\xef'
        >>> YakDBConnectionBase._extractRequestId("\\x31\\x01\\x10\\xBE\\xEF", 3)
        '\\xbe\\xef'
        """
        return responseHeader[requestExpectedSize:]
    @staticmethod
    def _getWriteHeader(requestCode, partsync, fullsync, requestId):
        """Build the request header string including the write flags"""
        flags = (1 if partsync else 0) | (2 if fullsync else 0)
        return "\x31\x01%s%s%s" % (requestCode, chr(flags), requestId)
    @staticmethod
    def _checkDictionaryForNone(dictionary):
        """Throws a parameter exception if the given dict contains any None keys or values"""
        #None keys or values are not supported, they can't be mapped to binary!
        # Use empty strings if neccessary.
        if any(key == None for key in dictionary.iterkeys()) is None:
            raise ParameterException("Dictionary contains a key = None. Can't convert that to binary!")
        if any(value == None for value in dictionary.itervalues()) is None:
            raise ParameterException("Dictionary contains a value = None. Can't convert that to binary!")
    @staticmethod
    def _checkListForNone(thelist):
        """Throws a ParameterException if any of the values is None"""
        #None keys or values are not supported, they can't be mapped to binary!
        # Use empty strings if neccessary.
        if any(key == None for key in thelist) is None:
            raise ParameterException("Dictionary contains a key = None. Can't convert that to binary!")
    @staticmethod
    def _checkParameterType(value, expectedType, name,  allowNone=False):
        """
        Raises a ParameterException if the given value does not have the given type
        @param allowNone If this is set to true, a 'None' value is also allowed and doesn't throw
        """
        if allowNone and value is None:
            return
        if type(value) is not expectedType:
            raise ParameterException("Parameter '%s' is not a %s but a %s!" % (name, str(expectedType), str(type(value))))
    @staticmethod
    def _mapReadKeyValues(keys, values):
        """Maps the request keys and response values from a read request to a dictionary"""
        #For mapping we need to ensure 'keys' is array-ish
        if not isinstance(keys, collections.Iterable): keys = [keys]
        #If this fails, the keys list probably wasn't sent to the server
        assert len(keys) == len(values)
        #Dict comprehension, requires Python 2.7+
        return {key: values[i] for i, key in enumerate(keys)}
    @staticmethod
    def _mapScanToTupleList(dataParts):
        """
        Maps the resulting data (without the header frame) to a list of key-value tuples in the correct order
        """
        return [(dataParts[i], dataParts[i+1]) for i in range(0,len(dataParts),2)]
    @staticmethod
    def _mapScanToDict(dataParts):
        """
        Maps the resulting data (without the header frame) to a key-value dict
        """
        return {dataParts[i]: dataParts[i+1] for i in range(0,len(dataParts),2)}
    @staticmethod
    def _rangeToFrames(startKey, endKey):
        startKey = "" if startKey is None else ZMQBinaryUtil.convertToBinary(startKey)
        endKey = "" if endKey is None else ZMQBinaryUtil.convertToBinary(endKey)
        return [startKey, endKey]
    @staticmethod
    def _checkHeaderFrame(msgParts, expectedResponseType=None):
        """
        Given a list of received message parts, checks the first message part.
        Checks performed:
            - Magic byte (expects 0x31)
            - Version byte (expects 0x01)
            - Response code (expects 0x00, else it assumes that the next frame contains the error description msg)
        This function throws an exception on check failure and exits normally on success
        """
        if len(msgParts) == 0:
            raise YakDBProtocolException("Received empty reply message")
        headerFrame = msgParts[0]
        if len(headerFrame) < 4:
            #Check if it looks like a header frame (magic byte + version byte)
            looksLikeAHeaderFrame = (len(headerFrame) >= 1)
            if ((len(headerFrame) >= 1 and headerFrame[0] != '\x31') or
                (len(headerFrame) >= 2 and headerFrame[1] != '\x01')):
                 looksLikeAHeaderFrame = False
            raise YakDBProtocolException("Reponse header frame has size of %d, but expected size-4 frame, %s"
                                         % (len(headerFrame),
                                           ("it doesn't even look like a header frame" if not looksLikeAHeaderFrame
                                               else "but it looks like some kind of header frame")))
        if headerFrame[2] == '\xFF':
            raise YakDBProtocolException("Server responded with protocol error")
        if expectedResponseType is not None and headerFrame[2] != expectedResponseType:
            raise YakDBProtocolException("Response code received from server is "
                        "%d instead of %d" % (ord(headerFrame[2]),  ord(expectedResponseType)))
        if headerFrame[3] != '\x00':
            errorMsg = msgParts[1] if len(msgParts) >= 2 else "<Unknown>"
            raise YakDBProtocolException(
                "Response status code is %d instead of 0x00 (ACK), error message: %s"
                % (ord(headerFrame[3]),  errorMsg))
        #Parse and return request ID, if any
        if len(headerFrame) > 4:
            return headerFrame[4:]
        return None #No request ID
    def _checkConnection(self):
        """Check if the current instance socket is connected, else raise an exception"""
        if self.socket is None:
            raise ConnectionStateException("Please connect to server before sending requests (use connect()!)")
        if self.numConnections <= 0:
            raise ConnectionStateException("Connection is setup, but not connected. Please connect before sending requests!")
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
        if self.mode not in [zmq.REQ, zmq.DEALER]:
            raise Exception("Only request/reply connections support this message type!")
    def useRequestReplyMode(self):
        """Sets the current YakDB connection into Request/reply mode (default)"""
        self.socket = self.context.socket(zmq.REQ)
        self.mode = zmq.REQ
    def usePushMode(self):
        """Sets the current YakDB connection into Push/pull mode (default)"""
        self.socket = self.context.socket(zmq.PUSH)
        self.mode = zmq.PUSH
    def usePubMode(self):
        """Sets the current YakDB connection into publish/subscribe mode"""
        self.socket = self.context.socket(zmq.PUB)
        self.mode = zmq.PUB
    def useDealerMode(self):
        """
        Sets the current YakDB connection into DEALER-based REQ/REP mode
        Sets a large random number as socket identity
        """
        self.socket = self.context.socket(zmq.DEALER)
        #self.socket.setsockopt(zmq.IDENTITY, str(random.randint(0, 1000000000)))
        self.mode = zmq.DEALER
    def connect(self, endpoints):
        """
        Connect to a YakDB server.
        @param endpoint The ZMQ endpoint to connect to, e.g. tcp://localhost:7100.
                An array is also allowed
        """
        if isinstance(endpoints, str): endpoints = [endpoints]
        #Use request/reply as default
        if self.socket == None:
            self.useRequestReplyMode()
        for endpoint in endpoints:
            self.__class__._checkParameterType(endpoint, str, "[one of the endpoints]")
            self.socket.connect(endpoint)
        self.numConnections += len(endpoints)
    def buildScanRequest(self, tableNo, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, invert=False):
        """
        Build a scan request message frame list
        See Connection.scan() docs for a detailed description.
        """
        #Check parameters and create binary-string only key list
        self._checkParameterType(tableNo, int, "tableNo")
        self._checkParameterType(limit, int, "limit",  allowNone=True)
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
        #Send header frame
        msgParts = ["\x31\x01\x13" + ("\x01" if invert else "\x00")]
        #Create the table number frame
        msgParts.append(struct.pack('<I', tableNo))
        #Send limit frame
        msgParts.append("" if limit is None else struct.pack('<q', limit))
        #Send range. "" --> empty frame --> start/end of table
        msgParts += YakDBConnectionBase._rangeToFrames(startKey, endKey)
        #Send key filter parameters
        msgParts.append("" if keyFilter is None else keyFilter)
        #Send value filter parameters
        msgParts.append("" if keyFilter is None else valueFilter) 
        return msgParts
    def buildReadRequest(self, tableNo, keys):
        """
        Build a list of read request message frames.
        See Connection.read() docs for a detailed description.
        """
        #Check if this connection instance is setup correctly
        self._checkSingleConnection()
        self._checkRequestReply()
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
        return msgParts
if __name__ == "__main__":
    import doctest
    doctest.testmod()
