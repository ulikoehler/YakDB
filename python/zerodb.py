import zmq
import struct

#TODO document these
class ParameterException(Exception):
    def __init__(self, message, Errors):
        Exception.__init__(self, message)

class ZeroDBProtocolException(Exception):
    def __init__(self, message, Errors):
        Exception.__init__(self, message)

class ZeroDBConnection:
    def __init__(self, context=None):
        if context is None:
            self.context = zmq.Context()
            self.cleanupContextOnDestruct = True
        else:
            self.context = context
            self.cleanupContextOnDestruct = False
        self.socket = None
        self.isConnected = False
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
        self.isConnected = True
    def _checkRequestReply(self):
        """Check if the current instance is correctly setup for req/rep, else raise an exception"""
        self._checkConnection()
        if self.mode is not zmq.REQ:
            raise Exception("Only request/reply connections support server info msgs")
    def _checkConnection(self):
        """Check if the current instance is correctly connected, else raise an exception"""
        if self.socket == None:
            raise Exception("Please connect to server before using serverInfo (use ZeroDBConnection.connect()!")
        if not self.isConnected:
            raise Exception("Current ZeroDBConnection is setup, but not connected. Please connect before usage!")
    def _sendBinary32(self, value):
        """
        Send a given int as 32-bit little-endian unsigned integer over self.socket, with SNDMORE
        
        This is e.g. used to send a table number frame.
        """
        if type(value) is not int:
            raise Exception("Can't format object of non-integer type as binary integer")
        self.socket.send(struct.pack('<I', value), zmq.SNDMORE)
    def _convertToBinary(self, value):
        """
        Given a string, float or int value, convert it to binary and return the converted value.
        Ints are converted to 32-bit little-endian signed integers (uint32_t).
        Floats are converted to 64-bit little-endian IEEE754 values (double).
        Numeric values are assumed to be signed.
        For other types, raise an exception
        """
        if type(value) is int:
            return struct.pack('<i', value)
        elif type(value) is float:
            return struct.pack('<d', value)
        elif type(value) is str:
            return value
        else:
            raise ParameterException("Value '%s' is neither int nor float nor str-type -- not mappable to binary. Please use a binary string for custom types." % value)
    def serverInfo(self):
        """
        Send a server info request to the client
        @return The server version string
        """
        self._checkRequestReply()
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
        if type(tableNo) is not int:
            raise ParameterException("Table number parameter is not an integer!")
        if type(valueDict) is not dict:
            raise ParameterException("valueDict parameter must be a dictionary")
        #Check if this connection instance is setup correctly
        self._checkConnection()
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
            value = _convertToBinary(valueDict[key])
            #Send the key and enqueue the value
            self.socket.send(key, zmq.SNDMORE)
            nextToSend = value
        #If nextToSend is None now, the dict didn't contain valid data
        #Send the last value without SNDMORE
        self.socket.send(nextToSend)
        #If this is a req/rep connection, receive a reply
        if self.mode is zmq.REQ:
            msgParts = self.socket.recv_multipart(copy=True)
            if len(msgParts) == 0:
                raise ZeroDBProtocolException("Received empty put reply message")
            if msgParts[0][2] != '\x20':
                raise ZeroDBProtocolException("Put response code was %d instead of 0x20" % msgParts[0][2])
            if msgParts[0][3] != '\x00':
                raise ZeroDBProtocolException("Put response status code was %d instead of 0x00 (ACK)" % msgParts[0][3])
        return True
    def read(self, tableNo, keys):
        """
        Read one or multiples values, identified by their keys, from a table.
        
        @param tableNo The table number to read from
        @param keys A list, tuple or single value.
                        Must only contain strings, ints or floats.
                        integral types are automatically mapped to signed 32-bit little-endian binary,
                        floating point types are mapped to signed little-endian 64-bit IEEE754 values.
                        If you'd like to use another binary representation, use a binary string instead.
        @return A list of values, correspondent to the key order
        """
        #Check parameters and create binary-string only key list
        if type(tableNo) is not int:
            raise ParameterException("Table number parameter is not an integer!")
        convertedKeys = []
        if type(keys) is list or type(keys) is tuple:
            for value in keys:
                if value is None:
                    raise ParameterException("Key list contains 'None' value, not mappable to binary")
                convertedKeys.append(_convertToBinary(value))
        elif (type(keys) is string) or (type(keys) is int) or (type(keys) is float):
            #We only have a single value
            convertedKeys.append(_convertToBinary(value))
        #Check if this connection instance is setup correctly
        self._checkRequestReply()
        #Send header frame
        flags = 0
        if partsync: flags |= 1
        if fullsync: flags |= 2
        headerStr = "\x31\x01\x10" + chr(flags)
        self.socket.send(headerStr, zmq.SNDMORE)
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
        if len(msgParts) == 0:
            raise ZeroDBProtocolException("Received empty read reply message")
        if msgParts[0][2] != '\x10':
            raise ZeroDBProtocolException("Read response code was %d instead of 0x20" % msgParts[0][2])
        if msgParts[0][3] != '\x00':
            raise ZeroDBProtocolException("Read response status code was %d instead of 0x00 (ACK)" % msgParts[0][3])
        #Return the data frames
        return msgParts[1:]
    def __del__(self):
        """Cleanup ZMQ resources. Auto-destroys context if none was given"""
        if self.socket is not None:
            self.socket.close()
        if self.cleanupContextOnDestruct:
            self.context.destroy()