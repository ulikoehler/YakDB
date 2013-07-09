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
    def writeDict(self, tableNo, valueDict, partsync=False, fullsync=False):
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
        #Check if this connection instance is setup correctly
        self._checkConnection()
        #Before sending any frames, check the value dictionary for validity
        #Else, the socket could be left in an inconsistent state
        if len(valueDict) is 0:
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
            if nextToSend is not None: self.socket.send(value, zmq.SNDMORE)
            #Map key to binary data if neccessary
            value = valueDict[key]
            if type(value) is int:
                value = struct.pack('<i', value)
            if type(value) is float:
                value = struct.pack('<d', value)
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
                raise ZeroDBProtocolException("Received empty reply message")
            if msgParts[0][2] != '\x20':
                raise ZeroDBProtocolException("Put response code was %d instead of 0x20" % msgParts[0][2])
            if msgParts[0][3] != '\x00':
                raise ZeroDBProtocolException("Put response status code was %d instead of 0x00 (ACK)" % msgParts[0][3])
        return True
    def __del__(self):
        """Cleanup ZMQ resources. Auto-destroys context if none was given"""
        if self.socket is not None:
            self.socket.close()
        if self.cleanupContextOnDestruct:
            self.context.destroy()