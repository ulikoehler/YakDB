#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Inverted index utilities for YakDB.
"""
from YakDB.Utils import YakDBUtils
from YakDB.Connection import Connection
from YakDB.TornadoConnection import TornadoConnection
from zmq.eventloop.zmqstream import ZMQStream
import functools


class InvertedIndex:
    """
    An inverted index wrapper for YakDB that stores
    the entity list in a single row.
    
    See the inverted index documentation for details.
    
    Single tokens are searched as exact match and prefix match.
    The match sets are joined together.
    
    Multiple tokens are searched as exact matches.
    The matches of the single tokens are intersected.
    
    In any case, for a single search, only a single request
    is issued to the database.
    
    This class does not manage the entities referred to
    by the inverted index but only the index itself.
    """
    def __init__(self, connection, tableNo):
        """
        Keyword arguments:
            connection The YakDB connection
            tableNo: The table no to store the inverted index in.
        """
        assert isinstance(connection, Connection)
        self.conn = connection
        self.tableNo = tableNo
        self.connectionIsAsync = isinstance(connection, TornadoConnection)
    @staticmethod
    def getKey(token, level=""):
        """Get the inverted index database key for a given token and level"""
        return "%s\x1E%s" % (level, token)
    @staticmethod
    def extractLevel(dbKey):
        """
        Given a DB key, extracts the level
        
        >>> InvertedIndex.extractLevel("thelevel\\x1Ethetoken")
        'thelevel'
        >>> InvertedIndex.extractLevel("\x1Ethetoken")
        ''
        """
        return dbKey.rpartition("\x1E")[0]
    @staticmethod
    def splitValues(dbValue):
        """
        Given a DB values, extracts the list of related entities
        
        >>> sorted(InvertedIndex.splitValues("a\\x00b\\x00cd\\x00ef"))
        ['a', 'b', 'cd', 'ef']
        """
        return set(dbValue.split('\x00'))
    @staticmethod
    def _processSingleTokenResult(scanResult, level):
        """
        Process the scan result for a single token search
        Returns the set of entities relating to the token.
        
        >>> res = [('L1\\x1Ef','x\\x00y'),('X\\x1Eg','a\\x00y'),('L1\\x1Efoo','z\\x00y')]
        >>> sorted(InvertedIndex._processSingleTokenResult(res, 'L1'))
        ['x', 'y', 'z']
        """
        result = set()
        for key, value in scanResult:
            if InvertedIndex.extractLevel(key) == level:
                result.update(InvertedIndex.splitValues(value))
        return result
    @staticmethod
    def _processMultiTokenResult(scanResult, level):
        """
        Process the scan result for a single token search
        Returns the set of entities relating to the token.
        
        >>> res = [('L1\\x1Ef','x\\x00y'),('X\\x1Eg','a\\x00y'),('L1\\x1Efoo','z\\x00y')]
        >>> sorted(InvertedIndex._processMultiTokenResult(res, 'L1'))
        ['y']
        """
        #NOTE: If one of the tokens has no result at all, it is currently ignored
        initialized = False
        result = None
        print scanResult
        for key, value in scanResult:
            if InvertedIndex.extractLevel(key) == level:
                valueList = InvertedIndex.splitValues(value)
                if initialized:
                    result.intersection_update(valueList)
                else: #First level-matching result
                    result = set(valueList)
                    initialized = True
        return (set() if result is None else result)
    def searchSingleToken(self, token, level="", limit=10):
        """Search a single token (or token prefix) in the inverted index"""
        assert not self.connectionIsAsync
        startKey = InvertedIndex.getKey(token, level)
        endKey = YakDBUtils.incrementKey(startKey)
        scanResult = self.conn.scan(self.tableNo, startKey=startKey, endKey=endKey, limit=limit)
        return InvertedIndex._processSingleTokenResult(scanResult, level=level)
    def writeList(self, token, entityList, level=""):
        """
        Write a list of entities that relate to (token, level) to the index.
        The previous entity result for that (token, level) is replaced.
        """
        kv = {InvertedIndex.getKey(token, level): "\x00".join(entityList)};
        self.conn.put(self.tableNo, kv)
    def searchSingleTokenAsync(self, token, callback, level="", limit=25):
        """
        Search a single token in the inverted index using async connection
        """
        assert self.connectionIsAsync
        startKey = InvertedIndex.getKey(token, level)
        endKey = YakDBUtils.incrementKey(startKey)
        internalCallback = functools.partial(InvertedIndex.__searchSingleTokenAsyncRecvCallback, callback, level)
        self.conn.scan(self.tableNo, callback=internalCallback, startKey=startKey, endKey=endKey, limit=limit)
    @staticmethod
    def __searchSingleTokenAsyncRecvCallback(origCallback, level, response):
        result = InvertedIndex._processSingleTokenResult(response, level)
        origCallback(result)
    def searchMultiTokenExact(self, tokens, level=""):
        """Search multiple tokens in the inverted index"""
        assert not self.connectionIsAsync
        readKeys = [InvertedIndex.getKey(token, level) for token in tokens]
        readResult = self.conn.read(self.tableNo, readKeys)
        return InvertedIndex._processMultiTokenResult(readResult, level)
    def searchMultiTokenExactAsync(self, tokens, callback, level="", ):
        """Search multiple tokens in the inverted index"""
        assert not self.connectionIsAsync
        readKeys = [InvertedIndex.getKey(token, level) for token in tokens]
        internalCallback = functools.partial(InvertedIndex.__searchMultiTokenExactAsyncRecvCallback, callback, level)
        self.conn.read(self.tableNo, readKeys, callback=internalCallback, callbackParam=callback)
    @staticmethod
    def __searchMultiTokenExactAsyncRecvCallback(origCallback, level, response):
        """This is called when the response for a multi token search has been received"""
        result = InvertedIndex._processMultiTokenResult(response, level)
        origCallback(result)
        
if __name__ == "__main__":
    import doctest
    doctest.testmod()
