#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Inverted index utilities for YakDB.
"""
from YakDB.Utils import YakDBUtils
from YakDB.Connection import Connection
from YakDB.ConnectionBase import YakDBConnectionBase
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
        assert isinstance(connection, YakDBConnectionBase)
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
        #Empty input --> empty output
        if not dbValue: return set()
        return set(dbValue.split('\x00'))
    @staticmethod
    def _processSingleTokenResult(scanResult, level):
        """
        Process the scan result for a single token search
        Returns the set of entities relating to the token.
        
        >>> res = [('L1\\x1Ef','x\\x00y'),('X\\x1Eg','a\\x00y'),('L1\\x1Efoo','z\\x00y')]
        >>> sorted(InvertedIndex._processSingleTokenResult(res, 'L1'))
        ['x', 'y', 'z']
        >>> res = [('L2\\x1Ef','x\\x00y'),('L3\\x1Eg','a\\x00y'),('L4\\x1Efoo','z\\x00y')]
        >>> sorted(InvertedIndex._processSingleTokenResult(res, 'L1'))
        []
        """
        result = set()
        for key, value in scanResult:
            if InvertedIndex.extractLevel(key) == level:
                result.update(InvertedIndex.splitValues(value))
        return result
    @staticmethod
    def _processMultiTokenResult(scanResult, level):
        """
        Process the scan result for a multi-token exact search
        Returns the set of entities relating to the token.
        
        >>> res = {'L1\\x1Ef':'x\\x00y','X\\x1Eg':'a\\x00y','L1\\x1Efoo':'z\\x00y'}
        >>> sorted(InvertedIndex._processMultiTokenResult(res, 'L1'))
        ['y']
        >>> res = {'L4\\x1Ef':'x\\x00y','L3\\x1Eg':'a\\x00y','L5\\x1Efoo':'z\\x00y'}
        >>> sorted(InvertedIndex._processMultiTokenResult(res, 'L1'))
        []
        """
        #NOTE: If one of the tokens has no result at all, it is currently ignored
        initialized = False
        result = None
        for key, value in scanResult.iteritems():
            if InvertedIndex.extractLevel(key) == level:
                valueList = InvertedIndex.splitValues(value)
                if initialized:
                    result.intersection_update(valueList)
                else: #First level-matching result
                    result = set(valueList)
                    initialized = True
        return (set() if result is None else result)
    @staticmethod
    def _processMultiTokenPrefixResult(resultList):
        """
        Process the scan result for a multi-token prefix search.
        The result list argument is the list of result sets from
        the single token searches
        
        >>> res = [set(["a","b","x"]),set(["x","y","a"])]
        >>> sorted(InvertedIndex._processMultiTokenPrefixResult(res))
        ['a', 'x']
        >>> res = [set(["a","b","x"]),set(["d","e","f"]),set(["x","y","a"])]
        >>> sorted(InvertedIndex._processMultiTokenPrefixResult(res))
        []
        """
        #NOTE: If one of the tokens has no result at all, it is currently ignored
        initialized = False
        result = None
        for resultSet in resultList:
            if initialized:
                result.intersection_update(resultSet)
            else: #First result
                result = resultSet
                initialized = True
        return (set() if result is None else result)
    def writeList(self, token, entityList, level=""):
        """
        Write a list of entities that relate to (token, level) to the index.
        The previous entity result for that (token, level) is replaced.
        """
        kv = {InvertedIndex.getKey(token, level): "\x00".join(entityList)};
        self.conn.put(self.tableNo, kv)
    def searchSingleTokenExact(self, token, level="", limit=10):
        """Search a single token by exact match in the inverted index"""
        assert not self.connectionIsAsync
        readResult = self.conn.read(self.tableNo, InvertedIndex.getKey(token, level))
        return InvertedIndex.splitValues(readResult)
    def searchSingleTokenExactAsync(self, token, callback, level="", limit=25):
        """
        Search a single token in the inverted index using async connection
        """
        assert self.connectionIsAsync
        startKey = InvertedIndex.getKey(token, level)
        internalCallback = functools.partial(InvertedIndex.__searchSingleTokenExactAsyncRecvCallback, callback, level)
        self.conn.read(self.tableNo, InvertedIndex.getKey(token, level), callback=internalCallback)
    @staticmethod
    def __searchSingleTokenExactAsyncRecvCallback(origCallback, level, response):
        result = InvertedIndex.splitValues(response)
        origCallback(result)
    def searchSingleTokenPrefix(self, token, level="", limit=10):
        """Search a single token (or token prefix) in the inverted index"""
        assert not self.connectionIsAsync
        startKey = InvertedIndex.getKey(token, level)
        endKey = YakDBUtils.incrementKey(startKey)
        scanResult = self.conn.scan(self.tableNo, startKey=startKey, endKey=endKey, limit=limit)
        return InvertedIndex._processSingleTokenResult(scanResult, level=level)
    def searchSingleTokenPrefixAsync(self, token, callback, level="", limit=25):
        """
        Search a single token in the inverted index using async connection
        """
        assert self.connectionIsAsync
        startKey = InvertedIndex.getKey(token, level)
        endKey = YakDBUtils.incrementKey(startKey)
        internalCallback = functools.partial(InvertedIndex.__searchSingleTokenPrefixAsyncRecvCallback, callback, level)
        self.conn.scan(self.tableNo, callback=internalCallback, startKey=startKey, endKey=endKey, limit=limit)
    @staticmethod
    def __searchSingleTokenPrefixAsyncRecvCallback(origCallback, level, response):
        result = InvertedIndex._processSingleTokenResult(response, level)
        origCallback(result)
    def searchMultiTokenExact(self, tokens, level=""):
        """Search multiple tokens in the inverted index (by exact match)"""
        assert not self.connectionIsAsync
        readKeys = [InvertedIndex.getKey(token, level) for token in tokens]
        readResult = self.conn.read(self.tableNo, readKeys)
        return InvertedIndex._processMultiTokenResult(readResult, level)
    def searchMultiTokenExactAsync(self, tokens, callback, level="", ):
        """Search multiple tokens in the inverted index, for exact matches"""
        assert self.connectionIsAsync
        readKeys = [InvertedIndex.getKey(token, level) for token in tokens]
        internalCallback = functools.partial(InvertedIndex.__searchMultiTokenExactAsyncRecvCallback, callback, level)
        #Here, we need access to both keys and values --> map the data into a dict
        self.conn.read(self.tableNo, readKeys, callback=internalCallback, mapKeys=True)
    @staticmethod
    def __searchMultiTokenExactAsyncRecvCallback(origCallback, level, response):
        """This is called when the response for a multi token search has been received"""
        result = InvertedIndex._processMultiTokenResult(response, level)
        origCallback(result)
    def searchMultiTokenPrefix(self, tokens, level="", limit=25):
        """Search multiple tokens in the inverted index"""
        assert not self.connectionIsAsync
        #Basically we execute multiple single token searches and emulate a multi-token
        # result to pass it into the merging algorithm
        singleTokenResults = [self.searchSingleTokenPrefix(token, level, limit =limit) for token in tokens]
        return InvertedIndex._processMultiTokenPrefixResult(singleTokenResults)
    def searchMultiTokenPrefixAsync(self, tokens, callback, level="", limit=25):
        """Search multiple tokens in the inverted index, for exact matches"""
        assert self.connectionIsAsync
        readKeys = [InvertedIndex.getKey(token, level) for token in tokens]
        #In the results array, we set each index to the corresponding result, if any yet
        results = [None] * len(tokens)
        for i, token in enumerate(tokens):
            internalCallback = functools.partial(InvertedIndex.__searchMultiTokenPrefixAsyncRecvCallback,
                                                 callback, level, results, i)
            #Here, we need access to both keys and values --> map the data into a dict
            self.searchSingleTokenPrefixAsync(token=token, level=level, limit=limit, callback=internalCallback)
    @staticmethod
    def __searchMultiTokenPrefixAsyncRecvCallback(origCallback, level, results, i, response):
        """This is called when the response for a multi token search has been received"""
        #Process & Call the callback if ALL requests have been finished
        results[i] = response
        if all([obj != None for obj in results]):
            #The multi-token search algorithm uses only the level from the key.
            #Therefore, for read() emulation, we don't need to know the token
            result = InvertedIndex._processMultiTokenPrefixResult(results)
            origCallback(result)
        
if __name__ == "__main__":
    import doctest
    doctest.testmod()
