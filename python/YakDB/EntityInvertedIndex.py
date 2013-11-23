#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Inverted index utilities for YakDB.
"""
from YakDB.Utils import YakDBUtils
from YakDB.Connection import Connection
from YakDB.ConnectionBase import YakDBConnectionBase
from YakDB.TornadoConnection import TornadoConnection
from YakDB.Utils import makeUnique
import functools
import cPickle as pickle


class EntityInvertedIndex(object):
    """
    An inverted index wrapper that handles saving and reading of entities.
    
    Mimics the behaviour of an InvertedIndex (and uses it internally),
    but allows multi-level searches and automatically fetches entities:
    Each consecutive level is searched if the previous level did not return
    at least a minimal number of results. The overall number of results

    The default implementation uses pickle as value packer and SHA1 hashing of
    full entities as key generator.
    """
    def __init__(self, connection, indexTableNo, entityTableNo, keyFunc, minEntities=25, maxEntities=250):
        """
        Keyword arguments:
            connection The YakDB connection
            tableNo: The table no to store the inverted index in.
        """
        assert isinstance(connection, YakDBConnectionBase)
        self.minEntities = minEntities
        self.maxEntities = maxEntities
        self.extractKey = keyFunc
        self.conn = connection
        self.entityTableNo = entityTableNo
        self.index = InvertedIndex(connection, indexTableNo)
        self.connectionIsAsync = isinstance(connection, TornadoConnection)
    def packValue(self, entitity):
        """Pack = Serialize an entity"""
        return pickle.dumps(entity)
    def unpackValue(self, packedEntity):
        """Unpack = deserialize an entity from the database."""
        return pickle.loads(packedEntity)
    def writeEntity(self, entity):
        """Write an entity to the database"""
        assert not self.connectionIsAsync
        key = self.extractKey(entity)
        value = self.packValue(entity)
        self.conn.put(self, self.entityTableNo, {key: value})
    def getEntities(self, keyList):
        """Read a list of entities and unpack them. Return the list of objects"""
        assert not self.connectionIsAsync
        readResult = self.conn.read(self, keyList)
        return [self.unpackValue(val) for val in readResult]
    def getEntitiesAsync(self, keyList, callback):
        """Read entities asynchronously"""
        assert self.connectionIsAsync
        internalCallback = functools.partial(self.__getEntitiesAsyncCallback, callback)
        self.conn.read(self.entityTableNo, keyList callback=internalCallback)
    @staticmethod
    def __getEntitiesAsyncCallback(self, callback, values):
        entities = [self.unpackValue(val) for val in values]
        callback(entities)
    def __execSyncSearch(self, searchFunc, tokenObj, levels, scanLimit):
        """
        Internal search runner for synchronous multi-level search
        """
        endKey = YakDBUtils.incrementKey(startKey)
        allResults = []
        #Scan all levels
        for level in levels:
            allResults = searchFunc(tokenObj, level, scanLimit)
            if len(allResults) >= self.minEntities: break
        #Remove duplicate results
        allResults = makeUnique(allResults)
        #Clamp to the maximum number of entities
        allResults = allResults[:self.maxEntities]
        #Read the entity objects
        return self.getEntities(allResults)
    def __execAsyncSearch(self, searchFunc, callback, tokenObj, levels, scanLimit):
        """
        Internal search runner for async multi-level search
        """
        endKey = YakDBUtils.incrementKey(startKey)
        allResults = []
        #Scan all levels
        states = [None] * len(levels) #Saves the result data for each level
        internalCallbackTpl = functools.partial(self.__execAsyncSearchScanCB, states, callback)
        #Start parallel jobs for each level
        for i, level in enumerate(levels):
            internalCallback = functools.partial(internalCallbackTpl, i)
            allResults = searchFunc(tokenObj, level, scanLimit, callback=internalCallback)
            if len(allResults) >= self.minEntities: break
    def __execAsyncSearchScanCB(self, states, callback, i, resultList):
        """Called from the async search runner once a scan result comes in"""
        states[i] = resultList
        if not any([state is None for state in states]): #All requests finished
            allResults = []
            [allResults.append(res) for res in state]
            #Remove duplicate results
            allResults = makeUnique(allResults)
            #Clamp to the maximum number of entities
            allResults = allResults[:self.maxEntities]
            #Read the entity objects
            return self.getEntities(allResults)
    def searchSingleTokenPrefix(self, token, levels=[""], limit=10):
        """
        Search a single token (or token prefix) in one or multiple levels
        """
        assert not self.connectionIsAsync
        assert isinstance(levels, collections.Iterable)
        assert type(levels) != str and type(levels) != unicode
        return self.__execSyncSearch(self.index.searchSingleTokenPrefix, token, levels, scanLimit)
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
