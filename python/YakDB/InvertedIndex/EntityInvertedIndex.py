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
from YakDB.InvertedIndex import InvertedIndex
import functools
import cPickle as pickle

#Only needed for the default key extractor, but it's a thin wrapper
import hashlib

def hashEntity(entity):
    """
    Default entity key extractor: Hex-SHA1 of pickled entity
    """
    return hashlib.sha1(pickle.dumps(entity)).hexdigest()

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
    def __init__(self, connection, entityTableNo, indexTableNo, keyExtractor=hashEntity, minEntities=50, maxEntities=250):
        """
        Keyword arguments:
            connection The YakDB connection
            tableNo: The table no to store the inverted index in.
        """
        assert isinstance(connection, YakDBConnectionBase)
        self.minEntities = minEntities
        self.maxEntities = maxEntities
        self.extractKey = keyExtractor
        self.conn = connection
        self.entityTableNo = entityTableNo
        self.index = InvertedIndex(connection, indexTableNo)
        self.connectionIsAsync = isinstance(connection, TornadoConnection)
    def packValue(self, entity):
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
        self.conn.put(self.entityTableNo, {key: value})
    def writeEntities(self, entities):
        """Write a list of entities at onces"""
        writeDict = {self.extractKey(e): self.packValue(e) for e in entities}
        self.conn.put(self.entityTableNo, writeDict)
    def writeList(self, token, entityList, level=""):
        """
        Write a list of entities that relate to (token, level) to the index.
        The previous entity result for that (token, level) is replaced.
        """
        self.index.writeList(token, entityList, level)
    def getEntities(self, keyList):
        """Read a list of entities and unpack them. Return the list of objects"""
        assert not self.connectionIsAsync
        readResult = self.conn.read(keyList)
        return [self.unpackValue(val) for val in readResult]
    def getEntitiesAsync(self, keyList, callback):
        """Read entities asynchronously"""
        assert self.connectionIsAsync
        internalCallback = functools.partial(self.__getEntitiesAsyncCallback, callback)
        self.conn.read(self.entityTableNo, keyList, callback=internalCallback)
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
            #Append level results until minimal results are reacheds
            for result in states:
                allResults += result
                if len(allResults) >= self.minEntities: break
            #Remove duplicate results
            allResults = makeUnique(allResults)
            #Clamp to the maximum number of entities
            allResults = allResults[:self.maxEntities]
            #Read the entity objects
            self.getEntitiesAsync(allResults, callback=callback)
    def searchSingleTokenPrefix(self, token, levels=[""], limit=10):
        """
        Search a single token (or token prefix) in one or multiple levels
        """
        assert not self.connectionIsAsync
        assert isinstance(levels, collections.Iterable)
        assert type(levels) != str and type(levels) != unicode
        return self.__execSyncSearch(self.index.searchSingleTokenPrefix, token, levels, scanLimit)
    def searchSingleTokenPrefixAsync(self, token, callback, levels=[""], limit=25):
        """
        Search a single token in the inverted index using async connection
        """
        assert self.connectionIsAsync
        return self.__execAsyncSearch(self.index.searchSingleTokenPrefixAsync, callback, token, levels, scanLimit)
    def searchMultiTokenExact(self, tokens, level=""):
        """Search multiple tokens in the inverted i     ndex (by exact match)"""
        assert not self.connectionIsAsync
        return self.__execSyncSearch(self.index.searchMultiTokenExact, tokens, levels, scanLimit)
    def searchMultiTokenExactAsync(self, tokens, callback, level="", ):
        """Search multiple tokens in the inverted index, for exact matches"""
        assert not self.connectionIsAsync
        return self.__execAsyncSearch(self.index.searchMultiTokenExactAsync, callback, tokens, levels, scanLimit)
    def searchMultiTokenPrefix(self, tokens, level="", limit=25):
        """Search multiple tokens in the inverted index"""
        assert not self.connectionIsAsync
        return self.__execSyncSearch(self.index.searchMultiTokenPrefix, tokens, levels, scanLimit)
    def searchMultiTokenPrefixAsync(self, tokens, callback, level="", limit=25):
        """Search multiple tokens in the inverted index, for exact matches"""
        assert self.connectionIsAsync
        return self.__execAsyncSearch(self.index.searchMultiTokenPrefixAsync, callback, token, levels, scanLimit)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
