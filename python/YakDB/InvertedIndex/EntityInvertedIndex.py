#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
Inverted index plus entity management utilities for YakDB.
"""
from YakDB.ConnectionBase import YakDBConnectionBase
from YakDB.Utils import makeUnique
from YakDB.InvertedIndex import InvertedIndex
from YakDB.Iterators import KeyValueIterator
import collections
#Python3 has no separate cPickle module
try:
    import cPickle as pickle
except ImportError:
    import pickle

#Only needed for the default key extractor, but it's a thin wrapper
import hashlib
import base64


def hashEntity(entity):
    """
    Default entity key extractor: Hex-SHA1 of pickled entity
    """
    return base64.b64encode(hashlib.sha1(pickle.dumps(entity)).digest())[:16]


class EntityInvertedIndex(object):
    """
    An inverted index decorator that handles saving and reading of entities.

    Mimics the behaviour of an InvertedIndex (and uses it internally),
    but allows multi-level searches and automatically fetches entities:
    Each consecutive level is searched if the previous level did not return
    at least a minimal number of results. The overall number of results can be set by limit.

    The default implementation uses pickle as value packer and SHA1 hashing of the
    entity (the first 16 chars of the b64 representation are used) as key generator.
    """
    def __init__(self, connection, entityTableNo, indexTableNo, keyExtractor=hashEntity, minEntities=50, maxEntities=250):
        """
        Keyword arguments:
            connection The YakDB connection
            tableNo: The table no to store the inverted index in.
        """
        assert isinstance(connection, YakDBConnectionBase)
        self.index = InvertedIndex(connection, indexTableNo)
        self.minEntities = minEntities
        self.maxEntities = maxEntities
        self.extractKey = keyExtractor
        self.conn = connection
        self.entityTableNo = entityTableNo
    def packValue(self, entity):
        """Pack = Serialize an entity"""
        return pickle.dumps(entity)
    def unpackValue(self, packedEntity):
        """Unpack = deserialize an entity from the database."""
        return pickle.loads(packedEntity)
    @staticmethod
    def _entityIdsToKey(entityId):
        """
        Convert an entity ID to a key, i.e. remove the part identifier and the separator

        >>> EntityInvertedIndex._entityIdsToKey(b"foo:bar")
        b'foo:bar'
        >>> EntityInvertedIndex._entityIdsToKey(b"foo\x1Ebar")
        b'foo'
        >>> EntityInvertedIndex._entityIdsToKey(b"")
        b''
        """
        return entityId.partition(b"\x1E")[0]
    def writeEntity(self, entity):
        """Write an entity to the database"""
        key = self.extractKey(entity)
        value = self.packValue(entity)
        self.conn.put(self.entityTableNo, {key: value})
    def writeEntities(self, entities):
        """Write a list of entities at onces"""
        writeDict = {self.extractKey(e): self.packValue(e) for e in entities}
        self.conn.put(self.entityTableNo, writeDict)
    def getEntities(self, entityIds):
        """Read a list of entities and unpack them. Return the list of objects"""
        #Shortcut for empty resultset
        if not entityIds: return {}
        #One-shot read all selected entities at once
        readResults = self.conn.read(self.entityTableNo,
            keys=(EntityInvertedIndex._entityIdsToKey(k) for k in entityIds)
        )
        return (self.unpackValue(val) for val in readResults)
    def __execSyncSearch(self, searchFunc, tokenObj, levels, limit):
        """
        Internal search runner for synchronous multi-level search
        """
        #Scan
        resultDict = searchFunc(tokenObj, levels, limit)
        #Reduce results based on level priority
        allResults = InvertedIndex.selectResults(resultDict, levels,
            minHits=self.minEntities, maxHits=self.maxEntities)
        #Remove duplicate results (using set would destroy the order)
        allResults = makeUnique(allResults)
        #FAILSAFE: Although we SHOULD have at most maxEntities results, we need to be sure
        allResults = allResults[:self.maxEntities]
        #Read the entity objects3
        return self.getEntities(allResults)
    def searchSingleTokenPrefix(self, token, levels=[""], limit=10):
        """
        Search a single token (or token prefix) in one or multiple levels
        """
        assert isinstance(levels, collections.Iterable)
        assert type(levels) != str and type(levels) != bytes
        return self.__execSyncSearch(
            self.index.searchSingleTokenPrefix, token, levels, limit)
    def searchMultiTokenExact(self, tokens, levels=[""]):
        """Search multiple tokens in the inverted index (by exact match)"""
        return self.__execSyncSearch(
            self.index.searchMultiTokenExact, tokens, levels)
    def searchMultiTokenPrefix(self, tokens, levels=[""], limit=25):
        """Search multiple tokens in the inverted index"""
        return self.__execSyncSearch(
            self.index.searchMultiTokenPrefix, tokens, levels, limit)
    def iterateEntities(self, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, skip=0, invert=False, chunkSize=1000):
        "Wrapper to initialize a EntityIterator iterating over self"
        return EntityIterator(self, startKey, endKey, limit, keyFilter,
            valueFilter, skip, invert, chunkSize)
    def iterateIndex(self, *args, **kwargs):
        "Decorator for InvertedIndex.iterateIndex"
        return self.index.iterateIndex(*args, **kwargs)

class EntityIterator(KeyValueIterator):
    """
    Lazy iterator wrapper that directly iterates over documents.
    """
    def __init__(self, idx, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, skip=0, invert=False, chunkSize=1000):
        KeyValueIterator.__init__(self, idx.conn, idx.entityTableNo,
            startKey, endKey, limit, keyFilter,
            valueFilter, skip, invert, chunkSize)
        self.idx = idx
    def __next__(self):
        k, v = KeyValueIterator.__next__(self)
        return (k, self.idx.unpackValue(v))



if __name__ == "__main__":
    import doctest
    doctest.testmod()
