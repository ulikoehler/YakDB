#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
Inverted index utilities for YakDB.
"""
from YakDB.Utils import YakDBUtils
from YakDB.ConnectionBase import YakDBConnectionBase
from YakDB.TornadoConnection import TornadoConnection
from YakDB.Iterators import KeyValueIterator
import functools
import itertools
from collections import defaultdict

class InvertedIndex(object):
    """
    An inverted index wrapper for YakDB that stores
    the entity list in a single row.
    See the inverted index specification for details.
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
    def _processReadResult(scanResult):
        """
        Process the read result for a single token search
        Returns a dict (indexed by level) containing a set of hits

        >>> res = [('L1\\x1Ef','x\\x00y'),('X\\x1Eg','a\\x00y'),('L1\\x1Efoo','z\\x00y')]
        >>> sorted(InvertedIndex._processReadResult(res))
        {'L1': {'x', 'y', 'z'}, 'X':{'a', 'y'}}
        >>> res = [('L2\\x1Ef','x\\x00y'),('L3\\x1Eg','a\\x00y'),('L4\\x1Efoo','z\\x00y')]
        >>> sorted(InvertedIndex._processReadResult(res))
        {'2': {'x', 'y'}, 'X': {'a', 'y'}}
        """
        return {InvertedIndex.extractLevel(key): InvertedIndex.splitValues(value)
                for key, value in scanResult}
    @staticmethod
    def _processScanResult(scanResult):
        """
        Process the scan result for a single token prefix search and a single level
        Returns a set containing all hits

        >>> res = [('L1\\x1Ef','x\\x00y'),('X\\x1Eg','a\\x00y'),('L1\\x1Efoo','z\\x00y')]
        >>> sorted(InvertedIndex._processReadResult(res))
        {'L1': {'x', 'y', 'z'}, 'X':{'a', 'y'}}
        >>> res = [('L2\\x1Ef','x\\x00y'),('L3\\x1Eg','a\\x00y'),('L4\\x1Efoo','z\\x00y')]
        >>> sorted(InvertedIndex._processReadResult(res))
        {'2': {'x', 'y'}, 'X': {'a', 'y'}}
        """
        return set(InvertedIndex.splitValues(value) for _, value in scanResult)
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
        for key, value in scanResult.items():
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
    def writeIndex(self, token, entityList, levels=""):
        """
        Write a list of entities that relate to (token, level) to the index.
        The previous entity result for that (token, level) is replaced.

        Precondition (not checked): Either it is acceptable that the previous
        index entry is replaced (e.g. when assembling the index in memory)
        or the table has been opened with merge operator = NULAPPEND.
        """
        kv = {InvertedIndex.getKey(token, level): "\x00".join(entityList)}
        if not kv: return
        #One-shot write for all entities
        self.conn.put(self.tableNo, kv)
    def indexTokens(self, tokens, entity, level=""):
        """
        Like writeIndex, but does not add a single token for a list of documents
        but a list of tokens for a single document.

        Precondition (not checked): Either it is acceptable that the previous
        index entry is replaced (e.g. when assembling the index in memory)
        or the table has been opened with merge operator = NULAPPEND.
        """
        kv = {InvertedIndex.getKey(token, level): entity
              for token in tokens}
        if not kv: return
        self.conn.put(self.tableNo, kv)
    def searchSingleTokenExact(self, token, level=[""], limit=10):
        """Search a single token by exact match in the inverted index"""
        assert not self.connectionIsAsync
        readKeys = [InvertedIndex.getKey(token, level) for level in levels]
        readResult = self.conn.read(self.tableNo, readKeys)
        return self._processReadResult(zip(readKeys, readResult))
    def searchSingleTokenPrefix(self, token, levels=[""], limit=25):
        """Search a single token with prefix matching in the inverted index"""
        assert not self.connectionIsAsync
        res = {}
        #For prefix = scan operations we need to perform one operation per level
        for level in levels:
            startKey = InvertedIndex.getKey(token, level)
            endKey = YakDBUtils.incrementKey(startKey)
            scanResult = self.conn.scan(self.tableNo, startKey=startKey, endKey=endKey, limit=limit)
            res[level] = self._processScanResult(scanResult)
        return res
    def searchMultiTokenExact(self, tokens, levels=[""], strict=False):
        """
        Search multiple tokens in the inverted index (by exact match)

        Keyword arguments:
            strict: If this is set to False, token/level combinations
                without results will be ignored. Else, they yield an empty result set
                for that level.
        """
        assert not self.connectionIsAsync
        #Strategy Perform a large one-shot read to avoid any overhead
        levelsTokens = list(itertools.product(levels, tokens))
        #Perform read, with results being returned in known order
        readKeys = [InvertedIndex.getKey(token, level) for (level, token) in levelsTokens]
        readResult = self.conn.read(self.tableNo, readKeys)
        #Iterate over results
        res = {}
        for (value, (level, token)) in zip(readResult, levelsTokens):
            hits = InvertedIndex.splitValues(value)
            #Skip empty hitsets except in strict mode
            if not (hits or strict): continue
            #Initialize set if required
            if level not in res:
                res[level] = set(hits)
            else: #Merge already existing hitset with current hitset
                res[level] = res[level] & set(hits)
        return res
    def searchMultiTokenPrefix(self, tokens, levels=[""], limit=25, strict=False):
        """
        Search multiple tokens in the inverted index

        Keyword arguments:
            strict: If this is set to False, token/level combinations
                without results will be ignored. Else, they yield an empty result set
                for that level.
            limit: How many prefix entries will be scanned (in lexicographical order)
        """
        assert not self.connectionIsAsync
        ret = {}
        for token in tokens:
            result = self.searchSingleTokenPrefix(token, levels, limit=limit)
            #Skip empty hitsets except in strict mode
            if not (result[level] or strict): continue
            #Merge with result set
            for level in levels:
                if level not in ret:
                    ret[level] = result[level]
                else: #Merge already existing hitset with current hitset
                    ret[level] = ret[level] & result[level]
        return ret

        #Basically we execute multiple single token searches and emulate a multi-token
        # result to pass it into the merging algorithm
        singleTokenResults = [ for token in tokens]
        return InvertedIndex._processMultiTokenPrefixResult(singleTokenResults)
    def iterateIndex(self, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, skip=0, invert=False, chunkSize=1000):
        "Wrapper to initialize a IndexIterator iterating over self"
        return IndexIterator(self, startKey, endKey, limit, keyFilter,
            valueFilter, skip, invert, chunkSize)


class IndexIterator(KeyValueIterator):
    """
    Lazy iterator wrapper that directly iterates over an index table,
    i.e. splits the entity list and the level/token pair

    Iterates over tuples (level, token, [(entity, part)])

    Entity parts are returned as empty string if not present.
    """
    def __init__(self, idx, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, skip=0, invert=False, chunkSize=1000):
        KeyValueIterator.__init__(self, idx.conn, idx.tableNo,
            startKey, endKey, limit, keyFilter,
            valueFilter, skip, invert, chunkSize)
    def __next__(self):
        k, v = KeyValueIterator.__next__(self)
        level, _, token = k.partition(b"\x1E")
        entities = (IndexIterator._splitEntityIdPart(d) for d in v.split(b"\x00"))
        return (level, token, entities)
    @staticmethod
    def _splitEntityIdPart(entity):
        """
        Utility to split \x1E-separated entities into a 2-tuple

        >>> IndexIterator._splitEntityIdPart(b"foo:bar")
        (b'foo:bar', b'')
        >>> IndexIterator._splitEntityIdPart(b"foo\x1Ebar")
        (b'foo', b'bar')
        >>> IndexIterator._splitEntityIdPart(b"")
        (b'', b'')
        """
        (a, _, b) = entity.partition(b"\x1E")
        return (a, b)


class AsynchronousInvertedIndex(InvertedIndex):
    """
    Inverted index subclass that also provides asychronous methods
    TODO Implementation of most of these methods is unfinished
    """
    def searchMultiTokenPrefixAsync(self, tokens, callback, levels=[""], limit=25):
        """Search multiple tokens in the inverted index, for exact matches"""
        assert self.connectionIsAsync
        readKeys = [InvertedIndex.getKey(token, levels) for token in tokens]
        #In the results array, we set each index to the corresponding result, if any yet
        results = [None] * len(tokens)
        for i, token in enumerate(tokens):
            internalCallback = functools.partial(InvertedIndex.__searchMultiTokenPrefixAsyncRecvCallback,
                                                 callback, levels, results, i)
            #Here, we need access to both keys and values --> map the data into a dict
            self.searchSingleTokenPrefixAsync(token=token, levels=levels, limit=limit, callback=internalCallback)
    @staticmethod
    def __searchMultiTokenPrefixAsyncRecvCallback(origCallback, level, results, i, response):
        """This is called when the response for a multi token search has been received"""
        #Process & Call the callback if ALL requests have been finished
        results[i] = response
        if all([obj is not None for obj in results]):
            #The multi-token search algorithm uses only the level from the key.
            #Therefore, for read() emulation, we don't need to know the token
            result = InvertedIndex._processMultiTokenPrefixResult(results)
            origCallback(result)
    def searchMultiTokenExactAsync(self, tokens, callback, level=""):
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
    def searchSingleTokenPrefixAsync(self, token, callback, levels=[""], limit=25):
        """
        Search a single token in the inverted index using async connection
        """
        assert self.connectionIsAsync
        startKey = InvertedIndex.getKey(token, level)
        endKey = YakDBUtils.incrementKey(startKey)
        internalCallback = functools.partial(InvertedIndex.__searchSingleTokenPrefixAsyncRecvCallback, callback)
        self.conn.scan(self.tableNo, callback=internalCallback, startKey=startKey, endKey=endKey, limit=limit)
    @staticmethod
    def __searchSingleTokenPrefixAsyncRecvCallback(origCallback, response):
        "This gets called once an asynchronous request to "
        origCallback(InvertedIndex._processReadResult(response))
    def searchSingleTokenExactAsync(self, token, callback, levels=[""], limit=25):
        """
        Search a single token in the inverted index using async connection
        """
        assert self.connectionIsAsync
        readKeys = [InvertedIndex.getKey(token, level) for level in levels]
        internalCallback = functools.partial(
            InvertedIndex.__searchSingleTokenExactAsyncRecvCallback, callback, readKeys)
        self.conn.read(self.tableNo, readKeys, callback=internalCallback)
    @staticmethod
    def __searchSingleTokenExactAsyncRecvCallback(origCallback, readKeys, readResult):
        origCallback(self._processReadResult(zip(readKeys, readResult)))

if __name__ == "__main__":
    import doctest
    doctest.testmod()
