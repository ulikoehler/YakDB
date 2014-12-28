#!/usr/bin/env python3
# -*- coding: utf8 -*-

from collections import deque

from YakDB.Utils import YakDBUtils

class KeyValueIterator(object):
    """
    An iterator that iterates over key-value pairs in a table.
    
    The iterator yields tuples (key, value).
    """
    def __init__(self, conn, tableNo=1, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, skip=0, invert=False, chunkSize=1000):
        """
        Initialize a new key-value iterator
        @param startKey the first node to scan
        @param limit The number of nodes to load at once
        """
        self.conn = conn
        self.tableNo = tableNo
        self.limit = limit
        self.nextStartKey = startKey
        self.endKey = endKey
        self.limit = limit
        self.keyFilter = keyFilter
        self.valueFilter = valueFilter
        self.chunkSize = chunkSize
        self.skip = skip
        self.invert = invert
        self.buf = deque()
    def __iter__(self):
        return self
    def __loadNextChunk(self):
        """
        Load the next chunk of key-value pairs into the buffer
        """
        scanRes = self.conn.scan(self.tableNo, startKey=self.nextStartKey, endKey=self.endKey, limit=self.chunkSize, keyFilter=self.keyFilter, valueFilter=self.valueFilter, skip=self.skip, invert=self.invert)
        #Stop if there's nothing left to scan
        if len(scanRes) is 0:
            raise StopIteration
        for key, value in scanRes:
            dataTuple = (key, value)
            self.buf.append(dataTuple)
        #Get the key to use as start key on chunk load
        lastIdentifier = (scanRes[-1][0])
        self.nextStartKey = YakDBUtils.incrementKey(lastIdentifier)
    def next(self): return self.__next__()
    def __next__(self):
        if len(self.buf) == 0:
            self.__loadNextChunk() #raises StopIteration if needed
        return self.buf.popleft()


class KeyIterator(object):
    """
    An iterator that uses a list request to iterate over keys
    
    The iterator yields keys only.
    """
    def __init__(self, conn, tableNo=1, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, skip=0, invert=False, chunkSize=1000):
        """
        Initialize a new key iterator.
        The parameters are equivalent to those of KeyValueIterator.
        See KeyValueIterator docs for further reference.
        """
        self.conn = conn
        self.tableNo = tableNo
        self.limit = limit
        self.nextStartKey = startKey
        self.endKey = endKey
        self.limit = limit
        self.keyFilter = keyFilter
        self.valueFilter = valueFilter
        self.chunkSize = chunkSize
        self.skip = skip
        self.invert = invert
        self.buf = deque()
    def __iter__(self):
        return self
    def __loadNextChunk(self):
        """
        Load the next chunk of key-value pairs into the buffer
        """
        listRes = self.conn.list(self.tableNo, startKey=self.nextStartKey, endKey=self.endKey, limit=self.chunkSize, keyFilter=self.keyFilter, valueFilter=self.valueFilter, skip=self.skip, invert=self.invert)
        #Stop if there's nothing left to scan
        if len(listRes) is 0:
            raise StopIteration
        for key in listRes:
            self.buf.append(key)
        #Get the key to use as start key on chunk load
        lastIdentifier = (listRes[-1])
        self.nextStartKey = YakDBUtils.incrementKey(lastIdentifier)
    def next(self):
        """
        Get the next key-value pair
        """
        if len(self.buf) == 0:
            self.__loadNextChunk() #raises StopIteration if needed
        return self.buf.popleft()

class JobIterator(object):
    """
    An iterator that iterates over key-value pairs from a job.
    The iterator yields tuples (key, value).
    """
    def __init__(self, job):
        """
        Initialize a new job key-value iterator
        @param job the job object to use
        @param limit The number of nodes to load at once
        """
        self.job = job
        self.buf = deque()
    def __iter__(self):
        return self
    def __loadNextChunk(self):
        """
        Load the next chunk into the buffer
        """
        chunk = self.job.requestDataChunk()
        #Stop if there's nothing left to scan
        if not chunk: raise StopIteration
        self.buf = deque(chunk)
    def __next__(self):
        """
        Get the next node
        """
        if len(self.buf) == 0:
            self.__loadNextChunk() #raises StopIteration if needed
        return self.buf.popleft()
    def next(self): return self.__next__()