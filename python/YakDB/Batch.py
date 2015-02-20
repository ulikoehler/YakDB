#!/usr/bin/env python3
# -*- coding: utf8 -*-
from YakDB.Conversion import ZMQBinaryUtil
from YakDB.Exceptions import ParameterException

class AutoWriteBatch(object):
    """
    An utility class that auto-batches write requests to a backend Connection
    When calling flush, a put request is issued to the backend.
    Write request are automatically issued on batch overflow and
    object deletion.
    """
    def __init__(self, conn, tableNo, batchSize=2500, partsync=False, fullsync=False):
        """
        Create a new AutoWriteBatch.
        @param db The ZeroDB connection backend
        @param tableNo TConnectionhe table number this batch is related to.
        """
        self.conn = conn
        self.tableNo = tableNo
        self.batchSize = batchSize
        self.partsync = partsync
        self.fullsync = fullsync
        self.batchData = {}
    def put(self, valueDict):
        """
        Write a dictionary of values to the current batch.
        Note that this is slower than adding the keys one-by-one
        because of the merge method currently being used
        """
        if type(valueDict) is not dict:
            raise ParameterException("Batch put valueDict parameter must be a dictionary but it's a %s" % str(type(valueDict)))
        #Merge the dicts
        self.batchData = dict(self.batchData.items() + valueDict.items())
        self.__checkFlush()
    def putSingle(self, key, value):
        """
        Write a single key-value pair to the current batch
        """
        #Convert the key and value to a appropriate binary form (also checks if obj type is supported)
        #Without this, tracing back what added a key/value with an inappropriate type would not be possible
        convKey = ZMQBinaryUtil.convertToBinary(key)
        convValue = ZMQBinaryUtil.convertToBinary(value)
        self.batchData[convKey] = convValue
        self.__checkFlush()
    def __checkFlush(self):
        """
        Issues a flash if self.batchData overflowed
        """
        if len(self.batchData) >= self.batchSize:
            self.flush()
    def flush(self):
        """
        Immediately issue the backend write request and clear the batch write queue.
        It is NOT neccessary to flush before the object is deleted!
        """
        if len(self.batchData) != 0:
            self.conn.put(self.tableNo, self.batchData, self.partsync, self.fullsync)
            self.batchData = {}
    def __del__(self):
        self.flush()
