#!/usr/bin/env python
# -*- coding: utf8 -*-

from Identifier import Identifier
from YakDB.Exceptions import ParameterException

class ExtendedAttributes(object):
    """
    An instance of this class, which is always
    related to an entity instance that has basic attributes,
    represents the set of basic attributes for that class.
    """
    def __init__(self,  entity):
        """
        Initialize a new extended attribute set.
        """
        self.entity = entity
        self.entityId = entity.id
        self.graph = entity.graph
    def __getitem__(self, key):
        """
        Get an attribute by key
        """
        dbKey = ExtendedAttributes._serializeKey(self.entityId, key)
        return self.graph._loadExtendedAttributes(dbKey)[0]
    def __setitem__(self, key, value):
        """
        Set an attribute.
        This always saves the attribute in the database
        """
        Identifier.checkIdentifier(key)
        dbKey = ExtendedAttributes._serializeKey(self.entityId, key)
        self.graph._saveExtendedAttributes({dbKey: value})
    def __delitem__(self, key):
        """
        Delete an attribute from the current attributes.
        """
        self.graph._deleteExtendedAttributes([key])
    def getAttributes(self, keys):
        """
        Get a list of attributes by key.
        Using this function is faster that individual key reads
        @param keys An array of keys to read
        """
        dbKeys = [self._serializeKey(self.entityId, key) for key in keys]
        return self.graph._loadExtendedAttributes(dbKeys)
    def getAllAttributes(self,  startKey="", limit=None):
        """
        Get a dictionary of all attributes.
        """
        return self.getAttributeRange()
    def getAttributeRange(self,  startKey=None, endKey=None, limit=None):
        """
        Get a dictionary of all attributes.
        @param startKey The first attribute to get (inclusive)
        @param endKey The last attribute to get (exclusive)
        @param limit The maximum number of keys to get
        """
        #Serialize the database range keys
        dbStartKey = ExtendedAttributes._getEntityStartKey(self.entityId)
        dbEndKey = ExtendedAttributes._getEntityEndKey(self.entityId)
        if startKey is not None:
            dbStartKey = ExtendedAttributes._serializeKey(self.entityId, startKey)
        if endKey is not None:
            dbEndKey = ExtendedAttributes._serializeKey(self.entityId, endKey)
        #Scan
        return self.graph._loadExtendedAttributeRange(dbStartKey, dbEndKey, limit=limit)
    def setAttributes(self,  attrDict):
        """
        For any attribute in the given dictionary,
        set or replace the corresponding attribute in the current instance.
        """
        if type(attrDict) is not dict:
            raise ParameterException("attrDict parameter must be a Dictionary!")
        dbDict = {}
        for key, value in attrDict.iteritems():
            dbKey = ExtendedAttributes._serializeKey(self.entityId,  key)
            dbDict[dbKey] = value
        self.graph._saveExtendedAttributes(dbDict)
    def deleteAttribute(self,  key):
        """
        Delete a single attribute by key.
        The call is ignored if the attribute does not exist.
        """
    @staticmethod
    def _serializeKey(entityId, key):
        """
        Serialize the database key for an extended attribute.
        >>> ExtendedAttributes._serializeKey("myId","thekey")
        'myId\\x1dthekey'
        """
        Identifier.checkIdentifier(key)
        return "%s\x1D%s" % (entityId,  key)
    @staticmethod
    def _getAttributeKeyFromDBKey(dbKey):
        """
        Given an extended attribute database key, extracts the key from it
        >>> ExtendedAttributes._getAttributeKeyFromDBKey("node1\x1Dattr1")
        'attr1'
        >>> ExtendedAttributes._getAttributeKeyFromDBKey("mynode\x1Dtest")
        'test'
        """
        return dbKey[dbKey.find("\x1D")+1:]
    @staticmethod
    def _getEntityStartKey(entityId):
        """
        Get the start key for scanning all extended attributes of an entity
        
        >>> ExtendedAttributes._getEntityStartKey("mynode")
        'mynode\\x1d'
        """
        return "%s\x1D" % entityId
    @staticmethod
    def _getEntityEndKey(entityId):
        """
        Get the end key for scanning all extended attributes of an entity
        
        >>> ExtendedAttributes._getEntityEndKey("mynode")
        'mynode\\x1e'
        """
        return "%s\x1E" % entityId

if __name__ == "__main__":
    import doctest
    doctest.testmod()
