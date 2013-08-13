#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Graph.Identifier import Identifier
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
        return self.graph.readExtendedAttributes(self.entityId, key)[0]
    def __setitem__(self, key, value):
        """
        Set an attribute.
        This always saves the attribute in the database
        """
        Identifier.checkIdentifier(key)
        self.graph.saveExtendedAttributes(self.entityId, {key: value})
    def __delitem__(self, key):
        """
        Delete an attribute from tahe current attributes.
        """
        self.deleteAttributes([key])
    def getAttributes(self, keys):
        """
        Get a list of attributes by key.
        Using this function is faster that individual key reads
        @param keys An array of keys to read
        """
        return self.graph._readExtendedAttributes(dbKeys)
    def getAllAttributes(self,  limit=None):
        """
        Get a dictionary of all attributes.
        """
        return self.getAttributeRange(limit=limit)
    def getAttributeRange(self,  startKey=None, endKey=None, limit=None):
        """
        Get a dictionary of all attributes.
        @param startKey The first attribute to get (inclusive)
        @param endKey The last attribute to get (exclusive)
        @param limit The maximum number of keys to get
        """
        return self.graph.scanExtendedAttributes(self.entityId, startKey, endKey, limit=limit)
    def setAttributes(self,  attrDict):
        """
        For any attribute in the given dictionary,
        set or replace the corresponding attribute in the current instance.
        @param attrDict The attribute dictionary to set
        """
        self.graph.saveExtendedAttributes(self.entityId, attrDict)
    def deleteAttributes(self,  keys):
        """
        Delete a list of attributes at once.
        Attributes that dont exist are ignored silently
        @param keys An array of strings or a single string that represent keys to delete.
        """
        self.graph.deleteExtendedAttributes(self.entityId, keys)
    def deleteAll(self):
        """
        Deletes ALL extended attributes for the current node.
        """
        self.deleteAttributeRange() #Default args = everything
    def deleteAttributeRange(self, startKey=None, endKey=None, limit=None):
        """
        Deletes a range of attributes.
        """
        self.graph.deleteExtendedAttributeRange(entityId, startKey, endKey, limit)
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
    @staticmethod
    def _getEntityScanKeys(entityId):
        """
        @return (_getEntityStartkey(id), _getEntityEndKey(id))
        """
        
        return (ExtendedAttributes._getEntityStartKey(entityId),
                ExtendedAttributes._getEntityEndKey(entityId))

if __name__ == "__main__":
    import doctest
    doctest.testmod()
