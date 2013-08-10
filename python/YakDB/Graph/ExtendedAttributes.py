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
        Initialize a basic attribute set.
        """
        self.entity = entity
    @staticmethod
    def getAttribute(self, key):
        """
        Get a single attribute by key
        """
        return self.entity._getExtendedAttribute(key)
    def addAttribute(self,  key,  value):
        """
        Add or replace an attribute
        """
        self.entity._saveExtendedAttributes({key: value})
    def setAttributes(self,  attrDict):
        """
        For any attribute in the given dictionary,
        set or replace the corresponding attribute in the current instance.
        """
        if type(attrDict) is not dict:
            raise ParameterException("attrDict parameter must be a Dictionary!")
        self.entity._saveExtendedAttributes(attrDict)
    def deleteAttribute(self,  key):
        """
        Delete a single attribute by key.
        The call is ignored if the attribute does not exist.
        """
        self.entity._deleteExtendedAttributes([key])
    def getAttributes(self,  startKey="",  limit=1000):
        """
        Get a dictionary of all attributes.
        @param startKey The first attribute to get (inclusive)
        @param limit The maximum number of keys to get
        """
        return self.entity._getExtendedAttributes(startKey,  limit)
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

if __name__ == "__main__":
    import doctest
    doctest.testmod()
