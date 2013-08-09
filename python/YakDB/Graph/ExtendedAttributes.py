#!/usr/bin/env python
# -*- coding: utf8 -*-

def __serializeExtendedAttributeKey(entityId,  key):
    """
    Serialize the database key for an extended attributes
    """

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
        if key in self.attrs:
            return self.attrs[key]
        return None
    def addAttribute(self,  key,  value):
        """
        Add or replace an attribute
        """
        self.attrs[key] = value
    def deleteAttribute(self,  key):
        """
        Delete a single attribute by key.
        The call is ignored if the attribute does not exist.
        """
        if key in self.attrs:
            del self.attrs[key]
    def getAttributes(self):
        """
        Get a dictionary of all attributes
        """
        return self.attrs
    def reload(self):
        """
        Reload the basic attribute set from the database.
        Replaces the currently stored attribute set.
        """
        self.attrs = self.entity.__reloadBasicAttributes()
