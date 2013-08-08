#!/usr/bin/env python
# -*- coding: utf8 -*-

def __parseAttributeSet(attrSet):
    """
    Parse a serialized attribute set.
    
    >>> __parseAttributeSet("k1\\x00val1\\x00key2\\x00value2\\x00")
    {'key2': 'value2', 'k1': 'val1'}
    """
    ret = {}
    currentlyInValue = False
    currentKey = ""
    currentValue = ""
    for c in attrSet:
        #Check for cstring NUL delimiter
        if ord(c) == 0:
            if currentlyInValue:
                ret[currentKey] = currentValue
                currentKey = ""
                currentValue = ""
            currentlyInValue = not currentlyInValue
        else: #It's not a NUL terminator
            if currentlyInValue:
                currentValue += c
            else:
                currentKey += c
    return ret

class BasicAttributes(object):
    """
    An instance of this class, which is always
    related to an entity instance that has basic attributes,
    represents the set of basic attributes for that class.
    """
    def __init__(self,  entity,  attrSet):
        """
        Initialize a basic attribute set.
        """
        self.entity = entity
        self.attrs = self.__parseAttributeSet(attrSet)
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

if __name__ == "__main__":
    import doctest
    doctest.testmod()
