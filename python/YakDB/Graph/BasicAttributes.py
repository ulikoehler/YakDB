#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Exceptions import ParameterException

class BasicAttributes(object):
    """
    An instance of this class, which is always
    related to an entity instance that has basic attributes,
    represents the set of basic attributes for that class.
    """
    def __init__(self,  entity,  attrs=None):
        """
        Initialize a basic attribute set.
        @param attrs The attributes, or None to use empty set
        """
        self.entity = entity
        if attrs is None:
            self.attrs = {}
        else:
            self.attrs = attrs
    @staticmethod
    def getAttribute(self, key):
        """
        Get a single attribute by key
        """
        if key in self.attrs:
            return self.attrs[key]
        return None
    def setAttribute(self,  key,  value, save=True):
        """
        Set or an attribute. Replaces existing attributes.
        """
        self.attrs[key] = value
        if save:
            self.save()
    def setAttributes(self,  attrDict, save=True):
        """
        For any attribute in the given dictionary,
        set or replace the corresponding attribute in the current instance.
        """
        if type(attrDict) is not dict:
            raise ParameterException("attrDict parameter must be a Dictionary!")
        for key, value in attrDict.iteritemns():
            self.attrs[key] = value
        if save:
            self.save()
    def deleteAttribute(self,  key,  save=True):
        """
        Delete a single attribute by key.
        The call is ignored if the attribute does not exist.
        """
        if key in self.attrs:
            del self.attrs[key]
        if save:
            self.save()
    def getAttributes(self):
        """
        Get a dictionary of all attributes
        """
        return self.attrs
    def save(self):
        """
        Save the current set of attributes to the database.
        There is no need to call this explicitly unless you used
        save=False flag on previous changes. Any change without
        save=False automatically calls this method.
        """
        self.entity._save()
    @staticmethod
    def _parseAttributeSet(attrSet):
        """
        Parse a serialized attribute set.
        
        @return A dictionary of the parsed values
        
        >>> BasicAttributes._parseAttributeSet("k1\\x00val1\\x00key2\\x00value2\\x00")
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
            else: #It's not a NUL delimiter
                if currentlyInValue:
                    currentValue += c
                else:
                    currentKey += c
        return ret
    @staticmethod
    def _serialize(attrDict):
        """
        Serializes a dictionary of attributes into its representation in the
        database.
        
        >>> BasicAttributes._serialize({'key1': 'value1', 'k2': 'val2'})
        'k2\\x00val2\\x00key1\\x00value1\\x00'
        """
        serializedAttrs = []
        for key, value in attrDict.iteritems():
            serializedAttrs.append("%s\x00" % key)
            serializedAttrs.append("%s\x00" % value)
        return b"".join(serializedAttrs)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
