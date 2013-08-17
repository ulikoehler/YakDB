#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Exceptions import ParameterException

class BasicAttributes(object):
    """
    An instance of this class, which is always
    related to an entity instance that has basic attributes,
    represents the set of basic attributes for that class.
    """
    def __init__(self,  entity,  attrs=None, autosave=True):
        """
        Initialize a basic attribute set.
        @param attrs The attributes, or None to use empty set
        @param autosave If this is set to true, write-[] operators (__setitem__, __delitem__
            cause immediate writes to the database.
        """
        self.entity = entity
        self.autosave = autosave
        if attrs is None:
            self.attrs = {}
        else:
            self.attrs = attrs
    def __getitem__(self, key):
        """
        Get an attribute by key
        """
        if key in self.attrs:
            return self.attrs[key]
        return None
    def __setitem__(self, key, value):
        """
        Set an attribute.
        This always s
        """
        self.attrs[key] = value
        if self.autosave:
            self.save()
    def __delitem__(self, key):
        """
        Delete an attribute from the current attributes.
        """
        del self.attrs[key]
        if self.autosave:
            self.save()
    def __iter__(self):
        return self.attrs.__iter__()
    def setAttributes(self, attrDict, save=True):
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
        self.entity.save()
    def serialize(self):
        """
        Serialize the current attribute set
        """
        return BasicAttributes._serialize(self.attrs)
    @staticmethod
    def _parseAttributeSet(attrSet):
        """
        Parse a serialized attribute set.
        
        @param attrSet A string containing the serialized attributes
        @return A dictionary of the parsed values
        
        >>> BasicAttributes._parseAttributeSet("k1\\x1Fval1\\x1Ekey2\\x1Fvalue2\\x1E")
        {'key2': 'value2', 'k1': 'val1'}
        >>> BasicAttributes._parseAttributeSet("")
        {}
        """
        ret = {}
        while len(attrSet) > 0:
            keyEnd = attrSet.index('\x1F')
            valueEnd = attrSet.index('\x1E')
            key = attrSet[:keyEnd]
            value = attrSet[keyEnd+1:valueEnd]
            attrSet = attrSet[valueEnd+1:]
            ret[key] = value
        return ret
    @staticmethod
    def _serialize(attrDict):
        """
        Serializes a dictionary of attributes into its representation in the
        database.
        
        >>> BasicAttributes._serialize({'key1': 'value1', 'k2': 'val2'})
        'k2\\x1fval2\\x1ekey1\\x1fvalue1\\x1e'
        """
        serializedAttrs = ["%s\x1F%s\x1E" % (key,value)
                           for key, value in attrDict.iteritems()]
        return b"".join(serializedAttrs)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
