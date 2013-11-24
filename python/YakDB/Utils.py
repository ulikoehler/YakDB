#!/usr/bin/env python
# -*- coding: utf8 -*-

from YakDB.Exceptions import ParameterException

class YakDBUtils:
    """
    This class provides static utility methods for using YakDB.
    """
    @staticmethod
    def incrementKey(key):
        """
        Increment a database key
        
        >>> YakDBUtils.incrementKey("node:abc")
        'node:abd'
        >>> YakDBUtils.incrementKey("node:")
        'node;'
        >>> YakDBUtils.incrementKey("node;")
        'node<'
        >>> YakDBUtils.incrementKey("x")
        'y'
        >>> YakDBUtils.incrementKey("node\\xFF;")
        'node\\xff<'
        >>> YakDBUtils.incrementKey("x\\xFF")
        'y\\xff'
        >>> YakDBUtils.incrementKey("\\xFF\\xFF")
        '\\xff\\xff\\x00'
        """
        #Increment the last char that is != \xFF
        if isinstance(key, unicode): key = key.encode("utf-8")
        keyList = list(key)
        #Find & increment the last non-\xFF char 
        for idx in range(-1,-1-len(keyList),-1):
            lastChar = keyList[idx]
            if lastChar == '\xFF':
                continue
            newLastChar = ord(lastChar)+1
            keyList[idx] = chr(newLastChar)
            #If continue above wasn't called, return immediately
            return "".join(keyList)
        #The key consists of 0xFF characters only
        return key + "\x00"

def makeUnique(coll):
    """Return coll with duplicate instances removed. The ordering is maintained"""
    uniqueResults = set()
    return [r for r in coll if r not in uniqueResults and not uniqueResults.add(r)]
    
if __name__ == "__main__":
    import doctest
    doctest.testmod()
