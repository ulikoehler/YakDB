#!/usr/bin/env python3
# -*- coding: utf8 -*-

class YakDBUtils:
    """
    This class provides static utility methods for using YakDB.
    """
    @staticmethod
    def incrementKey(key):
        """
        Increment a database key
        
        >>> YakDBUtils.incrementKey(b"node:abc")
        b'node:abd'
        >>> YakDBUtils.incrementKey(b"node:")
        b'node;'
        >>> YakDBUtils.incrementKey(b"node;")
        b'node<'
        >>> YakDBUtils.incrementKey(b"x")
        b'y'
        >>> YakDBUtils.incrementKey(b"node\\xFF;")
        b'node\\xff<'
        >>> YakDBUtils.incrementKey(b"x\\xFF")
        b'y\\xff'
        >>> YakDBUtils.incrementKey(b"\\xFF\\xFF")
        b'\\xff\\xff\\x00'
        """
        #Increment the last char that is != \xFF
        if isinstance(key, str): key = key.encode("utf-8")
        keyList = list(key)
        #Find & increment the last non-\xFF char 
        for idx in range(-1,(-1)-len(keyList),-1):
            lastChar = keyList[idx]
            if lastChar == 255:
                continue
            newLastChar = lastChar + 1
            keyList[idx] = newLastChar
            #If continue above wasn't called, return immediately
            return bytes(keyList)
        #The key consists of 0xFF characters only: Extend length
        return key + b"\x00"

def makeUnique(coll):
    """Return coll with duplicate instances removed. The ordering is maintained"""
    uniqueResults = set()
    return [r for r in coll if r not in uniqueResults and not uniqueResults.add(r)]
    
if __name__ == "__main__":
    import doctest
    doctest.testmod()
