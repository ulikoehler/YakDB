#!/usr/bin/env python
# -*- coding: utf8 -*-
from Exceptions import *

class Identifier:
    """
    Provides utilities to check if a string
    represents a valid YakDB Graph identifier.
    """
    @staticmethod
    def isIdentifier(id):
        """
        @param id The identifier to test
        @return True if the given string is a valid identifier
        
        >>> Identifier.isIdentifier("abcdefg")
        True
        >>> Identifier.isIdentifier("node:mynode")
        True
        >>> Identifier.isIdentifier("")
        True
        >>> Identifier.isIdentifier("no\\x00:de")
        False
        >>> Identifier.isIdentifier("no\\x1F:de")
        False
        >>> Identifier.isIdentifier("no\\xFF:de")
        False
        """
        for c in bytearray(id):
            if c < 32:
                return False
            if c == 255:
                return False
        return True
    @staticmethod
    def checkIdentifier(id):
        """
        If the given string is not an identifier
        """
        if not Identifier.isIdentifier(id):
            raise IdentifierException("String '%s' is not a valid identifier! Ensure its binary representation only contains characters >= 32")
    @staticmethod
    def incrementKey(key):
        """
        @param key An identifier 
        @return A DB key immediately following the given key.
        
        >>> Identifier.incrementKey("node:abc")
        'node:abd'
        >>> Identifier.incrementKey("node:")
        'node;'
        >>> Identifier.incrementKey("node;")
        'node<'
        >>> Identifier.incrementKey("x")
        'y'
        """
        #Ensure the argument is an identifier
        Identifier.checkIdentifier(key)
        #Increment the last char.
        keyList = list(key)
        newLastChar = ord(keyList[-1])+1
        keyList[-1] = chr(newLastChar)
        return "".join(keyList)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
