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
        """
        for c in bytearray(id):
            if c < 32:
                return False
        return True
    @staticmethod
    def checkIdentifier(id):
        """
        If the given string is not an identifier
        """
        if not Identifier.isIdentifier(id):
            raise IdentifierException("String '%s' is not a valid identifier! Ensure its binary representation only contains characters >= 32")
