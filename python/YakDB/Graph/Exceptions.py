#!/usr/bin/env python3
# -*- coding: utf8 -*-

class IdentifierException(Exception):
    """
    An exception that indicates that the given string
    does not fulfil the requirements for 
    """
    def __init__(self, message):
        Exception.__init__(self, message)

class ConsistencyException(Exception):
    """
    An exception that indicates that two arguments
    are not consistent with each other.
    """
    def __init__(self, message):
        Exception.__init__(self, message)
