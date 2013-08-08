#!/usr/bin/env python
# -*- coding: utf8 -*-

class IdentifierException(Exception):
    """
    An exception that indicates that the given string
    does not fulfil the requirements for 
    """
    def __init__(self, message):
        Exception.__init__(self, message)
