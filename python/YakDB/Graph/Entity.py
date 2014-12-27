#!/usr/bin/env python3
# -*- coding: utf8 -*-

from YakDB.Graph.BasicAttributes import BasicAttributes
from YakDB.Graph.ExtendedAttributes import ExtendedAttributes

class Entity(object):
    """
    An abstract representation of a graph entity that provides utility methods.
    This should not be used directly.
    """
    def initializeAttributes(self, basicAttributes):
        """
        Initialize self.basicAttributes and self.extendedAttributes.
        """
        #Parse basic attrs if needed
        if type(basicAttributes) is str:
            basicAttributes = BasicAttributes._parseAttributeSet(basicAttributes)
        self.basicAttributes = BasicAttributes(self, basicAttributes)
        self.extendedAttributes = ExtendedAttributes(self)
