#!/usr/bin/env python3
# -*- coding: utf8 -*-

import msgpack
from YakDB.InvertedIndex import EntityInvertedIndex

class MsgpackEntityInvertedIndex(EntityInvertedIndex):
    """
    Lightweight msgpack-based EntityInvertedIndex wrapper.
    If msgpack is available, prefer using this class over
    EntityInvertedIndex.
    """
    def packValue(self, entity):
        """Pack = Serialize an entity"""
        return msgpack.packb(entity)
    def unpackValue(self, packedEntity):
        """Unpack = deserialize an entity from the database."""
        assert packedEntity
        return msgpack.unpackb(packedEntity)