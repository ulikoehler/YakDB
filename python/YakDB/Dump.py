#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Database dump / dump import for YakDB
Provides utility functions for dumping
"""
import struct

keyValueMagicByte = 0x6DE0

def __writeKeyValue(f, key, value):
    """
    Append a single key-value record to a file-like object
    
    Keyword arguments:
        f -- The file-like object to write to
        key -- The key to write, in any binary writable form (not unicode)
        value -- The value to write, in any binary writable form (not unicode)
    """
    hdr = struct.pack("<HQQ", keyValueMagicByte, len(key), len(value))
    f.write(hdr)
    f.write(key)
    f.write(value)

def dump(conn, tableNo, startKey=None, endKey=None, limit=None, keyFilter=None, valueFilter=None, chunkSize=1000):
    """
    Dump a table by using a snapshotted table version.
    """
    initializePassiveDataJob(conn, 