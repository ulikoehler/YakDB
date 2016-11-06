#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
Database dump / dump import for YakDB
Provides utility functions for dumping
"""
from __future__ import with_statement
import struct
import gzip
import lzma
import tempfile
import os.path
from YakDB.Batch import AutoWriteBatch

__keyValueMagicByte = 0x6DE0
__headerMagicByte = 0x6DDF
__headerVersionByte = 0x0001

def __writeYDFFileHeader(f):
    """
    Write the YDF file header to a file-like object
    """
    f.write(struct.pack("<HH", __headerMagicByte, __headerVersionByte))

def __verifyYDFFileHeader(f):
    """
    Read a YDF file header from a given file-like object,
    verify it and throw an exception if it doesn't match
    """
    hdr = f.read(4)
    if len(hdr) != 4:
        raise ValueError("Tried to read 4-byte YDF header, but only got %d bytes. Assuming invalid header" % len(hdr))
    magicByte, version = struct.unpack("<HH", hdr)
    if magicByte != __headerMagicByte:
        raise ValueError("YDF file header magic word mismatches, expected %d but was %d" % (__headerMagicByte, magicByte))
    if version != __headerVersionByte:
        raise ValueError("YDF file header version mismatches, expected %d but was %d" % (__headerVersionByte, version))

def __writeYDFKeyValue(f, key, value):
    """
    Append a single key-value record to a file-like object

    Keyword arguments:
        f -- The file-like object to write to
        key -- The key to write, in any binary writable form (not unicode)
        value -- The value to write, in any binary writable form (not unicode)
    """
    hdr = struct.pack("<HQQ", __keyValueMagicByte, len(key), len(value))
    f.write(hdr + key + value)

def __readYDFKeyValue(f):
    """
    Read a YDF-formatted key-value block.

    Return value: A tuple (key, value) or None if there is no record left
    """
    kvHdr = f.read(18) #2 magic word + 8 key size + 8 value size
    #Check if any data could be read
    if len(kvHdr) == 0: return None
    #Unpack binary format and check header
    magicByte, keySize, valueSize = struct.unpack("<HQQ", kvHdr)
    if magicByte != __keyValueMagicByte:
        raise ValueError("YDF Key-Value header magic word mismatches, expected %d but was %d" % (__keyValueMagicByte, magicByte))
    key = f.read(keySize)
    value = f.read(valueSize)
    return (key, value)


def dumpYDF(conn, outputFilename, tableNo, startKey=None, endKey=None, limit=None, chunkSize=1000):
    """
    Dump a table to YDF by using a snapshotted table version.
    """
    job = conn.initializePassiveDataJob(tableNo, startKey, endKey, limit, chunkSize)
    #Transparent compression
    openFunction = open
    if outputFilename.endswith(".gz"): openFunction = gzip.open
    if outputFilename.endswith(".xz"): openFunction = lzma.open
    with openFunction(outputFilename, "wb") as outfile:
        __writeYDFFileHeader(outfile)
        for key, value in job:
            __writeYDFKeyValue(outfile, key, value)

def importYDFDump(conn, inputFilename, tableNo):
    """
    Import a database dump in YDF format
    """
    #Auto-batch writes
    batch = AutoWriteBatch(conn, tableNo)
    #Transparent decompression
    openFunction = open
    if inputFilename.endswith(".gz"): openFunction = gzip.open
    if inputFilename.endswith(".xz"): openFunction = lzma.open
    with openFunction(inputFilename, "rb") as infile:
        __verifyYDFFileHeader(infile)
        while True:
            ret = __readYDFKeyValue(infile)
            #None --> EOF
            if ret is None: break
            key, value = ret
            batch.putSingle(key, value)


def copyTable(conn, srcTable, targetTable, truncate=False, extension=None, startKey=None, endKey=None, limit=None, **kwargs):
    """
    Copies an entire table to another, deleting all values in the first table.
    Unless truncate is True, this function uses deleteRange, making the operation fully safe
    This minimizes downtime of the target by deleting the target table as late as possible

    This operation first exports a snapshotted YDF dump of 

    @param srcTable The table to read from. This table is not modified and read using a snapshot
    @param targetTable The table to write to. This table is deleted entirely before updating its values,
        making this operation feasible with arbitrary merge operators
    @param extension The extension of the generated YDF file. Using ".xz" or ".gz" here
        saves disk space in /tmp, but the operation is usually slower.
    @param startKey: This start key is used for both dumping and deletion. Useful to update only part of a table
    @param endKey: This end key is used for both dumping and deletion. Useful to update only part of a table
    @param limit: This limit is used for both dumping and deletion. Rarely useful
    @param kwargs Passed onto dumpYDF()
    @return A dictionary of the returned key/value pairs
    """
    # Create temporary directory to store dump in
    tempdir = tempfile.TemporaryDirectory(prefix="YakPythonClient")
    filename = "t{}-t{}-copy.ydf{}".format(srcTable, targetTable, extension or "")
    dumpfile = os.path.join(tempdir, filename)
    try:
        print("Dumping to " + dumpfile)
        # Generate dump
        dumpYDF(conn, dumpfile, srcTable, startKey=startKey, endKey=endKey, limit=limit, **kwargs)
        # Delete target table
        if truncate:
            conn.truncate(targetTable)
        else:
            conn.deleteRange(targetTable, startKey=startKey, endKey=endKey, limit=limit)
        # Import table
        importYDFDump(conn, dumpfile, targetTable)
    finally:
        tempdir.cleanup()
