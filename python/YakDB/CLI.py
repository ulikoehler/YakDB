#!/usr/bin/env python3
# -*- coding: utf8 -*-

#YakDB commandline tool
import argparse
import sys
import code
import os
import os.path
import YakDB
import YakDB.Dump
from YakDB.Iterators import KeyIterator, KeyValueIterator

def info(db, args):
    print((db.serverInfo()).decode("utf-8"))

def tableInfo(db, args):
    print(db.tableInfo(args.table))

def stop(db, args):
    db.stopServer()

def read(db, args):
    tableNo = args.tableNo
    keys = args.keys
    output = db.read(tableNo, keys) #Always use custom mapping
    #Convert value-only to key-->value map if not --print-raw is set
    if args.printRaw:
        for line in output:
            print(line)
    else:
        outMap = {}
        for index, inval in enumerate(keys):
            outMap[inval] = output[index]
        print(outMap)

def put(db, args):
    tableNo = args.tableNo
    keysValues = args.keyValuePairs
    #Build the key/value dict
    if len(keysValues) % 2 != 0:
        print(("Didn't find a value for key %s, ignoring that key" % keysValues[-1]))
        keysValues = keysValues[0:-1]
    keys = keysValues[0::2]
    values = keysValues[1::2]
    putDict = dict(list(zip(keys, values)))
    db.put(tableNo, putDict)
    #Convert value-only to key-->value map
    if not args.quiet:
        for key, value in putDict.items():
            print(("Successfully put '%s' --> '%s'" % (key, value)))

def exists(db, args):
    tableNo = args.tableNo
    keys = args.keys
    result = db.exists(tableNo, keys)
    if args.printNumeric:
        result = [1 if x else 0 for x in result]
    #Convert value-only to key-->value map
    if args.printRaw:
        for value in result:
            print(value)
    else: #Print dictionary
        outMap = {}
        for index, inval in enumerate(keys):
            outMap[inval] = result[index]
        print(outMap)

def delete(db, args):
    tableNo = args.tableNo
    keys = args.keys
    db.delete(tableNo, keys)
    if not args.quiet:
        print(("Deleted [%s]" % ", ".join(keys)))

def deleteRange(db, args):
    tableNo = args.tableNo
    fromKey = args.fromKey
    toKey = args.toKey
    limit = args.scanLimit
    #Data is remapped into dictionary-form in connection class
    db.deleteRange(tableNo, fromKey, toKey, limit)

def scan(db, args):
    tableNo = args.tableNo
    #Override -t with positional argument, if any
    if args.table is not None:
        tableNo = args.table
    fromKey = args.fromKey
    toKey = args.toKey
    limit = args.scanLimit
    keyFilter = args.keyFilter
    valueFilter = args.valueFilter
    invert = args.invertDirection
    skip = args.skip
    #Lazily iterate over key value tuples
    it = KeyValueIterator(db, tableNo, fromKey, toKey, limit, keyFilter=keyFilter, valueFilter=valueFilter, skip=skip, invert=invert)
    for key, value in it:
        if args.onlyKeys:
            #Print key but strip "b'" and "'"
            print(str(key)[2:-1])
        else: #Print k,v
            print("{0},{1}".format(key, value))

def listKeysInRange(db, args):
    tableNo = args.tableNo
    #Override -t with positional argument, if any
    if args.table is not None:
        tableNo = args.table
    fromKey = args.fromKey
    toKey = args.toKey
    limit = args.scanLimit
    keyFilter = args.keyFilter
    valueFilter = args.valueFilter
    invert = args.invertDirection
    skip = args.skip
    codec = args.codec
    #Lazily iterate over the key list
    it = KeyIterator(db, tableNo, fromKey, toKey, limit, keyFilter=keyFilter, valueFilter=valueFilter, skip=skip, invert=invert)
    if codec == "binary":
        for key in it:
            print(key)
    else:
        for key in it:
            print(key.decode(codec))


def dump(db, args):
    tableNo = args.tableNo
    #Override -t with positional argument, if any
    if args.table is not None:
        tableNo = args.table
    fromKey = args.fromKey
    toKey = args.toKey
    limit = args.scanLimit
    outputFile = args.outputFile
    YakDB.Dump.dumpYDF(db, outputFile, tableNo, fromKey, toKey, limit)

def importDump(db, args):
    tableNo = args.tableNo
    #Override -t with positional argument, if any
    if args.table is not None:
        tableNo = args.table
    inputFile = args.inputFile
    if not os.path.exists(inputFile):
        print("Error: Input file '%s' does not exist!" % inputFile, file=sys.stderr)
        sys.exit(1)
    if not os.path.isfile(inputFile):
        print("Error: Input file '%s' is not a file!" % inputFile, file=sys.stderr)
        sys.exit(1)
    YakDB.Dump.importYDFDump(db, inputFile, tableNo)

def count(db, args):
    tableNo = args.tableNo
    #Override -t with positional argument, if any
    if args.table is not None:
        tableNo = args.table
    fromKey = args.fromKey
    toKey = args.toKey
    print((db.count(tableNo, fromKey, toKey)))

def compact(db, args):
    tables = [args.tableNo]
    fromKey = args.fromKey
    toKey = args.toKey
    if len(args.tables) > 0:
        tables = args.tables
    for table in tables:
        db.compactRange(table, fromKey, toKey)
        if not args.quiet:
            print("Compaction finished")

def openTable(db, args):
    compression = args.compression
    mergeOperator = args.mergeOperator
    lruCacheSize = args.lruCacheSize
    writeBufferSize = args.writeBufferSize
    blocksize = args.blocksize
    bloomFilterBitsPerKey = args.bloomFilterBitsPerKey
    for table in args.tables:
        db.openTable(table, lruCacheSize=lruCacheSize, writeBufferSize=writeBufferSize,
                    tableBlocksize=blocksize, bloomFilterBitsPerKey=bloomFilterBitsPerKey,
                    compression=compression, mergeOperator=mergeOperator)
        if not args.quiet:
            print("Opening table #%d finished" % table)

def closeTable(db, args):
    tables = [args.tableNo]
    if len(args.tables) > 0:
        tables = args.tables
    for table in tables:
        db.closeTable(table)
        if not args.quiet:
            print("Closing table #%d finished" % table)

def truncateTable(db, args):
    tables = [args.tableNo]
    if len(args.tables) > 0:
        tables = args.tables
    for table in tables:
        db.truncateTable(table)
        if not args.quiet:
            print("Truncating table #%d finished" % table)


def repl(db, args):
    code.InteractiveConsole(locals={"db":db}).interact("YakDB REPL -- Connection is available in 'db' variable")

def yakCLI():
    """
    Call this function to use the yak CLI on sys.argv.
    """
    if sys.version_info.major < 3:
        print("YakDB requires Python 3 to run. Please run yak using a python3k interpreter!")

    parser = argparse.ArgumentParser(description="YakDB client tool")
    #Server optionstype=int
    serverArgsGroup = parser.add_argument_group(parser, "Server options")
    serverArgsGroup.add_argument("-s","--server",
            help="Specifies server URL to connect to",
            default="tcp://localhost:7100",
            action='store',
            dest="serverURL")
    serverArgsGroup.add_argument("-g","--server-group",
            help="Specifies the server group to connect to for PUB/SUB connections",
            action='store',
            dest="serverGroup")
    serverArgsGroup.add_argument("-c","--connection-mode",
            choices=["PUB","PUSH","REQ"],
            default="REQ",
            action="store",
            dest="connectMode",
            help="The connection mode to use for requests. Most requests only work in REQ mode.")
    #Database options
    dbOptsGroup = parser.add_argument_group(parser, "Database options")
    dbOptsGroup.add_argument("-t","--table",
            help="The table number / ID to use",
            action="store",
            default=1,
            type=int,
            dest="tableNo")
    #CLI options
    cliOptsGroup = parser.add_argument_group(parser, "CLI options")
    #Data is remapped in connection class
    cliOptsGroup.add_argument("-q","--quiet",
            help="Don't print connection info",
            action="store_true",
            dest="quiet",
            default=False)
    cliOptsGroup.add_argument("-r","--repl",
            help="Start into a REPL where db is the database connection",
            action="store_true",
            default=False,
            dest="repl")
    ###
    #Create parsers for the individual commands
    ###
    subparsers = parser.add_subparsers(title="Commands")
    #Info
    parserInfo = subparsers.add_parser("info", description="Request server information (features and version number)")
    parserInfo.set_defaults(func=info)
    #Info
    parserTableInfo = subparsers.add_parser("table-info", description="Request table information")
    parserTableInfo.add_argument('table',
        type=int,
        nargs='?',
        action="store",
        help="The tables to get info from. Overrides -t option.")
    parserTableInfo.set_defaults(func=tableInfo)
    #Stop
    parserStop = subparsers.add_parser("stop", description="Stop the YakDB server")
    parserStop.set_defaults(func=stop)
    #Read
    parserRead = subparsers.add_parser("read", description="Read the values for one or more keys")
    parserRead.add_argument('keys',
            nargs='+',
            action="store",
            help="The keys to read")
    parserRead.add_argument('-p','--print-raw',
            dest="printRaw",
            action="store_true",
            default=False,
            help="Print only the values read (separated by newline), not a dictionary")
    parserRead.set_defaults(func=read)
    #Put
    parserPut = subparsers.add_parser("put", description="Write a single key-value pair")
    parserPut.add_argument('keyValuePairs',
            action="store",
            nargs="+",
            help="The keys / values to write, in alternating order (key value key2 value2 ...)")
    parserPut.set_defaults(func=put)
    #Exists
    parserExists = subparsers.add_parser("exists", description="Check if one or more keys exist in the table")
    parserExists.add_argument('keys',
            nargs='+',
            action="store",
            help="The keys to check for existence")
    parserExists.add_argument('-n','--numeric-boolean',
            dest="printNumeric",
            action="store_true",
            default=False,
            help="Instead of printing True or False, print 1 or 0")
    parserExists.add_argument('-p','--print-raw',
            dest="printRaw",
            action="store_true",
            default=False,
            help="Do not print a map from key to its existence value, but only the existence values in the same order as the keys")
    parserExists.set_defaults(func=exists)
    #Delete
    parserDelete = subparsers.add_parser("delete", description="Delete one or more keys")
    parserDelete.add_argument('keys',
            nargs='+',
            action="store",
            help="The keys to delete (keys that don't exist will be ignored)")
    parserDelete.set_defaults(func=delete)
    #Delete range
    parserDeleteRange = subparsers.add_parser("delete-range", description="Delete a range of keys")
    parserDeleteRange.add_argument('--from',
            action="store",
            dest="fromKey",
            default=None,
            help="The key to start deleting at (inclusive), default: start of table")
    parserDeleteRange.add_argument('--to',
            action="store",
            dest="toKey",
            default=None,
            help="The key to stop deleting at (exclusive), default: end of table")
    parserDeleteRange.add_argument('-l','--limit',
            action="store",
            dest="scanLimit",
            type=int,
            default=None,
            help="The maximum number of keys to delete")
    parserDeleteRange.set_defaults(func=deleteRange)
    #Scan
    parserScan = subparsers.add_parser("scan", description="Scan over a specified range in the table and return all key-value-pairs in that range")
    parserScan.add_argument('--from',
            action="store",
            dest="fromKey",
            default=None,
            help="The key to start scanning at (inclusive), default: Start of table")
    parserScan.add_argument('--to',
            action="store",
            dest="toKey",
            default=None,
            help="The key to stop scanning at (exclusive), default: end of table")
    parserScan.add_argument('-k','--key-filter',
            action="store",
            dest="keyFilter",
            default=None,
            help="The server-side key filter. Ignored KV pairs don't count when calculating the limit.")
    parserScan.add_argument('-v','--value-filter',
            action="store",
            dest="valueFilter",
            default=None,
            help="The server-side value filter. Ignored KV pairs don't count when calculating the limit.")
    parserScan.add_argument('-l','--limit',
            action="store",
            dest="scanLimit",
            type=int,
            default=None,
            help="The maximum number of keys to scan")
    parserScan.add_argument('-s','--skip',
            type=int,
            nargs='?',
            default=0,
            action="store",
            help="How many records to skip. Only filter-passing records count as skipped.")
    parserScan.add_argument('-i','--invert-direction',
            action="store_true",
            dest="invertDirection",
            default=False,
            help="Invert the scan direction")
    parserScan.add_argument('--only-keys',
            action="store_true",
            dest="onlyKeys",
            default=False,
            help="Only print keys, omit values completely")
    parserScan.add_argument('table',
            type=int,
            nargs='?',
            action="store",
            help="The tables to scan. Overrides -t option.")
    parserScan.set_defaults(func=scan)
    #List
    parserList = subparsers.add_parser("list", description="List keys in a specified range in the table and return all key-value-pairs in that range")
    parserList.add_argument('--from',
            action="store",
            dest="fromKey",
            default=None,
            help="The key to start listing at (inclusive), default: Start of table")
    parserList.add_argument('--to',
            action="store",
            dest="toKey",
            default=None,
            help="The key to stop listing at (exclusive), default: end of table")
    parserList.add_argument('-k','--key-filter',
            action="store",
            dest="keyFilter",
            default=None,
            help="The server-side key filter. Ignored KV pairs don't count when calculating the limit.")
    parserList.add_argument('-v','--value-filter',
            action="store",
            dest="valueFilter",
            default=None,
            help="The server-side value filter. Ignored KV pairs don't count when calculating the limit.")
    parserList.add_argument('-n','--limit',
            action="store",
            dest="scanLimit",
            type=int,
            default=None,
            help="The maximum number of keys to scan")
    parserList.add_argument('-s','--skip',
            type=int,
            nargs='?',
            default=0,
            action="store",
            help="How many records to skip. Only filter-passing records count as skipped.")
    parserList.add_argument('-i','--invert-direction',
            action="store_true",
            dest="invertDirection",
            default=False,
            help="Invert the scan direction")
    parserList.add_argument('-c','--codec',
            action="store_true",
            dest="codec",
            default="utf-8",
            help="Which codec to use for printing (or 'binary')")
    parserList.add_argument('table',
            type=int,
            nargs='?',
            action="store",
            help="The table to list. Overrides -t option.")
    parserList.set_defaults(func=listKeysInRange)
    #Count
    parserCount = subparsers.add_parser("count", description="Count how many keys exist in a specified range of the table")
    parserCount.add_argument('--from',
            action="store",
            dest="fromKey",
            default=None,
            help="The key to start scanning at (inclusive)")
    parserCount.add_argument('--to',
            action="store",
            dest="toKey",
            default=None,
            help="The key to stop scanning at (exclusive)")
    parserCount.add_argument('table',
            type=int,
            nargs='?',
            action="store",
            help="The tables to count in. Overrides -t option.")
    parserCount.set_defaults(func=count)
    #Compact
    parserCompact = subparsers.add_parser("compact", description="Compact a specified range of the table\nThis operation may take a long time, especially for large tables.")
    parserCompact.add_argument('tables',
            nargs='*',
            action="store",
            type=int,
            help="The tables to compact. Overrides -t option.")
    parserCompact.add_argument('--from',
            action="store",
            dest="fromKey",
            default=None,
            help="The key to start compacting at (inclusive), default: start of table")
    parserCompact.add_argument('--to',
            action="store",
            dest="toKey",
            default=None,
            help="The key to stop compacting at (exclusive), default: end of table")
    parserCompact.set_defaults(func=compact)
    #Open table
    parserOpenTable = subparsers.add_parser("open", description="Open a table.\nThis is only neccessary if you intend to use nonstandard open options.")
    parserOpenTable.add_argument('tables',
            nargs='+',
            type=int,
            action="store",
            help="The table ID to open")
    parserOpenTable.add_argument('-c','--compression',
            dest="compression",
            default="SNAPPY",
            help="Blocklevel compression code (NONE, SNAPPY, ZLIB, BZIP2, LZ4 or LZ4HC)")
    parserOpenTable.add_argument('-m','--merge-operator',
            dest="mergeOperator",
            default="REPLACE",
            help="Merge operator (e.g. REPLACE, INT64ADD, DMUL or APPEND)")
    parserOpenTable.add_argument('-l','--lru-cache-size',
            action="store",
            dest="lruCacheSize",
            type=int,
            default=None,
            help="The size of the LRU cache where uncompressed block data will be stored. Increasing this yields better performance for random-read-access-heavy workloads.")
    parserOpenTable.add_argument('-w','--write-buffer-size',
            action="store",
            dest="writeBufferSize",
            type=int,
            default=None,
            help="The size of the write buffer. Increasing this yields better write performance")
    parserOpenTable.add_argument('-b','--blocksize',
            action="store",
            dest="blocksize",
            type=int,
            default=None,
            help="The table blocksize. Larger blocks yield better compression and scan performance, but usually worse random-access performance. See LevelDB docs for details")
    parserOpenTable.add_argument('-f','--bloom-filter-bits-per-key',
            action="store",
            dest="bloomFilterBitsPerKey",
            type=int,
            default=None,
            help="The number of bloom filter bits per key. Increasing this improves read & exist performance for keys that don't exist, but increases memory usage.")
    parserOpenTable.set_defaults(func=openTable)
    #Close table
    parserCloseTable = subparsers.add_parser("close", description="Close a table\nThis is usually not neccessary, unless you want to save memory. ")
    parserCloseTable.add_argument('tables',
            nargs='*',
            type=int,
            action="store",
            help="The tables to compact. Overrides -t option.")
    parserCloseTable.set_defaults(func=closeTable)
    #Close table
    parserTruncateTable = subparsers.add_parser("truncate", description="Close and truncate a table. Deletes the table's files on filesystem-level.")
    parserTruncateTable.add_argument('tables',
            nargs='*',
            type=int,
            action="store",
            help="The tables to compact. Overrides -t option.")
    parserTruncateTable.add_argument('-y','--yes',
            dest="confirmed",
            action="store_true",
            default=False,
            help="Skip the 'Do you really want to truncate?' question")
    parserTruncateTable.set_defaults(func=truncateTable)
    #Dump
    parserDump = subparsers.add_parser("dump", description="Dump a specified range of a table (default: entire table) into a YDF format. Automatically creates a table snapshot for the dump.")
    parserDump.add_argument('-o','--output',
            action="store",
            dest="outputFile",
            required=True,
            help="The file to write the YDF dump to. Append .gz to use gzipped output (recommended)")
    parserDump.add_argument('--from',
            action="store",
            dest="fromKey",
            default=None,
            help="The key to start dumping at (inclusive), default: Start of table")
    parserDump.add_argument('--to',
            action="store",
            dest="toKey",
            default=None,
            help="The key to stop dumping at (exclusive), default: end of table")
    parserDump.add_argument('-l','--limit',
            action="store",
            dest="scanLimit",
            type=int,
            default=None,
            help="The maximum number of keys to scan")
    parserDump.add_argument('table',
            type=int,
            nargs='?',
            action="store",
            help="The table to dump. Overrides -t option.")
    parserDump.set_defaults(func=dump)
    #Import
    parserImport = subparsers.add_parser("import", description="Import a YDF dump")
    parserImport.add_argument('-i','--input',
            action="store",
            dest="inputFile",
            required=True,
            help="The file to read the YDF dump from. Append .gz to use transparent decompression.")
    parserImport.add_argument('table',
            type=int,
            nargs='?',
            action="store",
            help="The table to import to. Overrides -t option.")
    parserImport.set_defaults(func=importDump)
    #REPL
    parserREPL = subparsers.add_parser("repl", description="Start a Read-eval-print loop (REPL) for interactive DB usage")
    parserREPL.set_defaults(func=repl)
    ###
    #Parse and call the function
    ###
    args = parser.parse_args()
    db = YakDB.Connection()
    if not args.quiet:
        sys.stderr.write("Connecting to %s\n" % args.serverURL)
    if args.connectMode == "PUSH":
        db.usePushMode()
    elif args.connectMode == "PUB":
        db.usePubMode()
    else:
        db.useRequestReplyMode()
    db.connect(args.serverURL)
    #For some reason, the default=info setting only works with Python2
    try:
        args.func(db, args)
    except AttributeError:
        info(db, args)
