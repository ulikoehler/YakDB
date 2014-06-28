# YakDB external protocol specification

## Low-Level protocol

The protocol shall use ZMTP/2.0 as defined in [ZeroMQ RFC 15](http://rfc.zeromq.org/spec:15).
The protocol is frame-based. Frames are equivalent to ZeroMQ message parts or
CZMQ frames.

The server shall provide multiple configurable ZeroMQ endpoints,
including (all optional):
    - A ROUTER endpoint for read operations and non-optimistic writes
    - A PULL endpoint for optimistic writes
    - A SUB socket for PGM-based optimistic redundancy writes
    - A ROUTER socket for mapreduce requests
    - A PUB socket for publishing log messages

## General behaviour

For request/reply sockets the server shall always send an response.
The server shall send responses as soon as possible.

Additional bytes in the header frame shall always be ignored by both
client and server.

For request/response connections, the server must always respond
with a response that has a response type equivalent
to the request type, unless it can't recognize the request at all, it shall
respond with the protocol error response listed below.

##### Endianness

All integral frames shall be interpreted as little-endian by both the client and server.
This decision was made because the main target platforms for ZeroDB, x86/x64 and ARM
are little-endian and conversion to the network byte order would not only decrease
performance but also increase API complexity. Every client would have to ensure
all integral values are converted properly

##### API Classes

*TODO* Req/req-only APIs vs Req/Rep+Push/Pub APIs

##### Asynchronous process IDs (APIDs)

For some compute-intensive data-processing-type requests,
the server spawns one or multiple background threads instead of processing
the request in the Update/Worker threadsets.

The reply for such requests is sent during the initialization phase.
Therefore, the client can expect the server to send a reply within a
fairly low timeout for all request types, unless in death-by-swap or
extremely-high-load situations. Additionally, TCP connections
don't need to be kept open while processing long-duration data analysis
applications.

For such requests, the server assings and returns a 64-bit asynchronous process ID (APID).
('Process' is not neccessarily related to system processed).
This ID can be used to query information about process status from the server.

Statistical information will not always be completely up-to-date, because it's
handled as low-priority. It will just give an indication of what's going on,
no exact or synchronized numbers.
All statistics are flushed after the job has finished, so when the job
is marked as completed (or failed), the statistics are guaranteed to be reliable.

APIDs are not related to system process IDs etc. in any way.

### Protocol error response

For request/reply sockets, the server uses this response if it can't recognize
the protocol.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0xFF Response type (protocol error)]
* Frame 1 (Optional): NUL-terminated protocol error description

### Request types

Request and response codes are divided into several groups to allow fast & easy boolean-logic-based routing:

* Low Nibble: Minor request type (assigned serially, it's just incremented whenever a new request is added)
* High Nibble: Major request type
    * 0x0: Meta / Initialization request
    * 0x1: Read-only request
    * 0x2: Write request (may also read, but write shall be dominant)
    * 0x4: Async data processing (MapRed etc.) bit
    * 0x5 (= 0x4 + 0x1): Data processing read requests
    * 0x6 (= 0x4 + 0x2): Data processing write requests

-----------------------

## Initialization/utility requests

##### Server info request:

You can use this to test if a server is online and if it's compatible.
* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x00 Request type (info request)]

##### Server info response:

Frame structure:
* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x00 Response type (info response)][8 bytes Supported Features]
* Frame 1: NUL-Terminated server software info

Supported features size might be expanded in future versions without protocol version change.
Clients shall therefore ignore additional bytes in the first response frame.

[Supported Features]: 64-bit integer, consisting of bitwise-OR-concatenated flags:
* 0x01: Server supports on-the-fly table open
* 0x02: Server supports (does not ignore) PARTSYNC
* 0x04: Server supports (does not ignore) FULLSYNC

For non-REQ/REP-type sockets the server shall ignore the PARTSYNC flag.

##### Table open request

Table Open request: Opens a specified table. Creates the table if not already present.
Tables can also be opened on-the-fly (optional feature), but you can't specify compression etc. in this case.
Additionally, opening tables takes a considerable amount of time, therefore

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x01 Request type (table open requst)]
* Frame 1: [4-byte unsigned integer table number]
* Frame 2-n (even numbers): Option key
* Frame 3-n (odd numbers): Option value

Table open options are specified as key/value pairs (both string).
Allowed key/value pairs:

* 'LRUCacheSize': LRU cache size in bytes (unsigned)
* 'Blocksize': Table blocksize in bytes (unsigned)
* 'WriteBufferSize': Write buffer size in bytes (unsigned)
* 'BloomFilterBitsPerKey': Bits per key for table bloom filter (unsigned)
* 'CompressionMode': String code for the table compression mode (see below)
* 'MergeOperator': String code for the table merge operator (see below)

Supported compression modes:

* NONE
* SNAPPY
* ZLIB
* BZIP2
* LZ4
* LZ4HC

Supported merge operators:

* REPLACE (default)
* INT64ADD (64-bit signed add)
* DMUL (64-bit double IEEE754 multiply)
* APPEND (Binary append)

##### Table Open response

Sent after table has been opened

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x01 Response type (open table response)] [1-byte response code]
* Frame 1 (Only present if response code indicates an error): NUL-terminated error string, UTF-8 encoded

Response codes:
* 0x00 Table opened successfully
* 0x10 Error while opening table (implies frame 1 being existent and non-empty)

##### Close table request

Close a table (e.g. to save memory) - includes flushing (not O_DIRECT) the unwritten table data to disk.

This request may only be used if no operations are active only the table for the duration
of the request, or the behaviour is undefined.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x02 Request type (table close request)]
* Frame 1: 4-byte unsigned integer table number

##### Close table response

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x02 Response type (open table response)] [1-byte response code]
* Frame 1 (Only present if response code indicates an error): NUL-terminated error string, UTF-8 encoded

Response codes:
* 0x00 Table closed successfully
* 0x10 Error while close table (implies frame 1 being existent and non-empty)

##### Compact request

Compact a table (clear the log and rebuild immutable table files). Could take some time.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x03 Request type (compact request)]
* Frame 1: 4-byte unsigned table number
* Frame 2: Start key (inclusive). If this has zero length, the compact starts at the first key
* Frame 3: End key (inclusive). If this has zero length, the compact ends at the last key

Frame 0-4 must be present under all circumstances.

##### Compact response:

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x03 Response type (compact response)][1-byte response code]
* Frame 1 (if response code indicates an error): NUL-terminated string describing the error

Response codes:
* 0x00 Success (--> frame 1 not present)
* 0x01 Error (--> frame 1 contains error description cstring)


##### Truncate request

Close a table and truncate all its contents.

This request may only be used if no operations are active only the table for the duration
of the request, or the behaviour is undefined.

Note that the table needs to be opened manually after the truncate request
if you wish to use non-standard table open options.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x04 Request type (truncate request)]
* Frame 1: 4-byte unsigned table number

##### Truncate response

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x04 Request type (truncate response)][1-byte response code]
* Frame 1 (if response code indicates an error): NUL-terminated string describing the error

Response codes:
* 0x00 Success (--> frame 1 not present)
* 0x01 Error (--> frame 1 contains error description cstring)


##### Stop server request

This request causes the server to shutdown cleanly.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x05 Request type (server stop request)]

##### Stop server response

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x05 Request type (server stop request)][1 byte status code]

Status codes:
    * 0x00 Success, server stopping
    * 0x01 Permission denied


-------------------------------

## Read-only requests

##### Read request

Read one or multiple keys (random-access) at once.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x10 Request type (read request)]
* Frame 1: 4-byte unsigned integer table number
* Frame 2-n: Each frame contains an arbitrary byte sequence containing the key to be read

None of the frames may be empty under any circumstances. Empty frames may lead to undefined behaviour.

##### Read response:

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x10 Response type (read response)][1-byte Response code]
* Frame 1-n: Read results (values only) in the same order as the requests, zero-sized if not found
* Frame 1 (if response code indicates an error): Error message

Response codes:
* 0x00 Success (--> frame 1 contains first value)
* 0x10 Error (--> frame 1 contains error description cstring)


##### Count request

Count the number of keys in a given range

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x11 Request type (count request)]
* Frame 1: 4-byte unsigned table number
* Frame 2: Start key (inclusive). If this has zero length, the count starts at the first key
* Frame 3: End key (inclusive). If this has zero length, the count ends at the last key

If frame 2 and 3 are not present, the full key range (=entire table) is counted
If only frame 2, but not frame 3 is present, frame 3 is treated as if it was zero-length.

##### Count response:

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x11 Response type (count response)][1-byte response code]
* Frame 1 (if response code indicates an error): NUL-terminated string describing the error
* Frame 1 (if response code indicates success): A 64-bit unsigned integer representing the number of values found in the given range (count)

Response codes:
* 0x00 Success (--> frame 1 contains count)
* 0x10 Error (--> frame 1 contains error description cstring)


##### Exists request

Check for existence of one or multiple keys in a table.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x12 Request type (exists request)]
* Frame 1: 4-byte unsigned integer table number
* Frame 2-n: Each frame contains an arbitrary byte sequence containing the key to be read

None of the frames may be empty under any circumstances. Empty frames may lead to undefined behaviour.

##### Exists response:

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x12 Response type (exists response)][1-byte Response code]
* Frame 1-n: Each frame has a size of 1 byte. If the byte is binary zero, the key doesn't exist in the table. Else, it exists.
* Frame 1 (if response code indicates an error): Error message

The frames 1-n are in the same order as the request keys.

Note that technically reads and exists are practically the same unless a the table is opened using a bloom filter.
In this case, exist request for keys not being present in the database are faster.

Response codes:
* 0x00 Success (--> frame 1 contains first value)
* 0x10 Error (--> frame 1 contains error description cstring)

##### Scan request

Read a range of keys at once ("read range request").
The scan ends when one of the following conditions are met:
- The end of the table is reached
- The end key is reached (unless the end key frame is zero-sized)
- The amount of key-value pairs scanned is equal to the limit (unless the limit frame is zero-sized)

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x13 Request type (scan request)][1 byte scan flags]
* Frame 1: 32-bit unsigned table number
* Frame 2: 64-bit unsigned limit. If this is zero-sized, no limit is imposed
* Frame 3: Start key (inclusive). If this has zero length, the count starts at the first key
* Frame 4: End key (exclusive). If this has zero length, the count ends at the last key
* Frame 5: Key substring filter (frame shall be zero-sized if no filter shall be applied)
* Frame 6: Value substring filter (frame shall be zero-sized if no filter shall be applied)
* Frame 7: 64-bit unsigned skip count (Zero-length frame --> 0. Specifies how many records are skipped.)

The substring filter provides fast (Boyer-Moore-Horspool) server-side filtering for keys and values.
Only
Filtered keys don't decrease the key-value count that is used to check the limit.
In any case, the filters are compared in a case-sensitive way on a char-by-char basis.

If there are filters and those filters do not match the key/value pair, the skip counter is not decremented.

Regardless of filters and skipping, the scan will stop at the end key.

**Scan flags:**
OR combination of these flags (default: reset):
* Bit 1: Invert scan. Set this flag to invert the scan direction.

*Note:* If the invert scan flag is set, the iterator is (just like with the non-inverted scan) moved
to the start key. The scan is continued until the end key is reached. It is therefore neccessary
to set *end key* > *start key* (requests that dont fulfill this requirement are still valid,
but yield an empty result). For the non-inverted scan, the opposite (*end key* <= *start key*) is true.
For inverted scan requests, the results are always returned in inverted order.

##### Scan response:

The scanned request returns the scan range as alternating key/value frames

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x13 Response type (scan response)][1-byte Response code]
* Frame 1-n (odd numbers): Next key
* Frame 1-n (even numbers): Next value (corresponds to key = previous frame)

Response codes:

* 0x00 Success (--> frame 1 contains first value)
* 0x10 Error (--> frame 1 contains error description cstring)

##### List request

Equivalent to the scan request, but only returns keys, not values.

See the scan request documentation for further reference

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x14 Request type (list request)][1 byte scan flags]
* Frame 1: 32-bit unsigned table number
* Frame 2: 64-bit unsigned limit. If this is zero-sized, no limit is imposed
* Frame 3: Start key (inclusive). If this has zero length, the count starts at the first key
* Frame 4: End key (exclusive). If this has zero length, the count ends at the last key
* Frame 5: Key substring filter (frame shall be zero-sized if no filter shall be applied)
* Frame 6: Value substring filter (frame shall be zero-sized if no filter shall be applied)
* Frame 7: 64-bit unsigned skip count (Zero-length frame --> 0. Specifies how many records are skipped.)

##### List response:

The list response returns the list range as alternating key/value frames

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x14 Response type (list response)][1-byte Response code]
* Frame 1-n: Next key

Response codes:

* 0x00 Success (--> frame 1 contains first value)
* 0x10 Error (--> frame 1 contains error description cstring)

-------------------------------

## Write requests

### Write flags: These flags can optionally be supplied with write requests (bitwise-OR different flags to get the flag bytes

* 0x01 PARTSYNC: Hand request over to database backend before acknowledging. Does not imply synchronous disk writes.
* 0x02 FULLSYNC: Force synchronous write to database

Requests that are not flagged PARTSYNC are called ASYNC.
The server shall acknowledge as soon as feasible after receiving the message.
ASYNC replies do not guarantee that the request has been checked for errors before acknowledging.
Using the PARTSYNC flag guarantees any subsequent write request yields the newly written value.

PARTSYNC may not be sent for non-REQ-REP sockets. Sending PARTSYNC over other sockets may lead to undefined behaviour.

##### Put request:

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x20 Request type (Put request)] [1 byte write flags]
* Frame 1-n (odd frame numbers): Key to write to. The next frame specifies the value to write
* Frame 2-n (even frame numbers): Value to write. The previous frame specifies the corresponding key.

If both the key and value frames are empty, the frame pair is ignored.

##### Delete request:

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x21 Request type (Delete request)] [1 byte write flags]
* Frame 1: 4-byte unsigned table number
* Frame 2-n: Key to delete (may contain arbitrary byte sequence)

None of the frames may be empty under any circumstances. Empty frames may lead to undefined behaviour.

##### Delete range request:

Deletes data until one of these events occurs:
- The end of the table is reached
- The end key is reached (if any)
- The limit is reached (if any)

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x22 Request type (Delete range request)] [1 byte Write flags]
* Frame 1: 4-byte unsigned table number
* Frame 1: 8-byte unsigned limit (or zero-length frame --> no limit)
* Frame 2: Start key (inclusive). If this has zero length, the count starts at the first key
* Frame 3: End key (exclusive). If this has zero length, the count ends at the last key

##### Multi-table write request:

** NOT IMPLEMENTED YET! **

The standard put request only allows to transactionally write into a single table.
The Multi-table write requests introduce additional overhead, but they allow
writes to different tables.

For auto-loadbalancing socket types (REQ, PUSH), using this request type
allows to group several operations into a single message and therefore
guarantee the entire dataset

The PARTSYNC flag is not allowed (and therefore ignored), because
the multi-table put request is internally translated to multiple individual put requests.
Response to this request type will therefore always be async.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x23 Request type (Put request)] [1-byte Write flags]

An arbitrary number of *modification messages* follow frame 0. Modification messages consist of (frame numbering relative to the mod msg start):
* Frame 0: [32-bit table no][32-bit number of valuesets in request][8-bit request type]
* Frame 1-n: List of keys for delete request type, alternating key-values for put request type

Request type:
    0x00: Put - number of valuesets is (total number of frames - 1)/2
    0x01: Delete - number of valuesets is (total number of frames - 1)

##### Write response

The response format is identical for all write-type requests

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][Response type (Same as request type)] [1 byte Response code]
* Frame 1 (Only present if response code indicates an error): NUL-terminated error string, UTF-8 encoded

Response codes (lower byte counts!):
* 0x00 Acknowledge (Only acknowledges that the request has been received)
* 0x01 Error, unspecified or unknown
* 0x02 Database error while processing request (implies Frame 1 being existing)
* 0x10 Protocol error, found key frame without value frame (implies Frame 1 being existing)

If the 0x10 bit is set, the server signals that the write has been applied partially
and can't be rolled back automatically.
If the 0x10 bit is unset, the server signals none of the updates was applied because
of an automatic batch rollback.

Note: If not using PARTSYNC flag, the server will always send a non-error acknowledge code,
because processing has not started when the reply is sent.

-------------------------------

## Data processing initialization / meta request

These requests are closely related to the MapReduce protocol,
as outlined in mapred-protocol.md.

##### 'Initialize range for data chunk requests' requests

This request spawns a new thread that waits for data chunks request

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x40 Request type (Forward range to socket request)]
* Frame 1: 4-byte unsigned table number
* Frame 2: Start key (inclusive). If this has zero length, the count starts at the first key
* Frame 3: End key (inclusive). If this has zero length, the count ends at the last keys
* Frame 4: 64-bit unsigned integer, interpreted as the limit of keys to scan.

The server may spawn a new thread to serve the request.
Replying to the request does not indicate any kind of success.

##### Forward range to socket response

**WIP** REQUEST FORMAT MAY CHANGE ; NOT IMPLEMENTED YET

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x40 Response type (Forward range to socket response)]
* Frame 1: 8-byte unsigned Job ID

##### Server-side table-sinked map initialization request (SSTSMIR)

**WIP** REQUEST FORMAT MAY CHANGE ; NOT IMPLEMENTED YET

Initializes a scan request whose result is not returned to the requesting instances,
but instead piped through an LLVM-based client-specified mapper.
The mapper output is then saved in a table.

This request uses snapshots for the input table, writing to the input table
is therefore possible without any special precautions.

For the mapper, both insertion and deletion is possible.

LLVM API is described in llvm-api.md

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x41 Request type (SSMTSSIR)]
* Frame 1: 4-byte unsigned integer input table number
* Frame 2: 4-byte unsigned integer output table number (may be the same as input table no)
* Frame 3: 4-byte unsigned integer, the number of concurrent worker threads to spawn
* Frame 4-n: Initialization parameters for the mapper, as alternating key-value pairs. (n === 1 mod 2)
* Frame n+1: Empty delimiter frame
* Frame n+2: LLVM bitcode

##### SSTSMIR response

The response is sent once the job has started.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version [Response type (Same as request type)] [1 byte Response code]
* Frame 1: 8-byte little-endian unsigned integer APID (can be used to retrieve process state etc)
* Frame 2 (Only present if response code indicates an error): NUL-terminated error string, UTF-8 encoded

Response codes (lower byte counts!):
* 0x00 Acknowledge (Only acknowledges that the request has been received)
* 0x01 Error, unspecified or unknown
* 0x02 Database error while processing request (implies Frame 1 being existing)
* 0x10 Protocol error, found key frame without value frame (implies Frame 1 being existing)

##### CSPTMIR (Client-Side Passive table map initialization request)

This request initializes a job with a REP socket that waits for requests from clients and deliverse data blocks upon
request. The data chunksize size is configurable. This request is called passive because the server waits for client
requests passively and does not actively send data without requests. It is called client-side because

This request uses snapshots for the source table.
It is upon the client how the data is handled. The client may write the data to a table (writing to the input table is allowed),
or write it to a file etc.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x42 Request type (CSPTMIR)]
* Frame 1: 4-byte unsigned integer input table number
* Frame 2: Empty or 4-byte chunksize (= number of key-value structures that will be returned upon request)
* Frame 3: 8-byte number of keys to scan limit (or empty --> no limit)
* Frame 4: Start key (inclusive). If this has zero length, the count starts at the first key
* Frame 5: End key (inclusive). If this has zero length, the count ends at the last keys

If frame 2 is empty, a default chunksize shall be assumed.

##### CSPTMIR Response

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x42 Response type (CSPTMIR)]
* Frame 1: 64-bit APID

##### CSATMIR (Client-Side Passive table map initialization request)

**WIP** REQUEST FORMAT MAY CHANGE ; NOT IMPLEMENTED YET

This request initializes a job with a REP socket that waits for requests from clients and deliverse data blocks upon
request. The data chunksize size is configurable. This request is called passive because the server waits for client
requests passively and does not actively send data without requests. It is called client-side because

This request uses snapshots for the source table.
It is upon the client how the data is handled. The client may write the data to a table (writing to the input table is allowed),
or write it to a file etc.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x43 Request type (CSATMIR)]
* Frame 1: 4-byte unsigned integer input table number
* Frame 2: Empty or 4-byte chunksize (= number of key-value structures that will be returned upon request)
* Frame 3: 8-byte number of keys to scan limit (or empty --> no limit)
* Frame 4: Start key (inclusive). If this has zero length, the count starts at the first key
* Frame 5: End key (inclusive). If this has zero length, the count ends at the last keys

If frame 2 is empty, a default chunksize shall be assumed.

##### CSATMIR Response

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x42 Response type (CSATMIR)]
* Frame 1: 64-bit APID


##### Job statistics request (JobStatR)

**WIP** REQUEST FORMAT MAY CHANGE ; NOT IMPLEMENTED YET

This request can be used to query statistical information about running jobs and jobs that have already terminated.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x48 Request type (JobStatR)][8-bit statistics request type]

This request uses several sub-requests, determined by the 'statistics request type' header field:
    * 0x00 Show APID statistics.
        For each APID, yields a list of frames:
            * Header: [64-bit APID] [8-bit job type] [8-bit job state]
            * Alternating keys and values (statistics info)
            * Empty delimiter frame (to separate from next entry)

Job type:
    0x00: Undefined
    0x10: Client-side passive
    0x11: Client-side active
    0x12: Server-side (LLVM)
    0x20: Table copy

Job state:
    0x00: Unknown
    0x10: Initializing
    0x20: Running
    0x30: Terminating
    0x40: Terminated

-------------------------------

## Data processing read requests

##### Client data request

This request type must only be used with APIDs that have been returned by CSPTMIRs.
By using this request, clients request a single data chunk at a time.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x50 Request type]
* Frame 1: 64-bit APID

##### Client data response

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x50 Response type][8-bit response flags]
* Frame 1-n (odd frame numbers): Key, corresponds to value in next frame
* Frame 2-n (even frame numbers): Value, corresponds to key in previous frame

The message shall contain at most chunksize*2+1 frames.

Response flags:
    0x01: No more data (--> last frame, client shall not request more frames as no data will be returned)
    0x02: Partial data (--> last frame, less than *chunksize* KV pairs). May not occur together with "No more data" flag.

----------------------------------

## Data processing Write requests

##### Table range copy request

**WIP** REQUEST FORMAT MAY CHANGE ; NOT IMPLEMENTED YET

Copies a range of a table to another table.
No modification of keys or values is performed.

This request uses a snapshot to process the table data.

The range+limit-behaviour is equivalent to the behaviour of scan requests.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x60 Request type][8-bit Write flags]
* Frame 1: 32-bit unsigned source table number
* Frame 2: 32-bit unsigned destination table number
* Frame 3: 64-bit unsigned int limit. If this is zero-sized, no limit is imposed.
* Frame 4: Start key (inclusive). If this has zero length, the count starts at the first key
* Frame 5: End key (exclusive). If this has zero length, the count ends at the last key

##### Table range copy response

The server returns an APID.

Check the APID to get information about the copy progress.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x60 Response type]
* Frame 1: 64-bit APID
