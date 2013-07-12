# ZeroDB external protocol specification

## General behaviour

For request/reply sockets the server shall always send an response.
The server shall send responses as soon as possible.

If a message isn't recognizable by the server, it shall send this protocol error message.

Additional bytes in the header frame shall always be ignored by both
client and server.

### Protocol error response

For request/reply sockets, the server uses this response if it can't recognize
the protocol.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0xFF Response type (protocol error)]
* Frame 1 (Optional): NUL-terminated protocol error description

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

[Supported Features]: 64-bit field, Little-Endian, with bitwise-OR-concatenated flags:
* 0x01: Server supports on-the-fly table open
* 0x02: Server supports (does not ignore) PARTSYNC
* 0x04: Server supports (does not ignore) FULLSYNC

##### Table open request

Table Open request: Opens a specified table. Creates the table if not already present.
Tables can also be opened on-the-fly (optional feature), but you can't specify compression etc. in this case.
Additionally, opening tables takes a considerable amount of time, therefore

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x01 Request type (table open requst)][1 byte Table open flags]
* Frame 1: [4-byte little-endian unsigned integer table number]
* Frame 2: [8 bytes little-endian unsigned integer LRU cache size (in bytes) or zero-length to assume default]
* Frame 3: [8 bytes little-endian unsigned integer table blocksize (in bytes) or zero-length to assume default]
* Frame 4: [8 bytes little-endian unsigned integer write buffer size (in bytes) or zero-length to assume default]
* Frame 4: [8 bytes little-endian unsigned integer bloom filter bits per key or zero-length to use no bloom filter]

* *[Table open flags]*: 8-bit-field, with bitwise-OR-concatenated flags.
* 0x01: NOCOMPRESSION: If this flag is set the table shall be opened with compression disabled

##### Table Open response

Sent after table has been opened

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x01 Response type (open table response)] [1-byte response code]
* Frame 1 (Only present if response code indicates an error): NUL-terminated error string, UTF-8 encoded

Response codes:
* 0x00 Table opened successfully
* 0x10 Error while opening table (implies frame 1 being existent and non-empty)

##### Close table request

Close a table (e.g. to save memory) - includes flushing (not sync) the unwritten table data to disk.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x02 Request type (table close request)]
* Frame 1: 4-byte little-endian unsigned integer table number

##### Close table response

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x02 Response type (open table response)] [1-byte response code]
* Frame 1 (Only present if response code indicates an error): NUL-terminated error string, UTF-8 encoded

Response codes:
* 0x00 Table closed successfully
* 0x10 Error while close table (implies frame 1 being existent and non-empty)
    
##### Compact request

Compact a table (clear the log and rebuild immutable table files). Could take some time.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x03 Request type (compact request)]
* Frame 1: 4-byte little-endian unsigned table number
* Frame 2: Start key (inclusive). If this has zero length, the compact starts at the first key
* Frame 3: End key (inclusive). If this has zero length, the compact ends at the last key

Frame 0-4 must be present under all circumstances.

##### Compact response:

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x03 Response type (compact response)][1-byte response code]
* Frame 1 (if response code indicates an error): NUL-terminated string describing the error

Response codes:
* 0x00 Success (--> frame 1 not present)
* 0x01 Error (--> frame 1 contains error description cstring)

###### Truncate request

Close a table and truncate all its contents.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x04 Request type (truncate request)]
* Frame 1: 4-byte little-endian unsigned table number

###### Truncate response

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x04 Request type (truncate response)]

-------------------------------

## Read-only requests


##### Read request

Read one or multiple keys (random-access) at once.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x10 Request type (read request)]
* Frame 1: 4-byte little-endian unsigned integer table number
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
* Frame 1: 4-byte little-endian unsigned table number
* Frame 2: Start key (inclusive). If this has zero length, the count starts at the first key
* Frame 3: End key (inclusive). If this has zero length, the count ends at the last key

If frame 2 and 3 are not present, the full key range (=entire table) is counted 
If only frame 2, but not frame 3 is present, frame 3 is treated as if it was zero-length.

##### Count response:

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x11 Response type (count response)][1-byte response code]
* Frame 1 (if response code indicates an error): NUL-terminated string describing the error
* Frame 1 (if response code indicates success): A 64-bit little-endian unsigned integer representing the number of values found in the given range (count)

Response codes:
* 0x00 Success (--> frame 1 contains count)
* 0x10 Error (--> frame 1 contains error description cstring)


##### Exists request

Check for existence of one or multiple keys in a table.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x12 Request type (exists request)]
* Frame 1: 4-byte little-endian unsigned integer table number
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

Read a range of keys at once ("read range request")

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x13 Request type (scan request)]
* Frame 1: 4-byte little-endian unsigned table number
* Frame 2: Start key (inclusive). If this has zero length, the count starts at the first key
* Frame 3: End key (inclusive). If this has zero length, the count ends at the last key

##### Scan response:

The scanned request returns the scan range as alternating key/value frames

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x13 Response type (scan response)][1-byte Response code]
* Frame 1-n (odd numbers): Next key
* Frame 1-n (even numbers): Next value (corresponds to key = previous frame)

Response codes:

* 0x00 Success (--> frame 1 contains first value)
* 0x10 Error (--> frame 1 contains error description cstring)
    
-------------------------------

## Write requests

### Write flags: These flags can optionally be supplied with write requests (bitwise-OR different flags to get the flag bytes

* 0x01 PARTSYNC: Hand request to database backend before acknowledging. Does not imply synchronous disk writes.
* 0x02 FULLSYNC: Force synchronous write to database

Requests that are not flagged PARTSYNC are called ASYNC.
The server shall acknowledge as soon as feasible after receiving the message.
ASYNC replies do not guarantee that the request has been checked for errors before acknowledging.
Using the PARTSYNC flag guarantees any subsequent write request yields the newly written value.

PARTSYNC may not be sent for non-REQ-REP sockets. Sending PARTSYNC over other sockets may lead to undefined behaviour.

##### Put request:

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x20 Request type (Put request)] [Optional: Write flags, defaults to 0x00]
* Frame 1-n (odd frame numbers): Key to write to. The next frame specifies the value to write
* Frame 2-n (even frame numbers): Value to write. The previous frame specifies the corresponding key.

None of the frames may be empty under any circumstances. Empty frames may lead to undefined behaviour.

##### Delete request:

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x21 Request type (Delete request)] [Optional: Write flags, defaults to 0x00]
* Frame 1-n: Key to delete (may contain arbitrary byte sequence)

None of the frames may be empty under any circumstances. Empty frames may lead to undefined behaviour.

##### Put/Delete response

The response format is identical for put and delete requests, besides the response type.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x20 or 0x21 Response type (Put/delete reponse)] [1 byte Response code]
* Frame 1 (Only present if response code indicates an error): NUL-terminated error string, UTF-8 encoded

Response codes (lower byte counts!):
* 0x00 Acknowledge (Only acknowledges that the request has been received)
* 0x01 Protocol error, found key frame without value frame (implies Frame 1 being existing)
* 0x02 Database error while processing request (implies Frame 1 being existing)
* 0x10 Protocol error, unspecified

If the 0x10 bit is set, the server signals that the write has been applied partially
and can't be rolled back automatically.
If the 0x10 bit is unset, the server signals none of the updates was applied because
of an automatic batch rollback.

Note: If not using PARTSYNC flag, the server will always send a non-error acknowledge code,
because processing has not started when the reply is sent.

-------------------------------

## Data processing requests

##### Forward range to socket request

Read a range of keys at once ("read range request")

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x40 Request type (Forward range to socket request)]
* Frame 1: 4-byte little-endian unsigned table number
* Frame 2: Start key (inclusive). If this has zero length, the count starts at the first key
* Frame 3: End key (inclusive). If this has zero length, the count ends at the last key
* Frame 4-n: PULL Endpoints to connect to. Put requests (spec: see above) will be sent to these sockets.
            Endpoints may not be inproc:// and shall be addressible to the server
* Frame n+1: Empty delimiter frame
* Frame (n+2)-n: SUB endpoints to send progress messages to.

No frame, except the delimiter frame, may be empty.

The server may spawn a new thread to serve the request.
Replying to the request does not indicate any kind of success.

##### Forward range to socket response

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0x40 Response type (Forward range to socket response)]
