# ZeroDB MapReduce protocol specification

This document describes the communications

## Connection directions

In order to allow clients to be firewalled from the server,
the server binds a port th

## General behavioural description

A MapReduce job consists of:
    - A configuration that specifies a data flow between data sources and data sinks
    - One or multiple Data sources that, once initiated, wait for data chunk
        requests from workers and reply to them with key-value data chunks
    - One or multiple map workers that request and receive data chunks from any
        data source, process them and (optionally) produce arbitrary amounts of
        key-value output, therefore acting as a data source
    - Zero or more sorter units that receives data chunks from one or multiple
        data sources and maps each input key to a list of input values.
    - Zero or more reduce workers that read key->value-list chunks from
        sorter units and processes them.
    - A MapReduce controller that controls interaction of workers, data sources,
        sorters and data sinks. Additionally, it might spawn workers

Common data sources include:
    - CSV file
    - ZeroDB table ranges
        - Use consistent database snapshots
        - Informs clients if no data is available
        - Waits for client requests
    - Worker output
    - Sorter output

There is no l
The initialization requests are outlined in external-protocol.md.

The client shall send a request (details are outlined below) including the Job ID.
The server shall then send a reply back to the client 

### Protocol error response

For request/reply sockets, the server uses this response if it can't recognize
the protocol.

* Frame 0: [0x31 Magic Byte][0x01 Protocol Version][0xFF Response type (protocol error)]
* Frame 1 (Optional): NUL-terminated protocol error description