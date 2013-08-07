# ZeroDB

A fast and simple database for large-scale data analytics.

**Most parts of ZeroDB are unfinished, untested or not implemented yet!**



## Optimistic vs Non-Optimistic writes

ZeroDB supports three subtypes for any write operation:
    - PARTSYNC REQ/REP requests (=non-optimistic). Reply is sent when the DB backend has received the dataset.
    - ASYNC REQ/REP requests (=partially optimistic). Reply is sent once received by the server.
    - Non-REQ/REP (--> PULL/SUB) (=fully optimistic) requests. No reply is sent

When the reply for PARTSYNC requests is received, it is guaranteed the data will be written,
unless the server or the system suffers a severe crash (because the buffer is not flushed).
However, the write latency is high (when compared to optimistic writes) because the reply can't
be sent until the request has been routed to the backend database and processed there
In case of a crash, the client will know what request caused the error because
the reply-receive will time out, and can e.g. resend the request in load-balancing
ZeroDB setups. Additionally, PARTSYNC is the only write type that guarantees
subsequent (--> after reply has been received) read requests will return the written value.
Possible sources of errors:
    - Client request socket timeout is not set properly, timeout occurs never or too late
    

When receiving the reply for ASYNC requests has been received, you know the message
has entered the internal router in the server and will be processed when an update
is available to process the request. If the server crashes after push the request
into the internal pipeline, however, the request will not be processed, and the
client will not be notified at all, unless the server only suffers a crash
that will be logged and does not disrupt the logging system and the client
subscribed to CRITICAL log messages. In other words, don't expect to be informed.
Possible sources of errors:
    - Server enters an infinite loop while processing request (--> no error)
    - *(TO BE FIXED)* Current scheduling algorithm uses airport-type
        scheduling of requests to threads, so a huge request can block other requests
        in the queue of the blocked thread, even if other threads have no work at all.
    - Server suffers a full crash whilerequest is waiting for processing
        - Unlikely in non-high-load situations, because the request
            will be processed almost immediately
    - Writing the request fails, but client can't be informed (it will be logged by the server, however)
    - Death by Swap after the request has entered the pipeline (unlikely because ZeroDB is fast)
        - If this is a problem (it's easy to debug using htop or free) use load balancing ZeroDB
    

Fully optimistic writes are more like write-and-forget, in a negative manner.
You won't get an reply at all - this means you don't have to wait for one, but
it also means if your request fails, it won't get noticed if you don't do
complicated load balancing. Besides that, if your request is delayed anywhere, you
won't get notified at all, because request processing is not logged (it would clog up the logger).

##### Why would you use optimistic writes at all?

When developing an application using ZeroDB, we don't recommend starting
using optimistic write methods. You can easily change the write method later,
if your application does not demand a specific access pattern (i.e. only 
PARTSYNC guarantees subsequent read requests will return the written value).

There is the one simple reason why you'd prefer optimistic writes for certain applications:
*They are FAST.*
If using them, you guarantee your client's database writes block as short as possible.
This can improve your application performance significantly,
especially for write-heavy loads (did you consider to increase batch size?).

## How to improve your application's performance

#### General

- In rare cases, the server router thread might be overloaded by the
    amount of incoming messages. Use load-balancing, redundancy or find a better
    way to batch your messages.

#### Write-heavy workloads

- Is *ZeroDB* really your problem?
    Profile your application, maybe your algorithms causes performance issues
- What batch size do you use? Each write batch introduces request parsing,
    routing and error handling overhead plus LevelDB sorting overhead.
    - Especially for smaller keysizes, larger batches in the 10k-50k range yield
        the best performance
    - Peak loads with too-large batch sizes (especially for large values) might
        push the server out-of-memory and force the server OS to swap
    - *(TO BE FIXED)* Because of the current scheduling algorithm,
        extremely large batches could cause smaller batches to clog up
    - Not all language bindings provide an Auto-Batching frontend (currently only Python)
    - For ZeroDB, everything is a batch, even with only one request
- If you have loads of memory, open the table beforehand using the table open
    request and increase the write buffer size. Default is 64 MiB
    LevelDB keeps up to two write buffers into memory (per table, of course).
- Do you use the FULLSYNC flag? It bypasses the write buffer and makes writes really
    slow, especially for PARTSYNC requests.
- Does the server swap? Possible reasons include, but are not limited to:
    - Requests clog up the input queue or the worker thread queue because of
        - Large amounts of optimistic writing (if the server can't process all incoming requests fast enough)
        - Does the client or language binding write in an infinite loop, e.g.
            not clearing the batched data for Auto-batching frontends

#### Read-havy workloads

- LevelDB is not lock-free, so even if ZeroDB uses different threadsets for reads
    and writes, LevelDB might be locked by writes while trying to read.
- Reads can't be auto-batched, so try to batch them as much as possible.
    Try to design your application for batching upfront (ZeroDB clients use ZeroMQ
    anyway, so we recommend you use a ZeroMQ-based actor model for the whole application),
    changing it later can be quite time-consuming.
- Increase the LRU cache size to cache decompressed blocks. Default is 10 MiB.
- If your workload heavily depends on random-access reads (whether batched or not),
    consider disabling block-level compression, because if blocks need to be uncompressed
    for every read, reads are slow. Compression is enabled per default
- If compression needs to be enabled, try changing the table block size (default is 4 KiB uncompressed).
    Lower blocksizes generate less decompression overhead for completely random reads.
    Read the LevelDB documentation for further reference.
- For non-random reads, use scan requests or limited scan requests.
- Consider abusing the MapReduce initialization API to get a Chunkwise
    scan interface in a dedicated server thread.