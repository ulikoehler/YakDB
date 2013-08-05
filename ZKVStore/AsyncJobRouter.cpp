#include "AsyncJobRouter.hpp"
#include "TableOpenHelper.hpp"
#include <leveldb/db.h>
#include <czmq.h>
#include "endpoints.hpp"
#include "protocol.hpp"
#include "zutil.hpp"

/**
 * This function contains the main loop for the thread
 * that serves passive client-side data request for a specific range
 */
static void clientSidePassiveWorkerThreadFn(
    zctx_t* ctx,
    uint64_t apid,
    uint32_t databaseId,
    uint32_t blocksize,
    std::string rangeStart,
    std::string rangeEnd,
    Tablespace& tablespace) {
    //Static response code
    static const char* responseOK = "\x31\x01\x50\x00";
    static const char* responseNoData = "\x31\x01\x50\x01";
    static const char* responsePartial = "\x31\x01\x50\x02";
    //Create a logger for this worker
    Logger logger(ctx, "AP Worker " + std::to_string(apid));
    //Create utility stuff
    TableOpenHelper tableOpenHelper(ctx);
    //Create the socket to receive requests from
    void* inSocket = zsocket_new(ctx, ZMQ_PAIR);
    zsocket_connect(inSocket, "inproc://apid/%ld", apid);
    //Create the socket to send replies to the main router
    void* outSocket = zsocket_new(ctx, ZMQ_PUSH);
    zsocket_connect(outSocket, externalRequestProxyEndpoint);
    //Get the database object and create a snapshot
    leveldb::DB* db = tablespace.getTable(databaseId, tableOpenHelper);
    logger.debug("AP Worker startup successful");
    //Setup the snapshot and iterator
    leveldb::ReadOptions options;
    options.snapshot = db->GetSnapshot();
    leveldb::Iterator* it = db->NewIterator(options);
    if (rangeStart.empty()) {
        it->Seek(rangeStart);
    } else {
        it->SeekToFirst();
    }
    bool haveRangeEnd = !(rangeEnd.empty());
    leveldb::Slice rangeEndSlice(rangeEnd);
    //Initialize a message buffer to read one data block ahead to improve latency.
    zmq_msg_t* keyMsgBuffer = new zmq_msg_t[blocksize];
    zmq_msg_t* valueMsgBuffer = new zmq_msg_t[blocksize];
    uint32_t bufferValidSize = 0; //Number of valid elements in the buffer
    //Main receive/respond loop
    zmq_msg_t routingFrame, delimiterFrame, headerFrame;
    zmq_msg_init(&routingFrame);
    zmq_msg_init(&delimiterFrame);
    zmq_msg_init(&headerFrame);
    while(true) {
        //Step 1: Read util the buffer is full or end of range is reached
        for(bufferValidSize = 0;
            bufferValidSize < blocksize && it->Valid();
            it->Next()) {
            leveldb::Slice key = it->key();
            if (haveRangeEnd && key.compare(rangeEndSlice) >= 0) {
                break;
            }
            leveldb::Slice value = it->value();
            //Create the msgs from the slices (can't zero-copy here, slices are just references
            zmq_msg_init_size(&keyMsgBuffer[bufferValidSize], key.size());
            zmq_msg_init_size(&valueMsgBuffer[bufferValidSize], value.size());
            memcpy(zmq_msg_data(&keyMsgBuffer[bufferValidSize]), key.data(), key.size());
            memcpy(zmq_msg_data(&valueMsgBuffer[bufferValidSize]), value.data(), value.size());
            bufferValidSize++;
        }
        //Step 2: Wait for client request (APID frame has already been stripped by router)
        // (we assume all frames are available and in the correct format because the router shall check that)
        zmq_msg_recv(&routingFrame, inSocket, 0);
        if(zmq_msg_size(&routingFrame) == 0) {
            /*
             * This occurs when the server is shutting down and
             * the router sends a stop msg.
             * We don't NEED to be super-clean here, but it won't do any harm either.
             */
            for(int i = 0 ; i < bufferValidSize; i++) {
                zmq_msg_close(&keyMsgBuffer[i]);
                zmq_msg_close(&valueMsgBuffer[i]);
            }
            break;
        }
        zmq_msg_recv(&delimiterFrame, inSocket, 0); //Note we shall not receive the header frame.
        //Step 3: Send the reply to client
        zmq_msg_send(&routingFrame, outSocket, ZMQ_SNDMORE);
        zmq_msg_send(&delimiterFrame, outSocket, ZMQ_SNDMORE);
        if(unlikely(bufferValidSize == 0)) { //No data at all
            sendConstFrame(responseNoData, 4, outSocket, 0);
            /*
             * If the last data has been sent, stop the thread
             * FIXME we need to ensure enqueued client requests get answered.
             * e.g. by waiting for a defined grace period.
             * However, the main async router thread needs to be informed
             * about the current thread being finished before waiting
             * for the defined grace period
             */
            break;
        } else if(bufferValidSize < blocksize) { //Partial data
            sendConstFrame(responsePartial, 4, outSocket, ZMQ_SNDMORE);
        } else {
            sendConstFrame(responseOK, 4, outSocket, ZMQ_SNDMORE);
        }
        //Send the data frames
        for(int i = 0 ; i < (bufferValidSize - 1) ; i++) {
            zmq_msg_send(&keyMsgBuffer[i], outSocket, ZMQ_SNDMORE);
            zmq_msg_send(&valueMsgBuffer[i], outSocket, ZMQ_SNDMORE);
        }
        //Send the last key/value
        zmq_msg_send(&keyMsgBuffer[bufferValidSize - 1], outSocket, ZMQ_SNDMORE);
        zmq_msg_send(&valueMsgBuffer[bufferValidSize - 1], outSocket, 0);
        //If this was a partial data, exit the loop
        if(bufferValidSize < blocksize) {
            break;
        }
    }
    //Cleanup
    delete it;
    delete[] keyMsgBuffer;
    delete[] valueMsgBuffer;
    db->ReleaseSnapshot(options.snapshot);
    zsocket_destroy(ctx, inSocket);
    zsocket_destroy(ctx, outSocket);
    logger.debug("AP exiting normally");
}

AsyncJobRouterController::AsyncJobRouterController(zctx_t* ctx, Tablespace& tablespace)
    : childThread(nullptr),
        ctx(ctx),
        tablespace(tablespace) {
}

void AsyncJobRouterController::start() {
    //Lambdas rock
    childThread = new std::thread([](zctx_t* ctx, Tablespace& tablespace) {
        AsyncJobRouter worker(ctx, tablespace);
        while(worker.processNextRequest()) {
            //Loop until stop msg is received (--> processNextRequest() returns false)
        }
    }, ctx, std::ref(tablespace));
}

AsyncJobRouter::AsyncJobRouter(zctx_t* ctx, Tablespace& tablespaceArg) : 
AbstractFrameProcessor(ctx, ZMQ_PULL, ZMQ_PUSH, "Async job router"),
apidGenerator("next-apid.txt"),
processSocketMap(),
processThreadMap(),
tablespace(tablespaceArg)
{   //Connect the socket that is used to proxy requests to the external req/rep socket
    zsocket_connect(processorOutputSocket, externalRequestProxyEndpoint);
    //Connect the socket that is used by the send() member function
    zsocket_connect(processorInputSocket, asyncJobRouterAddr);
    logger.debug("Async job router starting");
}

AsyncJobRouter::~AsyncJobRouter()
{
}

bool AsyncJobRouter::processNextRequest() {
    zmq_msg_t routingFrame, delimiterFrame, headerFrame;
    //Read routing info
    zmq_msg_init(&routingFrame);
    receiveLogError(&routingFrame, processorInputSocket, logger);
    //Empty frame means: Stop thread
    if (zmq_msg_size(&routingFrame) == 0) {
        logger.trace("Async job router thread received stop signal");
        zmq_msg_close(&routingFrame);
        return false;
    }
    //If it isn't empty, we expect to see the delimiter frame
    if (!expectNextFrame("Received nonempty routing frame, but no delimiter frame", false, "\x31\x01\xFF\xFF")) {
        zmq_msg_close(&routingFrame);
        return true;
    }
    zmq_msg_init(&delimiterFrame);
    receiveExpectMore(&delimiterFrame, processorInputSocket, logger);
    //Receive the header frame
    zmq_msg_init(&headerFrame);
    if (unlikely(!receiveMsgHandleError(&headerFrame, "Receive header frame in read worker thread", "\x31\x01\xFF\xFF", true))) {
        return true;
    }
    assert(isHeaderFrame(&headerFrame));
    //Read the APID
    //Get the request type
    RequestType requestType = getRequestType(&headerFrame);
    //Process the rest of the frame
    if (requestType == ForwardRangeToSocketRequest) {
        //TODO impleent
        zmq_msg_send(&routingFrame, processorOutputSocket, ZMQ_SNDMORE);
        zmq_msg_send(&delimiterFrame, processorOutputSocket, ZMQ_SNDMORE);
        zmq_send(processorOutputSocket, "\x31\x01\x40\x01", 4, ZMQ_SNDMORE);
        std::string errstr = "Forward range to socket request not yet implemented";
        sendFrame(errstr, processorOutputSocket);
        logger.error(errstr);
    } else if (requestType == ServerSideTableSinkedMapInitializationRequest) {
        zmq_msg_send(&routingFrame, processorOutputSocket, ZMQ_SNDMORE);
        zmq_msg_send(&delimiterFrame, processorOutputSocket, ZMQ_SNDMORE);
        zmq_send(processorOutputSocket, "\x31\x01\x41\x01", 4, ZMQ_SNDMORE);
        std::string errstr = "SSTSMIR not yet implemented";
        sendFrame(errstr, processorOutputSocket);
        logger.error(errstr);
    } else if (requestType == ClientSidePassiveTableMapInitializationRequest) {
        //Initialize it
    } else if (requestType == ClientDataRequest) {
        //Parse the APID frame
        uint64_t apid;
        if(!parseUint64Frame(apid, "APID frame", true, "\x31\01\x50\x01")) {
            return true;
        }
    } else {
        std::string errstr = "Internal routing error: request type " + std::to_string((int) requestType) + " routed to read worker thread!";
        logger.error(errstr);
        sendConstFrame("\x31\x01\xFF", 3, processorOutputSocket, ZMQ_SNDMORE);
        sendFrame(errstr, processorOutputSocket);
    }
    /**
     * In some cases (especially errors) the msg part input queue is clogged
     * up with frames that have not yet been processed.
     * Clear them
     */
    disposeRemainingMsgParts();
    return true;
}

uint64_t AsyncJobRouter::initializeJob() {
    uint64_t apid = apidGenerator.getNewId();
    void* sock = zsocket_new(ctx, ZMQ_PAIR);
    zsocket_bind(sock, "inproc://apid/%ld", apid);
    processSocketMap[apid] = sock;
    return apid;
}

void AsyncJobRouter::startServerSideJob(uint64_t apid) {
    logger.error("ERRRRRRRROOOOOOORRRRRRRRRR: Not yet implemented!");
    cleanupAPID(apid);
}

void AsyncJobRouter::startClientSidePassiveJob(uint64_t apid) {
    
}

void AsyncJobRouter::cleanupAPID(uint64_t apid) {
    void* socket = processSocketMap[apid];
    std::thread* thread = processThreadMap[apid];
    processSocketMap.erase(apid);
    processThreadMap.erase(apid);
    //Send an empty frame (--> stop request) to the thread and wait for it to exits
    zstr_send(socket, "");
    thread->join();
    //Cleanup the thread
    delete thread;
    //Destroy the socket
    zsocket_set_linger(socket, 0);
    zsocket_destroy(ctx, socket);
}

bool AsyncJobRouter::haveProcess(uint64_t apid) {
    return processSocketMap.count(apid) != 0;
}

void AsyncJobRouter::forwardToJob(uint64_t apid,
            zmq_msg_t* routingFrame,
            zmq_msg_t* delimiterFrame,
            zmq_msg_t* headerFrame) {
    void* outSock = processSocketMap[apid];
    zmq_msg_send(routingFrame, outSock, ZMQ_SNDMORE);
    zmq_msg_send(delimiterFrame, outSock);
    //Only send MORE flag for the header frame if there are more  frames to follow
    zmq_msg_send(headerFrame, outSock,
        (socketHasMoreFrames(processorInputSocket) ? ZMQ_SNDMORE : 0));
    //Proxy the rest of the message (if any)
    proxyMultipartMessage(processorInputSocket, outSock);
}