#include "AsyncJobRouter.hpp"
#include "TableOpenHelper.hpp"
#include <leveldb/db.h>
#include <czmq.h>
#include <atomic>
#include "endpoints.hpp"
#include "protocol.hpp"
#include "zutil.hpp"

struct ThreadStatisticsInfo {
    ThreadStatisticsInfo() : 
    transferredDataBytes(),
    transferredRecords() {
    }
    std::atomic<uint64_t> transferredDataBytes;
    std::atomic<uint64_t> transferredRecords;
};

/*
 * This provides variables written by the AP and read by the
 * router thread to manage the AP workflow.
 * This provides a lightweight alternative to using ZMQ sockets
 * for state communication
 * --------AP thread termination workflow----------
 * This workflow starts once the thread has sent out the last
 * non-empty data packet.
 * 1. AP sets the wantToTerminate entry to true
 *  -> Router shall not redirect any more client requests to the thread
 * 2. AP answers client requests until no request arrived for
 *    a predefined grace period (e.g. 1 sec).
 * 3. Thread exits and sets the exited flag and the 'request scrub job' flag
 * 4. Router scrub job cleans up stuff left behing
 */
class ThreadTerminationInfo {
public:
    ThreadTerminationInfo(std::atomic<unsigned int>* scrubJobRequestsArg) :
        wantToTerminate(),
        exited(),
        scrubJobRequests(scrubJobRequestsArg) {
    }
    void setWantToTerminate() {
        std::atomic_store(&wantToTerminate, true);
    }
    void setExited() {
        std::atomic_store(&exited, true);
    }
    bool wantsToTerminate() {
        return std::atomic_load(&wantToTerminate);
    }
    bool hasTerminated() {
        return std::atomic_load(&exited);
    }
    void requestScrubJob() {
        std::atomic_fetch_add(scrubJobRequests, (unsigned int)1);
    }
private:
    volatile std::atomic<bool> wantToTerminate;
    volatile std::atomic<bool> exited;
    std::atomic<unsigned int>* scrubJobRequests;
};

/**
 * This function contains the main loop for the thread
 * that serves passive client-side data request for a specific range
 */
static void clientSidePassiveWorkerThreadFn(
    zctx_t* ctx,
    uint64_t apid,
    uint32_t databaseId,
    uint32_t chunksize,
    std::string rangeStart,
    std::string rangeEnd,
    Tablespace& tablespace,
    ThreadTerminationInfo* tti,
    ThreadStatisticsInfo* statisticsInfo) {
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
    //Initialize a message buffer to read one data chunk ahead to improve latency.
    zmq_msg_t* keyMsgBuffer = new zmq_msg_t[chunksize];
    zmq_msg_t* valueMsgBuffer = new zmq_msg_t[chunksize];
    uint32_t bufferValidSize = 0; //Number of valid elements in the buffer
    //Main receive/respond loop
    zmq_msg_t routingFrame, delimiterFrame, headerFrame;
    zmq_msg_init(&routingFrame);
    zmq_msg_init(&delimiterFrame);
    zmq_msg_init(&headerFrame);
    while(true) {
        //Step 1: Read util the buffer is full or end of range is reached
        for(bufferValidSize = 0;
            bufferValidSize < chunksize && it->Valid();
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
            for(unsigned int i = 0 ; i < bufferValidSize; i++) {
                zmq_msg_close(&keyMsgBuffer[i]);
                zmq_msg_close(&valueMsgBuffer[i]);
            }
            break;
        }
        zmq_msg_recv(&delimiterFrame, inSocket, 0);
        //Step 3: Send the reply to client
        if(zmq_msg_send(&routingFrame, outSocket, ZMQ_SNDMORE) == -1) {
            logMessageSendError("Routing frame", logger);
        }
        if(zmq_msg_send(&delimiterFrame, outSocket, ZMQ_SNDMORE) == -1) {
            logMessageSendError("Delimiter frame", logger);
        }
        if(unlikely(bufferValidSize == 0)) { //No data at all
            sendConstFrame(responseNoData, 4, outSocket, logger, "No data response header frame", 0);
            /*
             * If the last data has been sent, stop the thread
             * FIXME we need to ensure enqueued client requests get answered.
             * e.g. by waiting for a defined grace period.
             * However, the main async router thread needs to be informed
             * about the current thread being finished before waiting
             * for the defined grace period
             */
            break;
        } else if(bufferValidSize < chunksize) { //Partial data
            sendConstFrame(responsePartial, 4, outSocket, logger, "Partial response header frame", ZMQ_SNDMORE);
        } else {
            sendConstFrame(responseOK, 4, outSocket, logger, "Full data response header frame", ZMQ_SNDMORE);
        }
        //Send the data frames
        for(unsigned int i = 0 ; i < (bufferValidSize - 1) ; i++) {
            if(zmq_msg_send(&keyMsgBuffer[i], outSocket, ZMQ_SNDMORE) == -1) {
                logMessageSendError("Key frame (not last)", logger);
            }
            if(zmq_msg_send(&valueMsgBuffer[i], outSocket, ZMQ_SNDMORE) == -1) {
                logMessageSendError("Value frame (not last)", logger);
            }
        }
        //Send the last key/value
        if(zmq_msg_send(&keyMsgBuffer[bufferValidSize - 1], outSocket, ZMQ_SNDMORE) == -1) {
            logMessageSendError("Key frame (last)", logger);
        }
        if(zmq_msg_send(&valueMsgBuffer[bufferValidSize - 1], outSocket, 0) == -1) {
            logMessageSendError("Value frame (last)", logger);
        }
        //If this was a partial data, exit the loop
        if(bufferValidSize < chunksize) {
            break;
        }
    }
    //Cleanup
    delete it;
    delete[] keyMsgBuffer;
    delete[] valueMsgBuffer;
    db->ReleaseSnapshot(options.snapshot);             
    //See ThreadTerminationInfo docs for information about what is done where
    tti->setWantToTerminate();
    logger.trace("Reached AP end of life");
    //Note that settings the RCVTIMEOUT sockopt only affects subsequent connects,
    // so we have pollers here
    zmq_pollitem_t items[1];
    items[0].socket = inSocket;
    items[0].events = ZMQ_POLLIN;
    while(zmq_poll(items, 1, 100) != 0) {
        logger.trace("Stuff happens");
        //TODO error handling
        //Just send a NODATA header
        zmq_msg_recv(&routingFrame, inSocket, 0);
        zmq_msg_send(&routingFrame, outSocket, ZMQ_SNDMORE);
        zmq_msg_recv(&delimiterFrame, inSocket, 0);
        zmq_msg_send(&delimiterFrame, outSocket, ZMQ_SNDMORE);
        sendConstFrame(responseNoData, 4, outSocket, logger, "No data response header frame", 0);
    }
    //Final cleanup
    zsocket_destroy(ctx, inSocket);
    zsocket_destroy(ctx, outSocket);
    //Set exit flag and request scrub job
    tti->setExited();
    tti->requestScrubJob();
    logger.debug("AP exiting normally");
}

COLD AsyncJobRouterController::AsyncJobRouterController(zctx_t* ctxArg, Tablespace& tablespace)
    : routerSocket(zsocket_new(ctxArg, ZMQ_PUSH)), 
        childThread(nullptr),
        tablespace(tablespace),
        ctx(ctxArg) {
    //Create the PAIR socket to the job router
    zsocket_bind(routerSocket, asyncJobRouterAddr);
}

void COLD AsyncJobRouterController::start() {
    //Lambdas rock
    childThread = new std::thread([](zctx_t* ctx, Tablespace& tablespace) {
        AsyncJobRouter worker(ctx, tablespace);
        while(worker.processNextRequest()) {
            //Loop until stop msg is received (--> processNextRequest() returns false)
        }
    }, ctx, std::ref(tablespace));
}

void COLD AsyncJobRouterController::terminate() {
    if(routerSocket) { //Ignore call if already cleaned up
        //Send stop message
        sendEmptyFrameMessage(routerSocket);
        //Wait for thread to finish and cleanup
        childThread->join();
        delete childThread;
        //Cleanup EVERYTHING zmq-related immediately
        zsocket_destroy(ctx, routerSocket);
        routerSocket = nullptr;
    }
}


COLD AsyncJobRouterController::~AsyncJobRouterController() {
    terminate();
}

COLD AsyncJobRouter::AsyncJobRouter(zctx_t* ctxArg, Tablespace& tablespaceArg) :
AbstractFrameProcessor(ctxArg, ZMQ_PULL, ZMQ_PUSH, "Async job router"),
processSocketMap(),
processThreadMap(),
apTerminationInfo(),
scrubJobsRequested(),
apidGenerator("next-apid.txt"),
ctx(ctxArg),
tablespace(tablespaceArg) {
    //Print warnings if not using lockfree atomics
    std::atomic<bool> boolAtomic;
    std::atomic<unsigned int> uintAtomic;
    std::atomic<uint64_t> uint64Atomic;
    if(!atomic_is_lock_free(&boolAtomic)) {
        logger.warn("atomic<bool> is not lockfree, some operations might be slower than expected");
    }
    if(!atomic_is_lock_free(&uintAtomic)) {
        logger.warn("atomic<unsigned int> is not lockfree, some operations might be slower than expected");
    }
    if(!atomic_is_lock_free(&uint64Atomic)) {
        logger.warn("atomic<uint64_t> is not lockfree, some operations might be slower than expected");
    }
    //Connect the socket that is used by the send() member function
    if(zsocket_connect(processorInputSocket, asyncJobRouterAddr) == -1) {
        logger.critical("Failed to bind processor input socket: " + std::string(zmq_strerror(errno)));
    }
    //Connect the socket that is used to proxy requests to the external req/rep socket
    if(zsocket_connect(processorOutputSocket, externalRequestProxyEndpoint) == -1) {
        logger.critical("Failed to bind processor output socket: " + std::string(zmq_strerror(errno)));
    }
    logger.debug("Asynchronous job router starting up");
}

AsyncJobRouter::~AsyncJobRouter() {
    logger.debug("Async job router terminating");
    //Clean up everything
    terminateAll();
    doScrubJob();
    //Sockets are cleaned up in AbstractFrameProcessor
}

bool AsyncJobRouter::processNextRequest() {
    zmq_msg_t routingFrame, delimiterFrame, headerFrame;
    //Read routing info
    zmq_msg_init(&routingFrame);
    if(receiveLogError(&routingFrame, processorInputSocket, logger, "Routing frame") == -1) {
        return true;
    }
    //Empty frame means: STOP thread
    if (zmq_msg_size(&routingFrame) == 0) {
        zmq_msg_close(&routingFrame);
        return false;
    }
    //If it isn't empty, we expect to see the delimiter frame
    if (!expectNextFrame("Received nonempty routing frame, but no delimiter frame", false, "\x31\x01\xFF\xFF")) {
        zmq_msg_close(&routingFrame);
        return true;
    }
    zmq_msg_init(&delimiterFrame);
    if(receiveExpectMore(&delimiterFrame, processorInputSocket, logger, "delimiter frame") == -1) {
        return true;
    }
    //Receive the header frame
    zmq_msg_init(&headerFrame);
    if (unlikely(!receiveMsgHandleError(&headerFrame, "Receive header frame in read worker thread", "\x31\x01\xFF\xFF", true))) {
        return true;
    }
    assert(isHeaderFrame(&headerFrame));
    //Get the request type
    RequestType requestType = getRequestType(&headerFrame);
    //Process the rest of the framex
    if (requestType == ClientDataRequest) {
        logger.trace("Client data request");
        //Parse the APID frame
        uint64_t apid;
        if(!parseUint64Frame(apid, "APID frame", true, "\x31\01\x50\x01")) {
            return true;
        }
        /*
         * Directly reply "No data" if:
         *  1) There is no such job (any more?)
         *  2) The job has sent already the last non-empty data packet and
         *     reached its end-of-life, only expect
         * Else forward to corresponding worker
         */
        if(!haveProcess(apid) || doesAPWantToTerminate(apid)) {
            //Respond "No more data"
            if(zmq_msg_send(&routingFrame, processorOutputSocket, ZMQ_SNDMORE) == -1) {
                logMessageSendError("Routing frame (branch: No such APID)", logger);
            }
            if(zmq_msg_send(&delimiterFrame, processorOutputSocket, ZMQ_SNDMORE) == -1) {
                logMessageSendError("Delimiter frame (branch: No such APID)", logger);
            }
            sendConstFrame("\x31\x01\x50\x01", 4, processorOutputSocket, logger, "No data response header (branch: No such APID)");
        } else { //Forward to the worker
            void* outSock = processSocketMap[apid];
            if(zmq_msg_send(&routingFrame, outSock, ZMQ_SNDMORE) == -1) {
                logMessageSendError("Routing frame (on route to worker thread)", logger);
            }
            if(zmq_msg_send(&delimiterFrame, outSock, 0) == -1) {
                logMessageSendError("Delimiter frame (on route to worker thread)", logger);
            }
        }
        //Do some cleanup
        zmq_msg_close(&headerFrame);
    } else if (requestType == ForwardRangeToSocketRequest) {
        //TODO implement
        zmq_msg_send(&routingFrame, processorOutputSocket, ZMQ_SNDMORE);
        zmq_msg_send(&delimiterFrame, processorOutputSocket, ZMQ_SNDMORE);
        zmq_send(processorOutputSocket, "\x31\x01\x40\x01", 4, ZMQ_SNDMORE);
        std::string errstr = "Forward range to socket request not yet implemented";
        sendFrame(errstr, processorOutputSocket, logger, "Errmsg (= yet to be implemented)");
        logger.error(errstr);
    } else if (requestType == ServerSideTableSinkedMapInitializationRequest) {
        //TODO implement
        zmq_msg_send(&routingFrame, processorOutputSocket, ZMQ_SNDMORE);
        zmq_msg_send(&delimiterFrame, processorOutputSocket, ZMQ_SNDMORE);
        zmq_send(processorOutputSocket, "\x31\x01\x41\x01", 4, ZMQ_SNDMORE);
        std::string errstr = "SSTSMIR not yet implemented";
        sendFrame(errstr, processorOutputSocket, logger, "Errmsg (= yet to be implemented)");
        logger.error(errstr);
    } else if (requestType == ClientSidePassiveTableMapInitializationRequest) {
        zmq_msg_close(&headerFrame);
        //Parse all parameters
        uint32_t tableId;
        if(!parseUint32Frame(tableId, "APID frame", true, "\x31\01\x42\x01")) {
            return true;
        }
        uint32_t chunkSize;
        if(!parseUint32FrameOrAssumeDefault(chunkSize, 1000, "Block size frame", true, "\x31\01\x42\x01")) {
            return true;
        }
        std::string rangeStart;
        std::string rangeEnd;
        parseRangeFrames(rangeStart, rangeEnd, "CSPTMIR range", "\x31\x01\x42\x01", true);
        //Initialize it
        uint64_t apid = initializeJob();
        startClientSidePassiveJob(apid, tableId, chunkSize, rangeStart, rangeEnd);
        //Send the reply
        if(zmq_msg_send(&routingFrame, processorOutputSocket, ZMQ_SNDMORE) == -1) {
            zmq_msg_close(&routingFrame);
            logMessageSendError("Routing frame (CSPTMI Response)", logger);
        }
        if(zmq_msg_send(&delimiterFrame, processorOutputSocket, ZMQ_SNDMORE) == -1) {
            logMessageSendError("Routing frame (CSPTMI Response)", logger);
        }
        if(zmq_send(processorOutputSocket, "\x31\x01\x42\x00", 4, ZMQ_SNDMORE) == -1) {
            logMessageSendError("Header frame (CSPTMI Response)", logger);
        }
        //Send APID frame //TODO error check
        sendUint64Frame(apid, "CSPTMI Response APID");
        //Persist the latest APID to generate strictly ascending APIDs after
        // server restart
        apidGenerator.persist();
    }  else {
        std::string errstr = "Internal routing error: request type " + std::to_string((int) requestType) + " routed to read worker thread!";
        logger.error(errstr);
        sendConstFrame("\x31\x01\xFF", 3, processorOutputSocket, logger, "Internal routing error header frame", ZMQ_SNDMORE);
        sendFrame(errstr, processorOutputSocket, logger, "Internal routing error message frame");
    }
    //Scrub, if needed
    if(isThereAnyScrubJobRequest()) {
        doScrubJob();
    }
    return true;
}

uint64_t AsyncJobRouter::initializeJob() {
    uint64_t apid = apidGenerator.getNewId();
    void* sock = zsocket_new(ctx, ZMQ_PAIR);
    zsocket_bind(sock, "inproc://apid/%ld", apid);
    processSocketMap[apid] = sock;
    //Create the thread termination info object
    apTerminationInfo[apid] = new ThreadTerminationInfo(&scrubJobsRequested);
    return apid;
}

void AsyncJobRouter::startServerSideJob(uint64_t apid) {
    logger.error("ERRRRRRRROOOOOOORRRRRRRRRR: Not yet implemented!");
    cleanupJob(apid);
}

void AsyncJobRouter::startClientSidePassiveJob(uint64_t apid,
    uint32_t databaseId,
    uint32_t chunksize,
    const std::string& rangeStart,
    const std::string& rangeEnd) {
    processThreadMap[apid] = new std::thread(clientSidePassiveWorkerThreadFn, 
            ctx,
            apid,
            databaseId,
            chunksize,
            rangeStart,
            rangeEnd,
            std::ref(tablespace),
            apTerminationInfo[apid],
            nullptr
    );
}

void AsyncJobRouter::cleanupJob(uint64_t apid) {
    //We assume the process has already received an exit signal
    // or finished processing its data.
    void* socket = processSocketMap[apid];
    //Wait for thread to exit completely, then free the mem
    processThreadMap[apid]->join();
    delete processThreadMap[apid];
    //Destroy the sockets that were used to communicate with the thread
    zsocket_set_linger(socket, 0);
    zsocket_destroy(ctx, socket);
    //Remove the map entries
    processSocketMap.erase(apid);
    processThreadMap.erase(apid);
    apTerminationInfo.erase(apid);
}

void AsyncJobRouter::terminate(uint64_t apid) {
    void* socket = processThreadMap[apid];
    //Send stop signal to thread
    sendEmptyFrameMessage(socket);
    //Wait for thread to exit completely and cleanup
    cleanupJob(apid);
}

void COLD  AsyncJobRouter::terminateAll() {
    for(auto pair : processThreadMap) {
        uint64_t apid = pair.first;
        terminate(apid);
    }
    //Manually execute scrub job
    doScrubJob();
}

bool AsyncJobRouter::haveProcess(uint64_t apid) {
    return processSocketMap.count(apid) != 0;
}

bool AsyncJobRouter::doesAPWantToTerminate(uint64_t apid) {
    return apTerminationInfo[apid]->wantsToTerminate();
}

bool AsyncJobRouter::isThereAnyScrubJobRequest() {
    return std::atomic_load(&scrubJobsRequested);
}

void AsyncJobRouter::doScrubJob() {
     /**
      * -----------Performance note-----------
      * Scrub jobs currently have a runtime complexity of O(n)
      * where n is the number of currently stored APs.
      * This can probably be optimized by informing the async
      * router of the APID that has terminated, but this
      * would require a more complex implementation
      */
     //Substract one from the scrub job request counter
     std::atomic_fetch_sub(&scrubJobsRequested, (unsigned int)1);
     //Find jobs that have already terminated and scrub them
     typedef std::pair<uint64_t, ThreadTerminationInfo*> JobPair;
     for(const JobPair& jobPair: apTerminationInfo) {
         if(jobPair.second->hasTerminated()) {
             logger.trace("Scrubbing job with APID " + std::to_string(jobPair.first));
             cleanupJob(jobPair.first);
         }
     }
 }

void AsyncJobRouter::forwardToJob(uint64_t apid,
            zmq_msg_t* routingFrame,
            zmq_msg_t* delimiterFrame,
            zmq_msg_t* headerFrame) {
    void* outSock = processSocketMap[apid];
    zmq_msg_send(routingFrame, outSock, ZMQ_SNDMORE);
    zmq_msg_send(delimiterFrame, outSock, ZMQ_SNDMORE);
    //Only send MORE flag for the header frame if there are more  frames to follow
    zmq_msg_send(headerFrame, outSock,
        (socketHasMoreFrames(processorInputSocket) ? ZMQ_SNDMORE : 0));
    //Proxy the rest of the message (if any)
    proxyMultipartMessage(processorInputSocket, outSock);
}
