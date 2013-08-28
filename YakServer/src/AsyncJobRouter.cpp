#include <leveldb/db.h>
#include <czmq.h>
#include <limits>
#include <atomic>
#include "AsyncJobRouter.hpp"
#include "TableOpenHelper.hpp"
#include "endpoints.hpp"
#include "protocol.hpp"
#include "ClientSidePassiveJob.hpp"
#include "zutil.hpp"

/**
 * This function contains the main loop for the thread
 * that serves passive client-side data request for a specific range
 */
static void clientSidePassiveWorkerThreadFn(zctx_t* ctxParam,
             uint64_t apid,
             uint32_t tableId,
             uint32_t chunksize,
             std::string rangeStart,
             std::string rangeEnd,
             uint64_t scanLimit,
             Tablespace& tablespace,
             ThreadTerminationInfo* tti,
             ThreadStatisticsInfo* statisticsInfo) {
    assert(tti);
    assert(statisticsInfo);
    ClientSidePassiveJob job(ctxParam, apid, tableId, chunksize, rangeStart, rangeEnd, scanLimit, tablespace, tti, statisticsInfo);
    job.mainLoop();
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
        uint64_t scanLimit;
        if(!parseUint64FrameOrAssumeDefault(scanLimit, UINT64_MAX, "Scan limit frame", true, "\x31\01\x42\x01")) {
            return true;
        }
        std::string rangeStart;
        std::string rangeEnd;
        parseRangeFrames(rangeStart, rangeEnd, "CSPTMIR range", "\x31\x01\x42\x01", true);
        //Initialize it
        uint64_t apid = initializeJob();
        startClientSidePassiveJob(apid, tableId, chunkSize, scanLimit, rangeStart, rangeEnd);
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
    apStatisticsInfo[apid] = new ThreadStatisticsInfo();
    return apid;
}

void AsyncJobRouter::startServerSideJob(uint64_t apid) {
    logger.error("ERRRRRRRROOOOOOORRRRRRRRRR: Not yet implemented!");
    cleanupJob(apid);
}

void AsyncJobRouter::startClientSidePassiveJob(uint64_t apid,
    uint32_t tableId,
    uint32_t chunksize,
    uint64_t scanLimit,
    const std::string& rangeStart,
    const std::string& rangeEnd) {

    //initializeJob() must be called before this
    apStatisticsInfo[apid]->jobType = JobType::CLIENTSIDE_PASSIVE;
    processThreadMap[apid] = new std::thread(clientSidePassiveWorkerThreadFn, 
            ctx,
            apid,
            tableId,
            chunksize,
            rangeStart,
            rangeEnd,
            scanLimit,
            std::ref(tablespace),
            apTerminationInfo[apid],
            apStatisticsInfo[apid]
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
    //Don't delete the statistics info immediately
    // -- clients might request info after the job has finished
}

void AsyncJobRouter::terminate(uint64_t apid) {
    void* socket = processSocketMap[apid];
    //Send stop signal to thread
    sendEmptyFrameMessage(socket);
    //Wait for thread to exit completely and cleanup
    cleanupJob(apid);
}

void COLD  AsyncJobRouter::terminateAll() {
    for(auto pair : processThreadMap) {
        uint64_t apid = pair.first;
        logger.trace("terminateAll(): Terminating job " + std::to_string(apid));
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
