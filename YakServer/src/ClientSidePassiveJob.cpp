#include "ClientSidePassiveJob.hpp"
#include "endpoints.hpp"
#include "zutil.hpp"

//Static response codes
static const char* responseOK = "\x31\x01\x50\x00";
static const char* responseNoData = "\x31\x01\x50\x01";
static const char* responsePartial = "\x31\x01\x50\x02";

ClientSidePassiveJob::ClientSidePassiveJob(zctx_t* ctxParam,
             uint64_t apid,
             uint32_t tableId,
             uint32_t chunksizeParam,
             std::string& rangeStart,
             std::string& rangeEndParam,
             uint64_t scanLimitParam,
             Tablespace& tablespace,
             ThreadTerminationInfo* tti,
             ThreadStatisticsInfo* statisticsInfo) :
                    inSocket(zsocket_new(ctxParam, ZMQ_PAIR)),
                    outSocket(zsocket_new_connect(ctxParam, ZMQ_PUSH, externalRequestProxyEndpoint)),
                    keyMsgBuffer(new zmq_msg_t[chunksizeParam]),
                    valueMsgBuffer(new zmq_msg_t[chunksizeParam]),
                    rangeEnd(rangeEndParam),
                    scanLimit(scanLimitParam),
                    chunksize(chunksizeParam),
                    db(tablespace.getTable(tableId, ctxParam)),
                    tti(tti),
                    logger(ctxParam, "AP worker " + std::to_string(apid)),
                    ctx(ctxParam) {
        //Create the socket to receive requests from
        zsocket_connect(inSocket, "inproc://apid/%ld", apid);
        //Setup the snapshot and iterator
        leveldb::ReadOptions options;
        this->snapshot = (leveldb::Snapshot*) db->GetSnapshot();
        options.snapshot = this->snapshot;
        it = db->NewIterator(options);
        //Seek the iterator
        if (rangeStart.empty()) {
            it->Seek(rangeStart);
        } else {
            it->SeekToFirst();
        }
        logger.debug("AP Worker successfully started up");
    }

void ClientSidePassiveJob::mainLoop() {
    bool haveRangeEnd = !(rangeEnd.empty());
    leveldb::Slice rangeEndSlice(rangeEnd);
    //Initialize a message buffer to read one data chunk ahead to improve latency and speed.
    //This slightly increases memory usage, but we can interleave DB reads and socket writes
    uint32_t bufferValidSize = 0; //Number of valid elements in the buffer
    uint64_t bufferDataSize = 0;
    //Init the receive frames only once.
    zmq_msg_t routingFrame, delimiterFrame, headerFrame;
    zmq_msg_init(&routingFrame);
    zmq_msg_init(&delimiterFrame);
    zmq_msg_init(&headerFrame);
    while(true) {
        bufferDataSize = 0;
        //Step 1: Read until the buffer is full or end of range is reached
        for(bufferValidSize = 0;
            bufferValidSize < chunksize && it->Valid();
            it->Next()) {
            //Check scan limit reached condition
            if (scanLimit <= 0) {
                break;
            }
            scanLimit--;
            //Check end key reached condition
            leveldb::Slice key = it->key();
            if (haveRangeEnd && key.compare(rangeEndSlice) >= 0) {
                break;
            }
            leveldb::Slice value = it->value();
            //Create the msgs from the slices (can't zero-copy here, slices are just references!)
            size_t keySize = key.size();
            size_t valueSize = value.size();
            zmq_msg_init_size(&keyMsgBuffer[bufferValidSize], keySize);
            zmq_msg_init_size(&valueMsgBuffer[bufferValidSize], valueSize);
            memcpy(zmq_msg_data(&keyMsgBuffer[bufferValidSize]), key.data(), keySize);
            memcpy(zmq_msg_data(&valueMsgBuffer[bufferValidSize]), value.data(), valueSize);
            bufferValidSize++;
            bufferDataSize += keySize + valueSize;
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
            logger.trace("Job received stop message, exiting");
            for(unsigned int i = 0 ; i < bufferValidSize; i++) {
                zmq_msg_close(&keyMsgBuffer[i]);
                zmq_msg_close(&valueMsgBuffer[i]);
            }
            break;
        }
        zmq_msg_recv(&delimiterFrame, inSocket, 0);
        threadStatisticsInfo->transferredRecords++;
        threadStatisticsInfo->transferredDataBytes += bufferDataSize;
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
}

ClientSidePassiveJob::~ClientSidePassiveJob() {
    //Free DB-related memory
    delete it;
    db->ReleaseSnapshot(snapshot);
    //Free buffer memory
    delete[] keyMsgBuffer;
    delete[] valueMsgBuffer;
    //See ThreadTerminationInfo docs for information about what is done here
    tti->setWantToTerminate();
    logger.trace("Reached AP end of life");
    //Skip the grace period if the server was SIGINTED
    if(!zctx_interrupted) {
        //Note that settings the RCVTIMEOUT sockopt only affects subsequent connects,
        // so we have pollers here
        zmq_pollitem_t items[1];
        items[0].socket = inSocket;
        items[0].events = ZMQ_POLLIN;
        zmq_msg_t routingFrame, delimiterFrame, headerFrame;
        zmq_msg_init(&routingFrame);
        zmq_msg_init(&delimiterFrame);
        zmq_msg_init(&headerFrame);
        const int gracePeriod = 1000; //ms
        while(zmq_poll(items, 1, gracePeriod) != 0) {
            //If this branch is executed, a request arrived
            // after we told the AP router thread we want to terminate.
            //TODO error handling
            //Just send a NODATA header
            zmq_msg_recv(&routingFrame, inSocket, 0);
            zmq_msg_send(&routingFrame, outSocket, ZMQ_SNDMORE);
            zmq_msg_recv(&delimiterFrame, inSocket, 0);
            zmq_msg_send(&delimiterFrame, outSocket, ZMQ_SNDMORE);
            sendConstFrame(responseNoData, 4, outSocket, logger, "No data response header frame", 0);
        }
    }
    //Cleanup sockets
    zsocket_destroy(ctx, inSocket);
    zsocket_destroy(ctx, outSocket);
    //Set exit flag and request scrub job
    tti->setExited();
    tti->requestScrubJob();
    logger.debug("AP exiting normally");
}