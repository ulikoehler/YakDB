/*
 * Server.cpp
 *
 *  Created on: 23.04.2013
 *      Author: uli
 */


#include <zmq.h>
#include <string>
#include <iostream>
#include "LogSinks.hpp"
#include "Server.hpp"
#include "zutil.hpp"
#include "protocol.hpp"
#include "endpoints.hpp"
#include "macros.hpp"
#include "autoconfig.h"
#include "ThreadUtil.hpp"

using namespace std;


/**
 * Sends a protocol error message over the given socket.
 * 
 * This function is marked COLD, which also affects branch predictions.
 * Branches that call this function are marked unlikely automatically.
 */
static void COLD sendProtocolError(zmq_msg_t* addrFrame,
                zmq_msg_t* delimiterFrame,
                void* sock,
                const std::string& errmsg,
                Logger& logger) {
    zmq_msg_send(addrFrame, sock, ZMQ_SNDMORE);
    zmq_msg_send(delimiterFrame, sock, ZMQ_SNDMORE);
    sendConstFrame("\x31\x01\xFF", 3, sock, logger, "Protocol error header frame", ZMQ_SNDMORE); //Send protocol error header
    sendFrame(errmsg, sock, logger, "Protocol error message frame");
}

void HOT KeyValueServer::handleRequestResponse() {
    //In the REQ/REP handler we only use one socket
    void* sock = externalRepSocket;
    //The message consists of four frames: Client addr, empty delimiter, msg type (1 byte) and data
    //Receive the routing info and the ZeroDB header frame
    zmq_msg_t addrFrame, delimiterFrame, headerFrame;
    zmq_msg_init(&addrFrame);
    if (unlikely(receiveExpectMore(&addrFrame, sock, logger, "Routing addr") == -1)) {
        logger.error("Frame envelope could not be received correctly");
        //There might be more frames of the current msg that clog up the queue
        // and could lead to nasty bugs. Clear them, if any.
        recvAndIgnore(sock);
        //We can't even send back an error message, because the address can't be correct,
        // considering the envelope is missing
        zmq_msg_close(&addrFrame);
        return;
    }
    zmq_msg_init(&delimiterFrame);
    if (receiveExpectMore(&delimiterFrame, sock, logger, "Delimiter frame") == -1) {
        sendProtocolError(&addrFrame,
                &delimiterFrame, sock,
                "Received empty message (no ZeroDB header frame)", logger);
        logger.warn("Client sent empty message (no header frame)");
        return;
    }
    zmq_msg_init(&headerFrame);
    if(receiveLogError(&headerFrame, sock, logger, "Header frame")) {
        zmq_msg_close(&addrFrame);
        zmq_msg_close(&delimiterFrame);
        return;
    }
    //Check the header -- send error message if invalid
    if (unlikely(!isHeaderFrame(&headerFrame))) {
        sendProtocolError(&addrFrame, &delimiterFrame, sock,
                "Received malformed message, header format is not correct: " +
                describeMalformedHeaderFrame(&headerFrame),
                logger);
        logger.warn("Client sent invalid header frame: " + describeMalformedHeaderFrame(&headerFrame));
        zmq_msg_close(&headerFrame);
        return;
    }
    //Extract the request type from the header
    char* headerData = (char*) zmq_msg_data(&headerFrame);
    std::string errmsg; //The error message, if any, will be stored here
    RequestType requestType = (RequestType) (uint8_t) headerData[2];
    if (requestType == RequestType::ReadRequest
            || requestType == RequestType::CountRequest
            || requestType == RequestType::ExistsRequest
            || requestType == RequestType::ScanRequest) {
        //Forward the message to the read worker controller, the response is sent asynchronously
        void* dstSocket = readWorkerController.workerPushSocket;
        zmq_msg_send(&addrFrame, dstSocket, ZMQ_SNDMORE);
        zmq_msg_send(&delimiterFrame, dstSocket, ZMQ_SNDMORE);
        zmq_msg_send(&headerFrame, dstSocket, ZMQ_SNDMORE);
        proxyMultipartMessage(sock, dstSocket);
    } else if (requestType == RequestType::OpenTableRequest
            || requestType == RequestType::CloseTableRequest
            || requestType == RequestType::CompactTableRequest
            || requestType == RequestType::TruncateTableRequest) {
        /**
         * Table open/close/compact/truncate requests are redirected to the table opener
         *  in the update threads in order to avoid introducing overhead
         * by starting specific threads.
         * 
         * These requests should are not expected to arrive in high-load situations
         * but merely provide a convenience tool for interactive access.
         * 
         * In the worst case, some work piles up for a compacting thread, but
         * as compacting is not the kind of operation you want to do while
         * heavily writing to the database anyway, this is considered
         * a non-bug and improvement only
         * 
         * In the future, this might be avoided by different worker scheduling
         * algorithms (post office style)
         */
        void* dstSocket = updateWorkerController.workerPushSocket;
        //Send the info frame (--> we have addr info)
        sendConstFrame("\x01", 1, dstSocket, logger,
            "(Frame to update worker) Response envelope to follow", ZMQ_SNDMORE);
        //Send the routing information
        zmq_msg_send(&addrFrame, dstSocket, ZMQ_SNDMORE);
        zmq_msg_send(&delimiterFrame, dstSocket, ZMQ_SNDMORE);
        //Send header and data
        zmq_msg_send(&headerFrame, dstSocket, ZMQ_SNDMORE);
        if(unlikely(proxyMultipartMessage(sock, dstSocket) == -1)) {
            logMessageSendError("Some frame while proxying meta request", logger);
        }
    } else if (requestType == RequestType::PutRequest
            || requestType == RequestType::DeleteRequest
            || requestType == RequestType::DeleteRangeRequest) {
        void* workerSocket = updateWorkerController.workerPushSocket;
        /**
         * Only for partsync messages the routing info (addr + delim frame)
         * is forwarded downstream.
         * For Async messages, the routing info is not forwarded downstream,
         * just as for PULL/SUB external connections
         */
        uint8_t writeFlags = getWriteFlags(&headerFrame);
        if (isPartsync(writeFlags)) {
            //Send the info frame (--> we have addr info)
            sendConstFrame("\x01", 1, workerSocket, logger,
                "(Frame to update worker) Response envelope to follow", ZMQ_SNDMORE);
            zmq_msg_send(&addrFrame, workerSocket, ZMQ_SNDMORE);
            zmq_msg_send(&delimiterFrame, workerSocket, ZMQ_SNDMORE);
        } else {
            //Send the info frame (--> we don't have addr info)
            sendConstFrame("\x00", 1, workerSocket, logger,
                "(Frame to update worker) No response envelope", ZMQ_SNDMORE);
        }
        //Send the message to the update worker (--> processed async)
        zmq_msg_send(&headerFrame, workerSocket, ZMQ_SNDMORE);
        proxyMultipartMessage(sock, workerSocket);
        //Send acknowledge message unless PARTSYNC is set (in which case it is sent in the update worker thread)
        if (!isPartsync(writeFlags)) {
             //Send response code 0x00 (ack) (this is the ASYNC reply)
            zmq_msg_send(&addrFrame, sock, ZMQ_SNDMORE);
            zmq_msg_send(&delimiterFrame, sock, ZMQ_SNDMORE);
            //Response type shall be the same as request type
            char data[] = "\x31\x01\x20\x00";
            data[2] = requestType;
            sendFrame(data, 4, sock, logger, "Update request async response header");
        }
    } else if (requestType == RequestType::ServerInfoRequest) {
        //Server info requests are answered in the main thread
        const uint64_t serverFlags = SupportOnTheFlyTableOpen | SupportPARTSYNC | SupportFULLSYNC;
        const size_t responseSize = 3/*Metadata*/ + sizeof (uint64_t)/*Flags*/;
        char serverInfoData[responseSize]; //Allocate on stack
        serverInfoData[0] = magicByte;
        serverInfoData[1] = protocolVersion;
        serverInfoData[2] = ServerInfoResponse;
        memcpy(serverInfoData + 3, &serverFlags, sizeof (uint64_t));
        //Send the routing information
        zmq_msg_send(&addrFrame, sock, ZMQ_SNDMORE);
        zmq_msg_send(&delimiterFrame, sock, ZMQ_SNDMORE);
        //Send response header
        sendFrame(serverInfoData, responseSize, sock, logger,
            "Server info response header", ZMQ_SNDMORE);
        //Send the server version info frame (declared in autoconfig.h)
        sendConstFrame(SERVER_VERSION, strlen(SERVER_VERSION), sock,
            logger, "Server info response version info");
        //Dispose non-reused messages
        zmq_msg_close(&headerFrame);
    } else if(requestType & 0x40) { //Any data processing request
        void* workerSocket = asyncJobRouterController.routerSocket;
        if(zmq_msg_send(&addrFrame, workerSocket, ZMQ_SNDMORE) == -1) {
            logMessageSendError("Data processing request address frame", logger);
        }
        if(zmq_msg_send(&delimiterFrame, workerSocket, ZMQ_SNDMORE) == -1) {
            logMessageSendError("Data processing request delimiter frame", logger);
        }
        if(zmq_msg_send(&headerFrame, workerSocket, ZMQ_SNDMORE) == -1) {
            logMessageSendError("Data processing request header frame", logger);
        }
        if(proxyMultipartMessage(sock, workerSocket) == -1) {
            logMessageSendError("Some frame while proxying data processing request", logger);
        }
    } else if(requestType == RequestType::StopServerRequest) {
        logger.debug("Received server stop request from client");
        //Send response envelope
        zmq_msg_send(&addrFrame, sock, ZMQ_SNDMORE);
        zmq_msg_send(&delimiterFrame, sock, ZMQ_SNDMORE);
        //Send header (ACK)
        zmq_msg_close(&headerFrame);
        zmq_send(sock, "\x31\x01\x05\x00", 4, 0);
        //Stop the poll loop by simulating sigint
        yak_interrupted = true;
    } else {
        logger.warn("Unknown message type " + std::to_string(requestType) + " from client");
        //Send a protocol error back to the client
        //TODO detailed error message frame (see protocol specs)
        sendProtocolError(&addrFrame, &delimiterFrame, sock, "Unknown message type", logger);
        //Dispose non-reused frames
        zmq_msg_close(&headerFrame);
        //There might be more frames of the current msg that clog up the queue
        // and could lead to nasty bugs. Clear them, if any.
        recvAndIgnore(sock);
    }
}

void HOT KeyValueServer::handlePushPull() {
    void* sock = externalPullSocket;
    //Receive the header frame
    zmq_msg_t headerFrame;
    zmq_msg_init(&headerFrame);
    if(receiveLogError(&headerFrame, sock, logger, "Header frame")) {
        return;
    }
    //Check the header -- send error message if invalid
    if (unlikely(!isHeaderFrame(&headerFrame))) {
        logger.warn("Client sent invalid header frame: " + describeMalformedHeaderFrame(&headerFrame));
        //There might be more frames of the current msg that clog up the queue
        // and could lead to nasty bugs. Clear them, if any.
        recvAndIgnore(sock);
        zmq_msg_close(&headerFrame);
        return;
    }
    //Extract the request type from the header
    char* headerData = (char*) zmq_msg_data(&headerFrame);
    std::string errmsg; //The error message, if any, will be stored here
    RequestType requestType = (RequestType) (uint8_t) headerData[2];
    if (likely(requestType == RequestType::PutRequest
            || requestType == RequestType::DeleteRequest
            || requestType == RequestType::DeleteRangeRequest)) {
        //Send the message to the update worker (--> processed async)
        //This is simpler than the req/rep controller because no 
        // response flags need to be checked
        void* workerSocket = updateWorkerController.workerPushSocket;
        //We don't have reply addr info --> \x00
        sendConstFrame("\x00", 1, workerSocket, logger,
                "(Frame to update worker) No response envelope", ZMQ_SNDMORE);
        //Send header + rest of msg
        zmq_msg_send(&headerFrame, workerSocket, ZMQ_SNDMORE);
        proxyMultipartMessage(sock, workerSocket);
        //Proxy the message
    } else if (unlikely(requestType == RequestType::ReadRequest
                || requestType == RequestType::CountRequest
                || requestType == RequestType::ScanRequest)) {
        //These request types demand an response and don't make sense over PUB/SUB sockets
        //TODO Use the logger
        logger.error("Error: Received read-type request over PULL/SUB socket (you need to use REQ/REP sockets for read/count requests)");
    } else {
        //Dispose non-reused frames
        zmq_msg_close(&headerFrame);
        //There might be more frames of the current msg that clog up the queue
        // and could lead to nasty bugs. Clear them, if any.
        recvAndIgnore(sock);
    }
}

KeyValueServer::KeyValueServer(ConfigParser& configParserParam) :
ctx(zmq_ctx_new()),
logServer(ctx, LogLevel::Trace, true), //Autostart log server
tables(),
externalRepSocket(nullptr),
externalSubSocket(nullptr),
externalPullSocket(nullptr),
responseProxySocket(nullptr),
tableOpenServer(ctx, configParserParam, tables.getDatabases()),
updateWorkerController(ctx, tables, configParserParam),
readWorkerController(ctx, tables),
asyncJobRouterController(ctx, tables),
httpServer(ctx, configParserParam.getHTTPEndpoint(), configParserParam.getStaticFilePath()),
logger(ctx, "Request router"),
configParser(configParserParam)
 {
    //Start the log server and configure logsinks
    logServer.addLogSink(new StderrLogSink());
    if(!configParser.getLogFile().empty()) {
        logServer.addLogSink(new FileLogSink(configParser.getLogFile()));
    }
    BufferLogSink* logBuffer = new BufferLogSink(32);
    logServer.addLogSink(logBuffer);
    httpServer.setLogBuffer(logBuffer);
    /*
     * Initialize and bind the external sockets
     */
    //Print HWM, if not default
    if(configParser.getExternalHWM() != 250) {
        logger.trace("Using external HWM of " + std::to_string(configParser.getExternalHWM()));
    }
    if(configParser.getInternalHWM() != 250) {
        logger.trace("Using internal HWM of " + std::to_string(configParser.getInternalHWM()));
    }
    /**
     * REP / ROUTER
     */
    externalRepSocket = zmq_socket(ctx, ZMQ_ROUTER);
    zmq_set_hwm(externalRepSocket, configParser.getExternalHWM());
    if(!configParser.isIPv4Only()) {
        logger.trace("Using IPv6-capable sockets");
        zmq_set_ipv6(externalRepSocket, true);
    }
    for(const std::string& endpoint : configParser.getREPEndpoints()) {
        logger.debug("Binding REP socket to " + endpoint);
        zmq_bind(externalRepSocket, endpoint.c_str());
    }
    zmq_bind(externalRepSocket, mainRouterAddr); //Bind to inproc router
    /*
     * PULL
     */
    externalPullSocket = zmq_socket(ctx, ZMQ_PULL);
    zmq_set_hwm(externalPullSocket, configParser.getExternalHWM());
    if(!configParser.isIPv4Only()) {
        zmq_set_ipv6(externalPullSocket, true);
    }
    for(const std::string& endpoint : configParser.getPULLEndpoints()) {
        logger.debug("Binding PULL socket to " + endpoint);
        zmq_bind(externalPullSocket, endpoint.c_str());
    }
    //Response proxy socket to route asynchronous responses
    responseProxySocket = zmq_socket_new_bind_hwm(ctx, ZMQ_PULL, externalRequestProxyEndpoint, configParser.getExternalHWM());
    //Now start the update and read workers
    //(before starting the worker threads the response sockets need to be bound)
    updateWorkerController.start();
    readWorkerController.start();
    //Notify the user that the server has been started successfully
    logger.info("Server startup completed");
    //Start the async job router
    asyncJobRouterController.start();
}

KeyValueServer::~KeyValueServer() {
    //Destroy the sockets
    if (externalRepSocket != nullptr) {
        zmq_close(externalRepSocket);
    }
    if (externalSubSocket != nullptr) {
        zmq_close(externalSubSocket);
    }
    if (externalPullSocket != nullptr) {
        zmq_close(externalPullSocket);
    }
    zmq_close(responseProxySocket);
    //The log server has terminated, but we can still log directly to the backends
    logServer.log("Server", LogLevel::Info, "YakDB Server exiting...");
    //The context will be terminated before the member constructors are called
    logger.terminate();
    //Final cleanup
    zmq_ctx_destroy(&ctx);
}

void KeyValueServer::start() {
    //TODO sub poller
    zmq_pollitem_t items[3];
    items[0].socket = externalRepSocket;
    items[0].events = ZMQ_POLLIN;
    items[1].socket = externalPullSocket;
    items[1].events = ZMQ_POLLIN;
    items[2].socket = responseProxySocket;
    items[2].events = ZMQ_POLLIN;
    //Main server event loop. Returns after being interrupted
    //The stop server request simulates the interrupt
    while(true) {
        if(unlikely(zmq_poll(items, 3, -1) == -1)) {
            if(yak_interrupted) {
                break;
            }
            logOperationError("Polling HTTP server event loop", logger);
        }
        //Handle the event
        //NOTE: Multiple event flags can be set at once
        if(items[0].revents) {
            handleRequestResponse();
        }
        if(items[1].revents) {
            handlePushPull();
        }
        if(items[2].revents) { //Response proxy
            /**
            * This block is called by the
            * poll loop to process responses
            * being sent from the worker threads.
            * 
            * The worker threads can't directly use the main ROUTER socket because sockets may
            * only be used by one thread.
            * 
            * By proxying the responses (non-PARTSYNC responses are sent directly by the main
            * thread before the request has been processed by the worker thread) the main ROUTER
            * socket is only be used by the main thread.
            */
            int rc = zmq_proxy_single(responseProxySocket, externalRepSocket);
            if(unlikely(rc == -1)) {
                logger.error("Error while proxying response from worker thread: "
                             + std::string(zmq_strerror(errno)));
            }
        }
    }
    logger.trace("Main event loop interrupted, cleaning up...");
    /**
     * Cleanup procedure.
     * 
     * Cleanup as much as possible before terminating the ZMQ context
     * in order to be able to log all errors
     */
    //TODO Prevents shutdown - fix that!!
    updateWorkerController.terminateAll();
    readWorkerController.terminateAll();
    asyncJobRouterController.terminate();
    tableOpenServer.terminate();
    httpServer.terminate();
    tables.cleanup(); //Close & flush tables. This is NOT the table open server!
    logServer.terminate();
}
