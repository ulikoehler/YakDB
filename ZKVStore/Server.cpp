/*
 * Server.cpp
 *
 *  Created on: 23.04.2013
 *      Author: uli
 */


#include <czmq.h>
#include <string>
#include <iostream> 
#include "Server.hpp"
#include "zutil.hpp"
#include "protocol.hpp"
#include "endpoints.hpp"
#include "macros.hpp"
#include "../include/autoconfig.h"

using namespace std;

/**
 * This function is called by the reactor loop to process responses being sent from the worker threads.
 * 
 * The worker threads can't directly use the main ROUTER socket because sockets may
 * only be used by one thread.
 * 
 * By proxying the responses (non-PARTSYNC responses are sent directly by the main
 * thread before the request has been processed by the worker thread) the main ROUTER
 * socket is only be used by the main thread,
 * @param loop
 * @param poller
 * @param arg A pointer to the current KVServer instance
 * @return 
 */
int proxyWorkerThreadResponse(zloop_t *loop, zmq_pollitem_t *poller, void *arg) {
    KeyValueServer* server = (KeyValueServer*) arg;
    zmsg_t* msg = zmsg_recv(server->responseProxySocket);
    if (unlikely(!msg)) {
        return -1;
    }
    //We assume the message contains a valid envelope.
    //Just proxy it. Nothing special here.
    //zmq_proxy is a loop and therefore can't be used here
    int rc = zmsg_send(&msg, server->externalRepSocket);
    if (unlikely(rc == -1)) {
        debugZMQError("Proxy worker thread response", errno);
    }

    return 0;
}

/**
 * Sends a protocol error message over the given socket.
 * 
 * This function is marked COLD, which also affects branch predictions.
 * Branches that call this function are marked unlikely automatically.
 */
inline static void COLD sendProtocolError(zmq_msg_t* addrFrame, zmq_msg_t* delimiterFrame, void* sock, const std::string& errmsg) {
    zmq_msg_send(addrFrame, sock, ZMQ_SNDMORE);
    zmq_msg_send(delimiterFrame, sock, ZMQ_SNDMORE);
    sendConstFrame("\x31\x01\xFF", 3, sock, ZMQ_SNDMORE); //Send protocol error header
    sendFrame(errmsg, sock);
}

/**
 * Poll handler for the main request/response socket
 */
static int handleRequestResponse(zloop_t *loop, zmq_pollitem_t *poller, void *arg) {
    KeyValueServer* server = (KeyValueServer*) arg;
    //In the REQ/REP handler we only use one socket
    void* sock = server->externalRepSocket;
    //The message consists of four frames: Client addr, empty delimiter, msg type (1 byte) and data
    //Receive the routing info and the ZeroDB header frame
    zmq_msg_t addrFrame, delimiterFrame, headerFrame;
    zmq_msg_init(&addrFrame);
    if (unlikely(receiveExpectMore(&addrFrame, sock, server->logger))) {
        server->logger.error("Frame envelope could not be received correctly");
        //We can't even send back an error message, because the address can't be correct,
        // considering the envelope is missing
        zmq_msg_close(&addrFrame);
        return 0;
    }
    zmq_msg_init(&delimiterFrame);
    if (receiveExpectMore(&delimiterFrame, sock, server->logger)) {
        sendProtocolError(&addrFrame, &delimiterFrame, sock, "Received empty message (no ZeroDB header frame)");
        server->logger.warn("Client sent empty message (no header frame)");
        return 0;
    }
    zmq_msg_init(&headerFrame);
    receiveLogError(&headerFrame, sock, server->logger);
    if (unlikely(!isHeaderFrame(&headerFrame))) {
        sendProtocolError(&addrFrame, &delimiterFrame, sock,
                "Received malformed message, header format is not correct: " +
                describeMalformedHeaderFrame(&headerFrame));
        server->logger.warn("Client sent invalid header frame");
        zmq_msg_close(&headerFrame);
        return 0;
    }
    //Check the header -- send error message if invalid
    char* headerData = (char*) zmq_msg_data(&headerFrame);
    size_t headerSize = zmq_msg_size(&headerFrame);
    
    std::string errmsg; //The error message, if any, will be stored here
    RequestType requestType = (RequestType) (uint8_t) headerData[2];
    if (requestType == RequestType::ReadRequest
            || requestType == RequestType::CountRequest
            || requestType == RequestType::ExistsRequest
            || requestType == RequestType::ScanRequest
            || requestType == RequestType::LimitedScanRequest) {
        //Forward the message to the read worker controller, the response is sent asynchronously
        void* dstSocket = server->readWorkerController.workerPushSocket;
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
        //        cout << "XXXX" << endl;
        //        zframe_t* firstFrame = zmsg_first(msg);
        //        zframe_t* secondFrame = zmsg_next(msg);
        //        cout << "F1x " << zframe_size(firstFrame) << endl;
        //        if(true || zframe_size(firstFrame) != 5) {
        //            for (int i = 0; i < zframe_size(firstFrame); i++) {
        //                cout << "\t" << i << ":" << (int)zframe_data(firstFrame)[i] << endl;
        //            }
        //        }
        //        cout << "F2x " << zframe_size(secondFrame) << endl;
        //        cout << "YYYY" << endl;
        void* dstSocket = server->updateWorkerController.workerPushSocket;
        //Send the info frame (--> we have addr info)
        sendConstFrame("\x01", 1, dstSocket, server->logger, ZMQ_SNDMORE);
        //Send the routing information
        zmq_msg_send(&addrFrame, dstSocket, ZMQ_SNDMORE);
        zmq_msg_send(&delimiterFrame, dstSocket, ZMQ_SNDMORE);
        //Send header and data
        zmq_msg_send(&headerFrame, dstSocket, ZMQ_SNDMORE);
        proxyMultipartMessage(sock, dstSocket);
    } else if (requestType == RequestType::PutRequest
            || requestType == RequestType::DeleteRequest
            || requestType == RequestType::DeleteRangeRequest) {
        void* workerSocket = server->updateWorkerController.workerPushSocket;
        /**
         * Only for partsync messages the routing info (addr + delim frame)
         * is forwarded downstream.
         * For Async messages, the routing info is not forwarded downstream,
         * just as for PULL/SUB external connections
         */
        uint8_t writeFlags = getWriteFlags(&headerFrame);
        if (isPartsync(writeFlags)) {
            //Send the info frame (--> we have addr info)
            sendConstFrame("\x01", 1, workerSocket, server->logger, ZMQ_SNDMORE);
            zmq_msg_send(&addrFrame, workerSocket, ZMQ_SNDMORE);
            zmq_msg_send(&delimiterFrame, workerSocket, ZMQ_SNDMORE);
        } else {
            //Send the info frame (--> we don't have addr info)
            sendConstFrame("\x00", 1, workerSocket, server->logger, ZMQ_SNDMORE);
        }
        //Send the message to the update worker (--> processed async)
        zmq_msg_send(&headerFrame, workerSocket, ZMQ_SNDMORE);
        proxyMultipartMessage(sock, workerSocket);
        //Send acknowledge message unless PARTSYNC is set (in which case it is sent in the update worker thread)
        if (!isPartsync(writeFlags)) {
            zmq_msg_send(&addrFrame, sock, ZMQ_SNDMORE);
            zmq_msg_send(&delimiterFrame, sock, ZMQ_SNDMORE);
            //Response type shall be the same as request type
            char data[] = "\x31\x01\x20\x00";
            data[2] = requestType;
            sendFrame(data, 4, sock); //Send response code 0x00 (ack)
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
        sendFrame(serverInfoData, responseSize, sock, ZMQ_SNDMORE);
        //Send the server version info frame (declared in autoconfig.h)
        sendConstFrame(SERVER_VERSION, strlen(SERVER_VERSION), sock);
        //Dispose non-reused messages
        zmq_msg_close(&headerFrame);
    } else if(requestType & 0x40) { //Data processing request
        //TODO
    } else {
        server->logger.warn("Unknown message type " + std::to_string(requestType) + " from client\n");
        //Send a protocol error back to the client
        //TODO detailed error message frame (see protocol specs)
        sendProtocolError(&addrFrame, &delimiterFrame, sock, "Unknown message type");
        //Dispose non-reused frames
        zmq_msg_close(&headerFrame);
        //There might be more frames of the current msg that clog up the queue
        // and would lead to nasty bugs. Clear them, if any.
        recvAndIgnore(sock);
    }
    return 0;
}

static int handlePull(zloop_t *loop, zmq_pollitem_t *poller, void *arg) {
    KeyValueServer* server = (KeyValueServer*) arg;
    //Initialize a scoped lock
    zmsg_t *msg = zmsg_recv(server->externalRepSocket);
    if (unlikely(!msg)) {
        return -1;
    }
    //The message consists of four frames: Client addr, empty delimiter, msg type (1 byte) and data
    assert(zmsg_size(msg) >= 3); //return addr + empty delimiter + protocol header [+ data frames]
    //PULL requests do not contain routing information
    zframe_t* headerFrame = zmsg_first(msg);
    //The downstream processors require that if the message has been sent over a socket
    // that does not support replies (like PULL), the magic byte is set to 0x00 (they won't try to send a reply then)
    zframe_data(headerFrame)[0] = 0x00;
    //Check the header -- send error message if invalid
    string errorString;
    char* headerData = (char*) zframe_data(headerFrame);
    if (unlikely(!checkProtocolVersion(headerData, zframe_size(headerFrame), errorString))) {
        server->logger.error("Got illegal message (unmatching protocol version / magic bytes) from client");
        return 1;
    }
    RequestType requestType = (RequestType) (uint8_t) headerData[2];
    if (requestType == RequestType::PutRequest || requestType == RequestType::DeleteRequest) {
        //Send the message to the update worker (--> processed async)
        //This is simpler than the req/rep controller because no 
        // response flags need to be checked
        server->updateWorkerController.send(&msg);
    } else if (requestType == RequestType::OpenTableRequest
            || requestType == RequestType::CloseTableRequest
            || requestType == RequestType::CompactTableRequest
            || requestType == RequestType::TruncateTableRequest) {
        //Send the message to the update worker (--> processed async)
        server->updateWorkerController.send(&msg);
    } else if (unlikely(requestType == RequestType::ReadRequest || requestType == RequestType::CountRequest)) {
        //These request types demand an response and don't make sense over PUB/SUB sockets
        //TODO Use the logger
        cerr << "Error: Received read/count/server info request over PULL/SUB socket (you need to use REQ/REP sockets for read/count requests)" << endl;
    } else {
        server->logger.error("Unknown message type " + std::to_string(requestType) + " from client");
    }
    return 0;
}

KeyValueServer::KeyValueServer(bool dbCompressionEnabled) : ctx(zctx_new()),
tables(),
tableOpenServer(ctx, tables.getDatabases(), dbCompressionEnabled),
externalPullSocket(NULL),
externalSubSocket(NULL),
logServer(ctx),
updateWorkerController(ctx, tables),
readWorkerController(ctx, tables),
logger(ctx, "Request router") {
    const char* reqRepUrl = "tcp://*:7100";
    const char* writeSubscriptionUrl = "tcp://*:7101";
    const char* errorPubUrl = "tcp://*:7102";
    //Start the log server
    logServer.addLogSink(new StderrLogSink());
    logServer.startInNewThread();
    //Initialize the sockets that run on the main thread
    externalRepSocket = zsocket_new(ctx, ZMQ_ROUTER);
    zsocket_bind(externalRepSocket, reqRepUrl);
    responseProxySocket = zsocket_new(ctx, ZMQ_PULL);
    zsocket_bind(responseProxySocket, externalRequestProxyEndpoint);
    //Now start the update and read workers
    //(before starting the worker threads the response sockets need to be bound)
    updateWorkerController.start();
    readWorkerController.start();
    //Notify the user that the server has been started successfully
    logger.info("Server startup completed");
}

KeyValueServer::~KeyValueServer() {
    tables.cleanup();
    //Destroy the sockets
    if (externalRepSocket != nullptr) {
        zsocket_destroy(ctx, externalRepSocket);
    }
    if (externalSubSocket != nullptr) {
        zsocket_destroy(ctx, externalSubSocket);
    }
    if (externalPullSocket != nullptr) {
        zsocket_destroy(ctx, externalPullSocket);
    }
    zsocket_destroy(ctx, responseProxySocket);
    logger.info("Gracefully closed tables, exiting...");
    zctx_destroy(&ctx);
}

void KeyValueServer::start() {
    zloop_t *reactor = zloop_new();
    //The main thread listens to the external sockets and proxies responses from the worker thread
    zmq_pollitem_t externalPoller = {externalRepSocket, 0, ZMQ_POLLIN};
    if (zloop_poller(reactor, &externalPoller, handleRequestResponse, this)) {
        debugZMQError("Add external poller to reactor", errno);
    }
    zmq_pollitem_t responsePoller = {responseProxySocket, 0, ZMQ_POLLIN};
    if (zloop_poller(reactor, &responsePoller, proxyWorkerThreadResponse, this)) {
        debugZMQError("Add response poller to reactor", errno);
    }
    //Start the reactor loop
    zloop_start(reactor);
    //Cleanup (called when finished, e.g. by interrupt)
    zloop_destroy(&reactor);
}
