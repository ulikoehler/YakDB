/* 
 * File:   UpdateWorker.hpp
 * Author: uli
 *
 * Created on 23. April 2013, 10:35
 */

#ifndef UPDATEWORKER_HPP
#define	UPDATEWORKER_HPP
#include <thread>
#include <czmq.h>
#include "Tablespace.hpp"
#include "AbstractFrameProcessor.hpp"

class UpdateWorkerController {
public:
    UpdateWorkerController(zctx_t* context, Tablespace& tablespace);
    ~UpdateWorkerController();
    /**
     * Send a message to one of the update workers (load-balanced).
     *
     * Asynchronous. Returns immediately.
     *
     * If no response is desired regardless of the message content,
     *  the first byte of the header message shall be set to 0x00 (instead of the magic byte 0x31)
     * @param msg
     */
    void send(zmsg_t** msg);
    /**
     * Start the worker threads
     */
    void start();
    void* workerPushSocket; //inproc PUSH socket to push work to the workers
private:
    std::thread** threads;
    Tablespace& tablespace;
    size_t numThreads; //size of this->threads
    zctx_t* context;
};

class UpdateWorker : private AbstractFrameProcessor {
public:
    UpdateWorker(zctx_t* ctx, Tablespace& tablespace);
    ~UpdateWorker();
    /**
     * The main function for the update worker thread.
     * Usually this shall be called in a loop. It blocks until a msg is received.
     * 
     * This basically receives the unmodified external message, but in any case
     * a 1-byte frame must be prepended.
     * If its one byte is 0, no address and delimiter frame shall be sent.
     * It its one byte is 1, an address and delimiter frame must follow.
     * If its one byte is 0xFF, the thread shall stop.
     * 
     * This function parses the header, calls the appropriate handler function
     * and sends the response for PARTSYNC requests
     * 
     * @return false if a stop message was received, true else
     */
    bool processNextMessage();
private:
    TableOpenHelper tableOpenHelper;
    Tablespace& tablespace;
    void handleUpdateRequest(zmq_msg_t* headerFrame, bool generateResponse);
    void handleDeleteRequest(zmq_msg_t* headerFrame, bool generateResponse);
    void handleDeleteRangeRequest(zmq_msg_t* headerFrame, bool generateResponse);
    void handleCompactRequest(zmq_msg_t* headerFrame, bool generateResponse);
    void handleTableOpenRequest(zmq_msg_t* headerFrame, bool generateResponse);
    void handleTableCloseRequest(zmq_msg_t* headerFrame, bool generateResponse);
    void handleTableTruncateRequest(zmq_msg_t* headerFrame, bool generateResponse);
};

#endif	/* UPDATEWORKER_HPP */

