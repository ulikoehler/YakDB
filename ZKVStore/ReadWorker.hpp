/* 
 * File:   UpdateWorker.hpp
 * Author: uli
 *
 * Created on 23. April 2013, 10:35
 */

#ifndef READWORKER_HPP
#define	READWORKER_HPP
#include <thread>
#include <czmq.h>
#include "Tablespace.hpp"
#include "AbstractFrameProcessor.hpp"

class ReadWorkerController {
public:
    ReadWorkerController(zctx_t* context, Tablespace& tablespace);
    ~ReadWorkerController();
    /**
     * Send a message to one of the read workers (load-balanced).
     * 
     * Asynchronous. Returns immediately.
     * @param msg
     */
    void send(zmsg_t** msg);
    /**
     * Start the worker threads
     */
    void start();
    void stopAll();
    void* workerPushSocket; //inproc PUSH socket to communicate to the workers
private:
    std::thread** threads;
    Tablespace& tablespace;
    size_t numThreads; //size of this->threads
    zctx_t* context;
};

class ReadWorker : private AbstractFrameProcessor {
public:
    ReadWorker(zctx_t* ctx, Tablespace& tablespace);
    ~ReadWorker();
    bool processNextRequest();
private:
    Tablespace& tablespace;
    TableOpenHelper tableOpenHelper;
    void handleExistsRequest(zmq_msg_t* headerFrame);
    void handleReadRequest(zmq_msg_t* headerFrame);
    void handleScanRequest(zmq_msg_t* headerFrame);
    void handleCountRequest(zmq_msg_t* headerFrame);
};

#endif	/* READWORKER_HPP */

