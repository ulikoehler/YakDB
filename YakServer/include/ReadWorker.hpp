/* 
 * File:   UpdateWorker.hpp
 * Author: uli
 *
 * Created on 23. April 2013, 10:35
 */

#ifndef READWORKER_HPP
#define	READWORKER_HPP
#include <thread>
#include <zmq.h>
#include "BoyerMoore.hpp"
#include "Tablespace.hpp"
#include "AbstractFrameProcessor.hpp"

class ReadWorkerController {
public:
    ReadWorkerController(void* context, Tablespace& tablespace);
    ~ReadWorkerController();
    /**
     * Start the worker threads
     */
    void start();
    void* workerPushSocket; //inproc PUSH socket to communicate to the workers
    /**
     * Gracefully terminates all update worker threads by sending them stop messages.
     */
    void terminateAll();
private:
    std::thread** threads;
    Tablespace& tablespace;
    size_t numThreads; //size of this->threads
    void* context;
};

/**
 * A single read worker instance.
 * Represents a thread that receives read work msgs.
 * 
 * This thread assumes an envelope always prefixes a frame.
 */
class ReadWorker : private AbstractFrameProcessor {
public:
    ReadWorker(void* ctx, Tablespace& tablespace);
    ~ReadWorker();
    bool processNextRequest();
private:
    Tablespace& tablespace;
    TableOpenHelper tableOpenHelper;
    void handleExistsRequest(zmq_msg_t* headerFrame);
    void handleReadRequest(zmq_msg_t* headerFrame);
    void handleScanRequest(zmq_msg_t* headerFrame);
    void handleLimitedScanRequest(zmq_msg_t* headerFrame);
    void handleCountRequest(zmq_msg_t* headerFrame);
};

#endif	/* READWORKER_HPP */

