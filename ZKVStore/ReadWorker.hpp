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
private:
    void* workerPushSocket; //inproc PUSH socket to communicate over
    std::thread** threads;
    Tablespace& tablespace;
    size_t numThreads; //size of this->threads
    zctx_t* context;
};

#endif	/* READWORKER_HPP */

