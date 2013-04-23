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

class UpdateWorkerController {
public:
    UpdateWorkerController(zctx_t* context, KVServer* serverInfo);
    ~UpdateWorkerController();
    /**
     * Send a message to one of the update workers (load-balanced).
     * 
     * Asynchronous. Returns immediately.
     * @param msg
     */
    void send(zmsg_t* msg);
private:
    void* workerPushSocket; //inproc PUSH socket to communicate over
    std::thread** threads;
    size_t numThreads; //size of this->threads
    zctx_t* context;
};

#endif	/* UPDATEWORKER_HPP */

