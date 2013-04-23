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

class ReadWorkerController {
public:
    ReadWorkerController(zctx_t* context, void* replyProxySocket);
    ~ReadWorkerController();
    /**
     * Send a message to one of the update workers (load-balanced).
     * 
     * Asynchronous. Returns immediately.
     * @param msg
     */
    void send(zmsg_t** msg);
private:
    void* workerPushSocket; //inproc PUSH socket to communicate over
    void* replySocket; //inproc PUSH socket that is directly proxied to the external REQ/REP socket
    std::thread** threads;
    size_t numThreads; //size of this->threads
    zctx_t* context;
};

#endif	/* READWORKER_HPP */

