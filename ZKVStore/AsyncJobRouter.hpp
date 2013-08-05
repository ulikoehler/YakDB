#ifndef ASYNCJOBROUTER_HPP
#define ASYNCJOBROUTER_HPP
#include "AbstractFrameProcessor.hpp"
#include "SequentialIDGenerator.hpp"
#include "Tablespace.hpp"
#include <thread>
#include <unordered_map>
#include <czmq.h>
#include <cstdint>
#include <string>

/**
 * Utility class to spawn an Async job router in a separate thread
 */
class AsyncJobRouterController {
public:
    /**
     * Creates a new async job router controller.
     * Does not automatically start the thread
     */
    AsyncJobRouterController(zctx_t* ctx, Tablespace& tablespace);
    void start();
private:
    std::thread* childThread;
    Tablespace& tablespace;
    zctx_t* ctx;
};

/**
 * This router handles messages for data processing requests.
 * It spawns asynchronous processes and manages APIDs and AP lifecycles.
 */
class AsyncJobRouter : private AbstractFrameProcessor
{
public:
    AsyncJobRouter(zctx_t* ctx, Tablespace& tablespace);
    ~AsyncJobRouter();
    /**
     * Process the next request message that is received from the input socket.
     * @return false if stop message has been received, true else
     */
    bool processNextRequest();
private:
    /**
     * Create a new Job.
     * Assigns a new APID, initializes a socket and saves both in
     * Does not initialize or start the thread.
     * @return The assigned APID
     */
    uint64_t initializeJob();
    void startServerSideJob(uint64_t apid);
    void startClientSidePassiveJob(uint64_t apid);
    /**
     * Stop an asynchronous job an cleanup all resources related to it.
     */
    void cleanupJob(uint64_t apid);
    /**
     * Forwards the given frames plus any frames from the current message
     * to the thread with the given APID.
     * 
     * This function does not check
     * if there is any thread with the given APID.
     * You need to check that before calling it!
     */
    void forwardToJob(uint64_t apid,
                zmq_msg_t* routingFrame,
                zmq_msg_t* delimiterFrame,
                zmq_msg_t* headerFrame);
    /**
     * @return True if and only if we have a running process for the current APID.
     */
    bool haveProcess(uint64_t apid);
    zctx_t* ctx;
    std::unordered_map<uint64_t, void*> processSocketMap; //APID --> ZMQ socket
    std::unordered_map<uint64_t, std::thread*> processThreadMap; //APID --> ZMQ socket
    SequentialIDGenerator apidGenerator;
    Tablespace& tablespace;
};

#endif // ASYNCJOBROUTER_HPP
