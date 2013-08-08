#ifndef ASYNCJOBROUTER_HPP
#define ASYNCJOBROUTER_HPP
#include "AbstractFrameProcessor.hpp"
#include "SequentialIDGenerator.hpp"
#include "Tablespace.hpp"
#include <thread>
#include <map>
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
    /**
     * A socket to the AsyncJobRouter for msg forwarding
     */
    void* routerSocket;
private:
    std::thread* childThread;
    Tablespace& tablespace;
    zctx_t* ctx;
};

class ThreadTerminationInfo;

/**
 * This router handles messages for data processing requests.
 * It spawns asynchronous processes and manages APIDs and AP lifecycles.
 * 
 * ----------- Map type ------------
 * std::map seems to be a better choice at the moment based on benchmarks
 * like this one here:
 * http://kariddi.blogspot.de/2012/07/c11-unorderedmap-vs-map.html
 * We usually expect the map to be quite small, so the (AVL?RB?) tree overhead
 * is minimal when compared to hashing each value.
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
    void startClientSidePassiveJob(uint64_t apid,
        uint32_t databaseId,
        uint32_t blocksize,
        const std::string& rangeStart,
        const std::string& rangeEnd);
    /**
     * Cleanup an asynchronous job an release all resources related to it.
     * Must only be used on jobs that have already signalled that they have exited
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
    /**
     * @return true if and only if the thread with the given APID
     *  has finished sending all non-empty datablocks and is currently in
     *  the termination grace period.
     */
    bool doesAPWantToTerminate(uint64_t apid);
    /**
     * Execute a scrub job to release resources acquired by
     * APs that already have ended
     */
    void doScrubJob();
    /**
     * @return true if and only if the value of this->scrubJobsRequested is > 0
     */
    bool isThereAnyScrubJobRequest();
    std::map<uint64_t, void*> processSocketMap; //APID --> ZMQ socket
    std::map<uint64_t, std::thread*> processThreadMap; //APID --> ZMQ socket
    std::map<uint64_t, ThreadTerminationInfo*> apTerminationInfo; //APID --> TTI object
    /**
     * This variable is incremented by APs when they exit
     * to request a scrub job.
     * If no messages arrive at the async router for a predefined
     * grace period and this variable is > 0,
     * it starts a scrub job that releases resources
     * acquired for terminated AP lifecycle management.
     */
    std::atomic<unsigned int> scrubJobsRequested;
    SequentialIDGenerator apidGenerator;
    zctx_t* ctx;
    Tablespace& tablespace;
};

#endif // ASYNCJOBROUTER_HPP
