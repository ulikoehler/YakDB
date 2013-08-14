#ifndef ASYNCJOBROUTER_HPP
#define ASYNCJOBROUTER_HPP
#include "AbstractFrameProcessor.hpp"
#include "SequentialIDGenerator.hpp"
#include "Tablespace.hpp"
#include <thread>
#include <map>
#include <atomic>
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
    ~AsyncJobRouterController();
    void start();
    /**
     * Terminate the async job router worker thread.
     * Includes a full cleanup.
     */
    void terminate();
    /**
     * A socket to the AsyncJobRouter for msg forwarding
     */
    void* routerSocket;
private:
    std::thread* childThread;
    Tablespace& tablespace;
    zctx_t* ctx;
};


struct ThreadStatisticsInfo {
    ThreadStatisticsInfo() : 
    transferredDataBytes(0),
    transferredRecords(0),
    jobExpungeTime(std::numeric_limits<int64_t>::max())
    {
    }
    JobType jobType;
    //We currently use non-atomics (because we assume 64-bit writes are atomic),
    // but this might have to change in the future
    uint64_t transferredDataBytes;
    uint64_t transferredRecords;
    /**
     * This is set to zclock_time() when the job is finished.
     * It is used to expunge the statistics some time after 
     * the job has finished.
     */
    int64_t jobExpungeTime;
    inline void addTransferredDataBytes(uint64_t bytes) {
        transferredDataBytes += bytes;
    }
    inline void addTransferredRecords(uint64_t bytes) {
        transferredDataBytes += bytes;
    }
    /**
     * Set the expunge time to zlock_time()
     */
    void setExpungeTime() {
        jobExpungeTime = zclock_time();
    }
};

/*
 * This provides variables written by the AP and read by the
 * router thread to manage the AP workflow.
 * This provides a lightweight alternative to using ZMQ sockets
 * for state communication
 * --------AP thread termination workflow----------
 * This workflow starts once the thread has sent out the last
 * non-empty data packet.
 * 1. AP sets the wantToTerminate entry to true
 *  -> Router shall not redirect any more client requests to the thread
 * 2. AP answers client requests until no request arrived for
 *    a predefined grace period (e.g. 1 sec).
 * 3. Thread exits and sets the exited flag and the 'request scrub job' flag
 * 4. Router scrub job cleans up stuff left behing
 */
class ThreadTerminationInfo {
public:
    ThreadTerminationInfo(std::atomic<unsigned int>* scrubJobRequestsArg) :
        wantToTerminate(),
        exited(),
        scrubJobRequests(scrubJobRequestsArg) {
    }
    void setWantToTerminate() {
        std::atomic_store(&wantToTerminate, true);
    }
    void setExited() {
        std::atomic_store(&exited, true);
    }
    bool wantsToTerminate() {
        return std::atomic_load(&wantToTerminate);
    }
    bool hasTerminated() {
        return std::atomic_load(&exited);
    }
    void requestScrubJob() {
        scrubJobRequests++;
    }
private:
    volatile std::atomic<bool> wantToTerminate;
    volatile std::atomic<bool> exited;
    std::atomic<unsigned int>* scrubJobRequests;
};


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
     * Terminate all jobs and cleanup
     */
    void terminateAll();
    /**
     * Terminates a single job and schedules a cleanup
     * @param id The id of the job to terminate
     */
    void terminate(uint64_t id);
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
    std::map<uint64_t, ThreadStatisticsInfo*> apStatisticsInfo; //APID --> TTI object
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
    /**
     * This is set to zclock_time() when a scrub job is excecuted.
     *
     * We need this because if the server is under constant load,
     * there might not be a n-second period without requests where
     * the scheduler would be called.
     * 
     * Another scenario is that the scrub job request mechanism
     * doesn't always work properly.
     * 
     * In the long term this might lead to a lot of unscrubbed jobs.
     * 
     * Therefore this variable is used to time a force-scrub job
     * every hour or so.
     * 
     * TODO implement
     */
    int64_t lastScrubJobTime;
};

#endif // ASYNCJOBROUTER_HPP
