#ifndef JOBINFO_HPP
#define JOBINFO_HPP
#include <cstdint>
#include <limits>
#include <atomic>
#include "Logger.hpp"

enum class JobType : uint8_t {
    CLIENTSIDE_PASSIVE,
    CLIENTSIDE_ACTIVE,
    SERVERSIDE,
    TABLE_COPY
};

struct ThreadStatisticsInfo {
    inline ThreadStatisticsInfo() : 
        transferredDataBytes(0),
        transferredRecords(0),
        jobExpungeTime(std::numeric_limits<int64_t>::max()) {
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
     * Set the expunge time
     * TODO unfinished
     */
    void setExpungeTime() {
        jobExpungeTime = Logger::getCurrentLogTime();
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
    inline void setWantToTerminate() {
        std::atomic_store(&wantToTerminate, true);
    }
    inline void setExited() {
        std::atomic_store(&exited, true);
    }
    inline bool wantsToTerminate() {
        return std::atomic_load(&wantToTerminate);
    }
    inline bool hasTerminated() {
        return std::atomic_load(&exited);
    }
    inline void requestScrubJob() {
        scrubJobRequests++;
    }
private:
    volatile std::atomic<bool> wantToTerminate;
    volatile std::atomic<bool> exited;
    std::atomic<unsigned int>* scrubJobRequests;
};

#endif //JOBINFO_HPP