#ifndef CLIENTSIDEPASSIVEJOB_HPP
#define CLIENTSIDEPASSIVEJOB_HPP
#include <zmq.h>
#include <leveldb/db.h>
#include "JobInfo.hpp"
#include "Tablespace.hpp"
#include "Logger.hpp"


/**
 * An instance of this class represents a running asynchrounous clientside
 * passive job
 */
class ClientSidePassiveJob {
public:
    ClientSidePassiveJob(void* ctxParam,
             uint64_t apid,
             uint32_t tableId,
             uint32_t chunksize,
             std::string& rangeStart,
             std::string& rangeEnd,
             uint64_t scanLimit,
             Tablespace& tablespace,
             ThreadTerminationInfo* tti,
             ThreadStatisticsInfo* statisticsInfo
            );
    void mainLoop();
    /**
     * Called when the last message has been received.
     * Implements the termination protocol, see
     * ThreadTerminationInfo documentation.
     */
    ~ClientSidePassiveJob();
private:
    void* inSocket;
    void* outSocket;
    zmq_msg_t* keyMsgBuffer;
    zmq_msg_t* valueMsgBuffer;
    leveldb::Iterator* it;
    std::string rangeEnd;
    uint64_t scanLimit;
    uint32_t chunksize;
    leveldb::DB* db;
    leveldb::Snapshot* snapshot;
    ThreadTerminationInfo* tti;
    ThreadStatisticsInfo* threadStatisticsInfo;
    Logger logger;
    void* ctx;
};

#endif //CLIENTSIDEPASSIVEJOB_HPP