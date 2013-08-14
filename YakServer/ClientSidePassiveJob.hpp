#ifndef CLIENTSIDEPASSIVEJOB_HPP
#define CLIENTSIDEPASSIVEJOB_HPP
#include <czmq.h>
#include <leveldb/db.h>

/**
 * An instance of this class represents a running asynchrounous clientside
 * passive job
 */
class ClientSidePassiveJob {
public:
    ClientSidePassiveJob(zctx_t* ctxParam,
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
    leveldb::Snapshot* snapshot;
    ThreadTerminationInfo* tti;
    ThreadStatisticsInfo* threadStatisticsInfo
    Logger logger;
    zctx_t* ctx;
    //Static response codes
    static const char* responseOK = "\x31\x01\x50\x00";
    static const char* responseNoData = "\x31\x01\x50\x01";
    static const char* responsePartial = "\x31\x01\x50\x02";
}

#endif //CLIENTSIDEPASSIVEJOB_HPP