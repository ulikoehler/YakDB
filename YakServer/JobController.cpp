/* 
 * File:   JobController.cpp
 * Author: uli
 * 
 * Created on 13. Juli 2013, 15:32
 */

#include "JobController.hpp"
#include <leveldb/db.h>

JobController::JobController(zctx_t* ctx, Tablespace& tablespace) :
AbstractFrameProcessor(ctx, ZMQ_PULL, ZMQ_PUSH, "Update worker"),
tableOpenHelper(ctx),
tablespace(tablespace) {
}

JobController::JobController(const JobController& orig) {
}

JobController::~JobController() {
}

void parse(void* processorInputSocket) {
    
}

static void doSth(void* outputSocket, leveldb::DB* db,
        const std::string rangeStartStr,
        const std::string rangeEndStr) {
    leveldb::Snapshot* snapshot = db->GetSnapshot();
    bool haveRangeStart = !(rangeStartStr.empty());
    bool haveRangeEnd = !(rangeEndStr.empty());
    //Convert the str to a slice, to compare the iterator slice in-place
    leveldb::Slice rangeEndSlice(rangeEndStr);
    //Do the compaction (takes LONG)
    //Create the response object
    leveldb::ReadOptions readOptions;
    leveldb::Status status;
    //Create the iterator
    leveldb::Iterator* it = db->NewIterator(readOptions);
    if (haveRangeStart) {
        it->Seek(rangeStartStr);
    } else {
        it->SeekToFirst();
    }
    //Iterate over all key-values in the range
    zmq_msg_t keyMsg, valueMsg;
    bool haveLastValueMsg = false; //Needed to send only last frame without SNDMORE
    for (; it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        //Check if we have to stop here
        if (haveRangeEnd && key.compare(rangeEndSlice) >= 0) {
            break;
        }
        leveldb::Slice value = it->value();
        //Send the previous value msg, if any
        if (haveLastValueMsg) {
            if (unlikely(!sendMsgHandleError(&valueMsg, ZMQ_SNDMORE, "ZMQ error while sending read reply (not last)", errorResponse))) {
                zmq_msg_close(&valueMsg);
                delete it;
                return;
            }
        }
        //Convert the slices into msgs and send them
        haveLastValueMsg = true;
        zmq_msg_init_size(&keyMsg, key.size());
        zmq_msg_init_size(&valueMsg, value.size());
        memcpy(zmq_msg_data(&keyMsg), key.data(), key.size());
        memcpy(zmq_msg_data(&valueMsg), value.data(), value.size());
        if (unlikely(!sendMsgHandleError(&keyMsg, ZMQ_SNDMORE, "ZMQ error while sending read reply (not last)", errorResponse))) {
            zmq_msg_close(&valueMsg);
            delete it;
            return;
        }
    }
    //Send the previous value msg, if any
    if (haveLastValueMsg) {
        if (unlikely(!sendMsgHandleError(&valueMsg, 0, "ZMQ error while sending last scan reply", errorResponse))) {
            delete it;
            return;
        }
    }
    //Cleanup
    db->ReleaseSnapshot(snapshot);
}

void JobController::waitForAll() {

}