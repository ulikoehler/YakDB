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
#include "Tablespace.hpp"

class UpdateWorkerController {
public:
    UpdateWorkerController(zctx_t* context, Tablespace& tablespace);
    ~UpdateWorkerController();
    /**
     * Send a message to one of the update workers (load-balanced).
     *
     * Asynchronous. Returns immediately.
     *
     * If no response is desired regardless of the message content,
     *  the first byte of the header message shall be set to 0x00 (instead of the magic byte 0x31)
     * @param msg
     */
    void send(zmsg_t** msg);
    /**
     * Start the worker threads
     */
    void start();
    void* workerPushSocket; //inproc PUSH socket to push work to the workers
private:
    std::thread** threads;
    Tablespace& tablespace;
    size_t numThreads; //size of this->threads
    zctx_t* context;
};

class UpdateWorker {
public:
    UpdateWorker(zctx_t* ctx, Tablespace& tablespace);
    ~UpdateWorker();
    /**
     * The main function for the update worker thread.
     * Usually this shall be called in a loop. It blocks until a msg is received.
     * 
     * This basically receives the unmodified external message, but in any case
     * a 1-byte frame must be prepended.
     * If its one byte is 0, no address and delimiter frame shall be sent.
     * It its one byte is not 0, an address and delimiter frame must follow.
     * If its one byte is 0xFF, the thread shall stop.
     * 
     * This function parses the header, calls the appropriate handler function
     * and sends the response for PARTSYNC requests
     */
    bool processNextMessage();
private:
    zctx_t* context;
    void* replyProxySocket;
    void* workPullSocket;
    TableOpenHelper tableOpenHelper;
    Logger logger;
    Tablespace& tablespace;
    /**
     * Parse a table ID, as little endian 32 bit unsigned integer in one frame.
     * Automatically receives the frame from replyProxySocket.
     * @param tableIdDst Where the table ID shall be placed
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @param errorResponseCode A 4-long response code to use if generating an error response
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool parseTableId(uint32_t& tableIdDst, bool generateResponse, const char* errorResponseCode);
    /**
     * Ensure the work pull socket has a next message part in the current message
     * @param errString A descriptive error string logged and sent to the client
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @param errorResponseCode A 4-long response code to use if generating an error response
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool expectNextFrame(const char* errString,
            bool generateResponse,
            const char* errorResponseCode);
    /**
     * Ensure the given LevelDB status code indicates success
     * @param errString A descriptive error string logged and sent to the client. status error string will be appended.
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @param errorResponseCode A 4-long response code to use if generating an error response
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool checkLevelDBStatus(const leveldb::Status& status,
            const char* errString,
            bool generateResponse,
            const char* errorResponseCode);
    void handleUpdateRequest(zmq_msg_t* headerFrame, bool generateResponse);
    void handleDeleteRequest(zmq_msg_t* headerFrame, bool generateResponse);
    void handleCompactRequest(zmq_msg_t* headerFrame, bool generateResponse);
};

#endif	/* UPDATEWORKER_HPP */

