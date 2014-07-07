/* 
 * File:   UpdateWorker.hpp
 * Author: uli
 *
 * Created on 23. April 2013, 10:35
 */

#ifndef UPDATEWORKER_HPP
#define	UPDATEWORKER_HPP
#include <thread>
#include <zmq.h>
#include "Tablespace.hpp"
#include "AbstractFrameProcessor.hpp"

class UpdateWorkerController {
public:
    UpdateWorkerController(void* context, Tablespace& tablespace, ConfigParser& configParser);
    ~UpdateWorkerController();
    /**
     * Start the worker threads
     */
    void start();
    void* workerPushSocket; //inproc PUSH socket to push work to the workers
    /**
     * Gracefully terminates all update worker threads by sending them stop messages.
     */
    void terminateAll();
private:
    std::thread** threads;
    Tablespace& tablespace;
    size_t numThreads; //size of this->threads
    void* context;
    Logger logger;
    ConfigParser& configParser;
};

class UpdateWorker : private AbstractFrameProcessor {
public:
    UpdateWorker(void* ctx, Tablespace& tablespace, ConfigParser& configParser);
    ~UpdateWorker();
    /**
     * The main function for the update worker thread.
     * Usually this shall be called in a loop. It blocks until a msg is received.
     * 
     * This basically receives the unmodified external message, but in any case
     * a 1-byte frame must be prepended.
     * If its one byte is 0, no address and delimiter frame shall be sent.
     * It its one byte is 1, an address and delimiter frame must follow.
     * If its one byte is 0xFF, the thread shall stop.
     * 
     * This function parses the header, calls the appropriate handler function
     * and sends the response for PARTSYNC requests
     * 
     * @return false if a stop message was received, true else
     */
    bool processNextMessage();
private:
    TableOpenHelper tableOpenHelper;
    Tablespace& tablespace;
    ConfigParser& cfg;
    void handlePutRequest(bool generateResponse);
    void handleDeleteRequest(bool generateResponse);
    void handleDeleteRangeRequest(bool generateResponse);
    void handleLimitedDeleteRangeRequest(bool generateResponse);
    void handleCompactRequest(bool generateResponse);
    void handleTableOpenRequest(bool generateResponse);
    void handleTableCloseRequest(bool generateResponse);
    void handleTableTruncateRequest(bool generateResponse);
};

#endif	/* UPDATEWORKER_HPP */

