#ifndef __TABLE_OPEN_SERVER_HPP
#define __TABLE_OPEN_SERVER_HPP


#include <thread>
#include "Tablespace.hpp"
#include "ConfigParser.hpp"

/**
 * This class starts a single thread in the background that shall receive an
 * inproc message.
 *
 * This class shall be instantiated exactly once or the behaviour will be undefined.
 */
class TableOpenServer : private AbstractFrameProcessor {
public:
    TableOpenServer(void* ctx,
                    ConfigParser& configParser,
                    Tablespace& tablespace);
    ~TableOpenServer();
    /**
     * Terminate the table open server.
     * Includes a full cleanup.
     */
    void terminate();
    void tableOpenWorkerThread();
private:
    std::thread* workerThread;
    ConfigParser& configParser;
    Tablespace& tablespace;
};

#endif //__TABLE_OPEN_SERVER_HPP