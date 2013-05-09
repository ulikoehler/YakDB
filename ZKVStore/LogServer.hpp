/* 
 * File:   LogServer.hpp
 * Author: uli
 *
 * Created on 8. Mai 2013, 19:58
 */

#ifndef LOGSERVER_HPP
#define	LOGSERVER_HPP
#include "Logger.hpp"
#include <thread>

/**
 * A LogSink instance represents the final destination of a log message,
 * e.g. a rotating file log sink or a email log sink
 */
class LogSink {
public:
    virtual void log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage) = 0;
};

class StderrLogSink : public LogSink {
public:
    StderrLogSink();
    void log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage);
};

/**
 * A log server that proxies inproc pattern to external PUB/SUB
 * 
 * A PULL-like socket is bound to the endpoint supplied at construction time.
 * 
 * The proxy is started in a separate thread that can be stopped
 * using a specific message 
 * 
 * Protocol specification (messages from logger to server):
 * Frame 0: Header: Magic byte + protocol version + request type - \x55\x01\x00
 * Frame 1: Log level (1 byte, as defined in the LogLevel enum)
 * Frame 2: uint64_t timestamp, localized = 1000 * epoch seconds + milliseconds
 * Frame 3: UTF8-encoded sender name
 * Frame 4: UTF8-encoded log message
 * 
 * To stop the log server, send this message:
 * Frame 0: \x55\x01\xFF
 */
class LogServer {
public:
    LogServer(zctx_t* ctx, LogLevel logLevel = LogLevel::Debug, const std::string& endpoint = std::string(DEFAULT_LOG_ENDPOINT));
    ~LogServer();
    /**
     * Start the log server message handler in the current thread.
     * Blocks until a stop message is received
     */
    void start();
    /**
     * Starts a new thread that executes the start() function
     */
    void startInNewThread();
    void setLogLevel(LogLevel logLevel);
    LogLevel getLogLevel();
    void addLogSink(LogSink* logSink);
private:
    void* internalSocket;
    LogLevel logLevel;
    zctx_t* ctx;
    std::thread* thread;
    std::vector<LogSink*> logSinks;
};

#endif	/* LOGSERVER_HPP */

