/* 
 * File:   LogServer.hpp
 * Author: uli
 *
 * Created on 8. Mai 2013, 19:58
 */

#ifndef LOGSERVER_HPP
#define	LOGSERVER_HPP
#include "Logger.hpp"

/**
 * A LogSink instance represents the final destination of a log message,
 * e.g. a rotating file log sink or a email log sink
 */
class LogSink {
public:
    virtual void log(LogLevel logLevel, uint64_t timestamp, std::string senderName, std::string& logMessage) = 0;
};

class StderrLogSink : public LogSink {
public:
    StderrLogSink();
    void log(LogLevel logLevel, uint64_t timestamp, std::string senderName, std::string& logMessage);
};

/**
 * A log server that proxies inproc pattern to external PUB/SUB
 * 
 * A PULL-like socket is bound to the endpoint supplied at construction time.
 * 
 * The proxy is started in a separate thread that can be stopped
 * using a specific message 
 * 
 * To stop the log, send a message with one zero-sized frame
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
