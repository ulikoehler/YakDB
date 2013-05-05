/* 
 * File:   Log.hpp
 * Author: uli
 *
 * Created on 24. April 2013, 04:29
 */

#ifndef LOG_HPP
#define	LOG_HPP
#include <string>
#include <czmq.h>
#include <thread>

//Internal endpoint of the internal log PULL socket
#define DEFAULT_LOG_ENDPOINT "inproc://defaultLog"

enum class LogLevel : uint8_t {
    Error = 0,
            Warn = 1,
            Info = 2,
            Debug = 3,
            Trace = 4
};

/**
 * A log client that connects to the Log server via inproc transport
 */
class LogSource {
public:
    LogSource(zctx_t* ctx, const std::string& name, const std::string& endpoint = std::string(DEFAULT_LOG_ENDPOINT));
    ~LogSource();
    void log(const std::string& message, LogLevel level = LogLevel::Info);
    void error(const std::string& message);
    void warn(const std::string& message);
    void info(const std::string& message);
    void debug(const std::string& message);
    void trace(const std::string& message);
private:
    zctx_t* ctx;
    void* socket;
    std::string loggerName;
};

/**
 * A log server that proxies inproc pattern to external PUB/SUB
 * 
 * A PULL-like socket is bound to the endpoint supplied at construction time.
 * 
 * The proxy is started in a separate thread that can be stopped
 * using a specific message 
 * 
 * Log message format specification:
 *      Frame 1: 1 byte log level
 *      Frame 2: Name of sender
 *      Frame 3: Log message
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
private:
    void* internalSocket;
    LogLevel logLevel;
    zctx_t* ctx;
    std::thread* thread;
};


#endif	/* LOG_HPP */

