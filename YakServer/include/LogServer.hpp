/* 
 * File:   LogServer.hpp
 * Author: uli
 *
 * Created on 8. Mai 2013, 19:58
 */

#ifndef LOGSERVER_HPP
#define	LOGSERVER_HPP
#include "LogSinks.hpp"
#include "Logger.hpp"
#include <thread>

/**
 * A log server that proxies inproc pattern to external PUB/SUB
 * and calls a runtime-configurable list of log sinks for each log message
 * 
 * A PULL-like socket is bound to the endpoint supplied at construction time.
 * 
 * The proxy is started in a separate thread that can be stopped
 * using a specific message 
 * 
 * Protocol specification (messages from logger to server):
 * Frame 0: Header: Magic byte + protocol version + request type - \x55\x01\x00
 * Frame 1: Log level (1 byte, as defined in the LogLevel enum)
 * Frame 2: uint64_t timestamp, localized = 1000 * epoch seconds + milliseconds = zclock_gettime()
 * Frame 3: UTF8-encoded sender name
 * Frame 4: UTF8-encoded log message
 * 
 * To stop the log server, send this message:
 * Frame 0: \x55\x01\xFF
 */
class LogServer {
public:
    /**
     * Create a new ZMQ log server.
     * @param ctx The ZeroMQ context to use
     * @param logLevel The log level of the server. Log msgs more verbose than this level are ignored
     * @param autoStart If this is set to true, the worker thread is started in the constructor.
     *                  Note that the worker thread always needs to be started before the first
     *                  logger is created for inproc:// endpoints (e.g. the default endpoint)
     * @param endpoint The endpoint to use.
     *                 Must be the same endpoint as the endpoint the loggers connect to.
     *                 By using different endpoints you can run different log servers in parallel.
     */
    LogServer(void* ctx,
            LogLevel logLevel = LogLevel::Debug,
            bool autoStart = false,
            const std::string& endpoint = std::string(DEFAULT_LOG_ENDPOINT));
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
    /**
     * Manual logging. This can be used after the ZeroMQ context has been terminated.
     * It logs synchronously by piping the message into all log sinks
     * @param loggerName The name of the simulated logger
     * @param logLevel The log level of the message
     * @param message The log message itself
     */
    void log(const std::string& loggerName, LogLevel msgLogLevel, const std::string& message);
    /**
     * Gracefully terminates the log server thread
     */
    void terminate();
private:
    void* logRequestInputSocket;
    LogLevel logLevel;
    void* ctx;
    std::thread* thread;
    std::vector<LogSink*> logSinks;
    Logger logger;
    std::string endpoint;
};

#endif	/* LOGSERVER_HPP */

