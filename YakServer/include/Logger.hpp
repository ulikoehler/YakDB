/* 
 * File:   Log.hpp
 * Author: uli
 *
 * Created on 24. April 2013, 04:29
 */

#ifndef LOGGER_HPP
#define	LOGGER_HPP
#include <string>
#include <vector>
#include <czmq.h>
#include <thread>

//Sample usage:
//LogSource source(ctx, "test");
//source.warn("This is a warning");
//source.error("This is an error");
//source.info("This is an information message");
//source.debug("This is a debug message");
//source.trace("This is a trace message");

//Internal endpoint of the internal log PULL socket
#define DEFAULT_LOG_ENDPOINT "inproc://defaultLog"

enum class LogLevel : uint8_t {
    /**
     * Critical: Use this for errors that prevent correct program execution
     * that can't be recovered from
     */
    Critical = 0,
            /**
             * Error: Use  this for errors that prevent normal execution
             * of a well-defined part of the application, if the application
             * is able to recover from these errors without user interaction
             */
            Error = 1,
            /**
             * Warn: Use this for errors that do not prevent normal execution
             * of any part of the application, but might yield unexpected or
             * untested application states that might cause errors during subsequent
             * program execution.
             */
            Warn = 2,
            /**
             * Info: Use this log level for messages that do not represent
             * an unexpected or erroneous application state, but provide
             * useful information not only for developers, but also
             * for application users
             */
            Info = 3,
            /**
             * Debug: Use this log level for messages that do not represent
             * an unexpected or erroneous application state, but provide
             * useful information that is not relevant to the application user
             * but only for debugging purposes
             */
            Debug = 4,
            /**
             * Info: Use this log level for messages that do not represent
             * an unexpected or erroneous application state, but provide
             * fine-grained information about current application state and
             * execution path that is only relevant for application developers
             * if debug-level messages do not provide the required information
             */
            Trace = 5
};

/**
 * A log source connecting to a log server.
 * Message delivery is guaranteed.
 */
class Logger {
public:
    Logger(zctx_t* ctx, const std::string& name, const std::string& endpoint = std::string(DEFAULT_LOG_ENDPOINT));
    ~Logger();
    void log(const std::string& message, LogLevel level = LogLevel::Info);
    void critical(const std::string& message);
    void error(const std::string& message);
    void warn(const std::string& message);
    void info(const std::string& message);
    void debug(const std::string& message);
    void trace(const std::string& message);
    /**
     * Releases all resources acquired by this logger instance.
     * This is automatically called in the destructor,
     * but the context must be active to cleanup properly.
     * 
     * If the current instance has already been cleaned up,
     * the call is ignored.
     * 
     * --> You need to call this if a destructor call would
     *     happen after context termination.
     */
    void terminate();
private:
    zctx_t* ctx;
    void* socket; //Connected to the log server
    std::string loggerName;
};



#endif	/* LOGGER_HPP */

