/* 
 * File:   Log.hpp
 * Author: uli
 *
 * Created on 24. April 2013, 04:29
 */

#ifndef LOG_HPP
#define	LOG_HPP
#include <string>

enum LogLevel : uint16_t {
    LogLevelError = 0,
    LogLevelWarn = 10,
    LogLevelInfo = 100,
    LogLevelDebug = 1000,
    LogLevelTrace = 10000
};

/**
 * A log client that connects to the Log server via inproc transport
 */
class LogSource {
public:
    LogSource(zctx_t* ctx, const std::string& name);
    ~LogSource();
private:
    zctx_t* ctx;
    void* socket;
};


/**
 * A log server that proxies inproc to an external endpoint
 * using XPUB/XSUB pattern
 * 
 * The proxy is started in a separate thread that can be stopped
 * using a specific message 
 */
class LogProxy {
public:
    LogProxy();
private:

};


#endif	/* LOG_HPP */

