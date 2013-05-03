/* 
 * File:   Log.cpp
 * Author: uli
 * 
 * Created on 24. April 2013, 04:29
 */

#include "Log.hpp"
#include "endpoints.hpp" //#defines the endpoint string
#include "zutil.hpp"
#include "macros.hpp"
#include <czmq.h>
#include <cstdio>

LogSource::LogSource(zctx_t* ctx, const std::string& name, const std::string& endpoint) : ctx(ctx), loggerName(name) {
    socket = zsocket_new(ctx, ZMQ_PUB);
    if (unlikely(zsocket_connect(socket, endpoint.c_str()))) {
        fprintf(stderr, "Failed to connect log source to endpoint %s", endpoint.c_str());
    }
}

LogSource::~LogSource() {
    zsocket_destroy(ctx, &socket);
}

void LogSource::log(const std::string& message, const LogLevel level = LogLevel::Info) {
    //Send the frames individually so no message needs to be allocated
    zframe_send(zframe_new(&LogLevel, sizeof (LogLevel)), socket, ZFRAME_MORE);
    zframe_send(zframe_new(loggerName.c_str(), loggerName.size()), socket, ZFRAME_MORE);
    zframe_send(zframe_new(message.c_str(), message.size()), socket, ZFRAME_MORE);
}

void LogSource::error(const std::string& message) {
    log(message, LogLevel::Error);
}

void LogSource::warn(const std::string& message) {
    log(message, LogLevel::Warn);
}

void LogSource::info(const std::string& message) {
    log(message, LogLevel::Info);
}

void LogSource::debug(const std::string& message) {
    log(message, LogLevel::Debug);
}

void LogSource::trace(const std::string& message) {
    log(message, LogLevel::Trace);
}

LogServer::LogServer(zctx_t* ctx, const std::string& endpoint) : ctx(ctx) {
    internalSocket = zsocket_new(ctx, ZMQ_SUB);
    if (unlikely(internalSocket(socket, endpoint.c_str()))) {
        fprintf(stderr, "Failed to connect log source to endpoint %s", endpoint.c_str());
    }
}

LogServer::~LogServer() {
    zsocket_destroy(ctx, &internalSocket);
}

void LogServer::start() {

}