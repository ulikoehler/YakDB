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
#include <cstdio>
#include <functional>
#include <iostream>
#include <iomanip>

Logger::Logger(zctx_t* ctx, const std::string& name, const std::string& endpoint) : ctx(ctx), loggerName(name) {
    socket = zsocket_new(ctx, ZMQ_PUSH);
    if (unlikely(zsocket_connect(socket, endpoint.c_str()))) {
        fprintf(stderr, "Failed to connect log source to endpoint %s", endpoint.c_str());
    }
}

Logger::~Logger() {
    zsocket_destroy(ctx, socket);
}

void Logger::log(const std::string& message, LogLevel level) {
    //Send the frames individually so no message needs to be allocated
    zframe_t* frame;
    frame = zframe_new(&level, sizeof (LogLevel));
    assert(!zframe_send(&frame, socket, ZFRAME_MORE));
    frame = zframe_new(loggerName.c_str(), loggerName.size());
    assert(!zframe_send(&frame, socket, ZFRAME_MORE));
    frame = zframe_new(message.c_str(), message.size());
    assert(!zframe_send(&frame, socket, 0));
}

void Logger::error(const std::string& message) {
    log(message, LogLevel::Error);
}

void Logger::critical(const std::string& message) {
    log(message, LogLevel::Critical);
}

void Logger::warn(const std::string& message) {
    log(message, LogLevel::Warn);
}

void Logger::info(const std::string& message) {
    log(message, LogLevel::Info);
}

void Logger::debug(const std::string& message) {
    log(message, LogLevel::Debug);
}

void Logger::trace(const std::string& message) {
    log(message, LogLevel::Trace);
}
