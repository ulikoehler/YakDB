/* 
 * File:   Log.cpp
 * Author: uli
 * 
 * Created on 24. April 2013, 04:29
 */

#include "Logger.hpp"
#include "endpoints.hpp" //#defines the endpoint string
#include "zutil.hpp"
#include "macros.hpp"
#include <cstdio>
#include <functional>
#include <iostream>
#include <ctime>

//This macro checks a zframe_send msg for errors and prints a verbose error message on stderr if any occur
#define GUARDED_LOGSEND(expr) if(unlikely((expr) != 0)) {fprintf(stderr, "\x1B[31;1m[Error] Logger '%s' failed to send log message '%s' to log server, reason: '%s'\x1B[0;30m\n", loggerName.c_str(), message.c_str(), zmq_strerror(errno));fflush(stderr);}

/**
 * Get the 64-bit log time: epoch (secs) * 1000 + epoch-millisecs
 */
static inline uint64_t HOT getCurrentLogTime() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return ((uint64_t) tv.tv_sec)*1000 + (tv.tv_usec / 1000);
}

Logger::Logger(zctx_t* ctx, const std::string& name, const std::string& endpoint) : ctx(ctx), loggerName(name) {
    socket = zsocket_new(ctx, ZMQ_PUSH);
    if(unlikely(socket == nullptr)) {
        fprintf(stderr, "\x1B[31;1m[Critical] Failed to create log socket while initializing logger with sender name '%s'\x1B[0;30m\n", loggerName.c_str());
        fflush(stderr);
    }
    if (unlikely(zsocket_connect(socket, endpoint.c_str()))) {
        //All logging will fail if the connect fails,
        // so this is really a critical error. Log it in bold red.
        fprintf(stderr, "\x1B[31;1m[Critical] Failed to connect log source to endpoint '%s' while initializing logger with sender name '%s'\x1B[0;30m\n", endpoint.c_str(), loggerName.c_str());
        fflush(stderr);
    }
}

Logger::~Logger() {
    fprintf(stderr, "Destructing logger '%s'\n", loggerName.c_str());
    fflush(stderr);
    zsocket_destroy(ctx, socket);
}

void Logger::log(const std::string& message, LogLevel level) {
    //Send the frames individually so no message needs to be allocated
    zframe_t* frame;
    frame = zframe_new("\x55\x01\x00", 3);
    GUARDED_LOGSEND(zframe_send(&frame, socket, ZFRAME_MORE));
    frame = zframe_new(&level, sizeof (LogLevel));
    GUARDED_LOGSEND(zframe_send(&frame, socket, ZFRAME_MORE));
    uint64_t currentLogTime = getCurrentLogTime();
    frame = zframe_new(&currentLogTime, sizeof (uint64_t));
    GUARDED_LOGSEND(zframe_send(&frame, socket, ZFRAME_MORE));
    frame = zframe_new(loggerName.c_str(), loggerName.size());
    GUARDED_LOGSEND(zframe_send(&frame, socket, ZFRAME_MORE));
    frame = zframe_new(message.c_str(), message.size());
    GUARDED_LOGSEND(zframe_send(&frame, socket, 0));
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
