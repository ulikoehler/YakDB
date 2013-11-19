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
#include <cassert>
#include <time.h>
#include <sys/time.h>

/**
 * Checks if the return code from a zmsg_send call indicates an error.
 * If it indicates an error, logs a bold red error message on stderr,
 * unless the context was terminated by a signal
 */
static inline void checkLogSendError(int rc, const std::string& loggerName, const std::string& message) {
    if(unlikely(rc == -1)) {
        if(yak_interrupted) {
            return; //TODO CRITICAL Fix that. Won' return the right function.
        }
        fprintf(stderr,
                "\x1B[31;1m[Error] Logger '%s' failed to send log message '%s' to log server, reason: '%s'\x1B[0;30m\n",
                loggerName.c_str(),
                message.c_str(),
                zmq_strerror(errno));
        fflush(stderr);
    }
}

/**
 * Get the 64-bit log time: epoch (secs) * 1000 + epoch-millisecs
 */
uint64_t Logger::getCurrentLogTime() {
    //This is equivalent to zclock_gettime(), but we don't need the dependency.
    struct timeval tv;
    gettimeofday (&tv, NULL);
    return (int64_t) ((int64_t) tv.tv_sec * 1000 + (int64_t) tv.tv_usec / 1000);
}

Logger::Logger(void* ctx, const std::string& name, const char* endpoint) : loggerName(name) {
    assert(ctx);
    assert(endpoint);
    socket = zmq_socket_new_connect(ctx, ZMQ_PUSH, endpoint);
    //TODO The error loggers don't check isatty!
    if (unlikely(socket == nullptr)) {
        fprintf(stderr, "\x1B[31;1m[Critical] Failed to create log socket while initializing logger with sender name '%s': '%s'\x1B[0;30m\n",
                loggerName.c_str(),
                zmq_strerror(errno)
               );
        fflush(stderr);
    }
}

void Logger::terminate() {
    if(socket != nullptr) {
        zmq_close(socket);
        socket = nullptr;
    }
}


Logger::~Logger() {
    terminate();
}

void Logger::log(const std::string& message, LogLevel level) {
    assert(socket);
    uint64_t currentLogTime = getCurrentLogTime();
    //Header frame
    checkLogSendError(zmq_send_const(socket, "\x55\x01\x00", 3, ZMQ_SNDMORE), loggerName, message);
    //Log level frame
    checkLogSendError(zmq_send(socket, &level, sizeof (LogLevel), ZMQ_SNDMORE), loggerName, message);
    //Log time
    checkLogSendError(zmq_send(socket, &currentLogTime, sizeof (uint64_t), ZMQ_SNDMORE), loggerName, message);
    //Log name
    checkLogSendError(zmq_send(socket, loggerName.c_str(), loggerName.size(), ZMQ_SNDMORE), loggerName, message);
    //Log message
    checkLogSendError(zmq_send(socket, message.c_str(), message.size(), 0), loggerName, message);
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
