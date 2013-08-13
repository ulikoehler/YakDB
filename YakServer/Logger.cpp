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

/**
 * Checks if the return code from a zmsg_send call indicates an error.
 * If it indicates an error, logs a bold red error message on stderr,
 * unless the context was terminated by a signal
 */
static inline void checkLogSendError(int rc, const std::string& loggerName, const std::string& message) {
	if(unlikely(rc == -1)) {
		if(zctx_interrupted) {
			return;
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
static inline uint64_t HOT getCurrentLogTime() {
    return zclock_time();
}

Logger::Logger(zctx_t* ctx, const std::string& name, const std::string& endpoint) : ctx(ctx), loggerName(name) {
    socket = zsocket_new(ctx, ZMQ_PUSH);
    if (unlikely(socket == nullptr)) {
        fprintf(stderr, "\x1B[31;1m[Critical] Failed to create log socket while initializing logger with sender name '%s'\x1B[0;30m\n", loggerName.c_str());
        fflush(stderr);
    }
    if (unlikely(zsocket_connect(socket, endpoint.c_str()) == -1)) {
		/**
		 * All logging will fail if the connect fails,
		 * so this is really a critical error. Log it in bold red.
		 * 
		 * This is no error if the context was interrupted
		 */
        fprintf(stderr,
				"\x1B[31;1m[Critical] Failed to connect log source to endpoint '%s' while initializing logger with sender name '%s'\x1B[0;30m\n",
				endpoint.c_str(),
				loggerName.c_str());
        fflush(stderr);
    }
}

Logger::~Logger() {
    fflush(stderr);
    zsocket_destroy(ctx, socket);
}

void Logger::log(const std::string& message, LogLevel level) {
    uint64_t currentLogTime = getCurrentLogTime();
    zmq_msg_t msg;
	//Header frame
	fillMsgConst(&msg, "\x55\x01\x00", 3);
	checkLogSendError(zmq_msg_send(&msg, socket, ZMQ_SNDMORE), loggerName, message);
	//Log level frame
    fillMsg(&msg, &level, sizeof (LogLevel));
    checkLogSendError(zmq_msg_send(&msg, socket, ZMQ_SNDMORE), loggerName, message);
	//Log time
    fillMsg(&msg, &currentLogTime, sizeof (uint64_t));
    checkLogSendError(zmq_msg_send(&msg, socket, ZMQ_SNDMORE), loggerName, message);
	//Log name
    fillMsg(&msg, loggerName.c_str(), loggerName.size());
    checkLogSendError(zmq_msg_send(&msg, socket, ZMQ_SNDMORE), loggerName, message);
	//Log message
    fillMsg(&msg, message.c_str(), message.size());
    checkLogSendError(zmq_msg_send(&msg, socket, 0), loggerName, message);
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
