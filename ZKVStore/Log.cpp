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
#include <ctime>

static const char* const ESCAPE_BOLD = "\x1B[1m";
static const char* const ESCAPE_BLACK_FOREGROUND = "\x1B[30m";
static const char* const ESCAPE_RED_FOREGROUND = "\x1B[31m";
static const char* const ESCAPE_GREEN_FOREGROUND = "\x1B[32m";
static const char* const ESCAPE_YELLOW_FOREGROUND = "\x1B[33m";
static const char* const ESCAPE_BLUE_FOREGROUND = "\x1B[34m";
static const char* const ESCAPE_MAGENTA_FOREGROUND = "\x1B[35m";
static const char* const ESCAPE_CYAN_FOREGROUND = "\x1B[36m";
static const char* const ESCAPE_WHITE_FOREGROUND = "\x1B[37m";
static const char* const ESCAPE_BOLD_BLACK_FOREGROUND = "\x1B[30;1m";
static const char* const ESCAPE_BOLD_RED_FOREGROUND = "\x1B[31;1m";
static const char* const ESCAPE_BOLD_GREEN_FOREGROUND = "\x1B[32;1m";
static const char* const ESCAPE_BOLD_YELLOW_FOREGROUND = "\x1B[33;1m";
static const char* const ESCAPE_BOLD_BLUE_FOREGROUND = "\x1B[34;1m";
static const char* const ESCAPE_BOLD_MAGENTA_FOREGROUND = "\x1B[35;1m";
static const char* const ESCAPE_BOLD_CYAN_FOREGROUND = "\x1B[36;1m";
static const char* const ESCAPE_BOLD_WHITE_FOREGROUND = "\x1B[37;1m";

inline static void printDate(std::ostream& stream) {
    //Get microsecond precision time
    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm * timeinfo;
    //Fill the tm struct
    timeinfo = localtime(&(tv.tv_sec));
    //Print the tm data to the ostream
    std::streamsize originalPrecision = stream.precision();
    stream.precision(3);
    stream << '[' << (timeinfo->tm_year + 1900) << '-' << (timeinfo->tm_mon + 1)
            << '-' << timeinfo->tm_mday << ' ' << timeinfo->tm_hour << ':'
            << timeinfo->tm_min << ':' << timeinfo->tm_sec << '.' << (tv.tv_usec / 1000) << ']';
    stream.precision(originalPrecision);
}

LogSource::LogSource(zctx_t* ctx, const std::string& name, const std::string& endpoint) : ctx(ctx), loggerName(name) {
    socket = zsocket_new(ctx, ZMQ_PUB);
    if (unlikely(zsocket_connect(socket, endpoint.c_str()))) {
        fprintf(stderr, "Failed to connect log source to endpoint %s", endpoint.c_str());
    }
}

LogSource::~LogSource() {
    zsocket_destroy(ctx, socket);
}

void LogSource::log(const std::string& message, LogLevel level) {
    //Send the frames individually so no message needs to be allocated
    zframe_t* frame;
    frame = zframe_new(&level, sizeof (LogLevel));
    assert(!zframe_send(&frame, socket, ZFRAME_MORE));
    frame = zframe_new(loggerName.c_str(), loggerName.size());
    assert(!zframe_send(&frame, socket, ZFRAME_MORE));
    frame = zframe_new(message.c_str(), message.size());
    assert(!zframe_send(&frame, socket, 0));
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

LogServer::LogServer(zctx_t* ctx, LogLevel logLevel, const std::string& endpoint) : ctx(ctx), logLevel(logLevel), thread(nullptr) {
    internalSocket = zsocket_new(ctx, ZMQ_SUB);
    const char* subscription = "";
    zsocket_set_subscribe(internalSocket, (char*) subscription);
    if (unlikely(zsocket_bind(internalSocket, endpoint.c_str()))) {
        fprintf(stderr, "Failed to connect log source to endpoint %s", endpoint.c_str());
    }
}
using namespace std;

LogServer::~LogServer() {
    if (thread) {
        //Send the STOP message (single empty frame);
        zframe_t* frame = zframe_new("", 0);
        assert(!zframe_send(&frame, internalSocket, 0));
        thread->join(); //Wait until it exits
        delete thread;
    }
    zsocket_destroy(ctx, internalSocket);
}

void LogServer::start() {
    while (true) {
        cout << "Logger waiting" << endl;
        zmsg_t* msg = zmsg_recv(internalSocket);
        if (!msg && errno == EINTR) {
            break;
        }
        assert(msg);
        zframe_t* logLevelFrame = zmsg_first(msg);
        assert(logLevelFrame);
        //Handle STOP messages
        if (zframe_size(logLevelFrame) == 0) {
            cout << "Logger exiting" << endl;
            break;
        }
        cout << "RX" << endl;
        zframe_t* senderName = zmsg_next(msg);
        assert(senderName);
        zframe_t* logMessageFrame = zmsg_next(msg);
        assert(logMessageFrame);
        //Parse the frames
        LogLevel logLevel = *((LogLevel*) zframe_data(logLevelFrame));
        //Get time info
        //        time_t rawtime;
        //        struct tm * timeinfo;
        //        time(&rawtime);
        //        timeinfo = localtime(&rawgtime);
        cout << "YZY" << endl;
        //        switch (logLevel) {
        //            case LogLevel::Error:
        //            {
        //                std::cout << ESCAPE_BOLD_RED_FOREGROUND;
        //                printDate(std::cout);
        //                std::cout << "[Error]" << frameToString(logMessageFrame) << std::endl;
        //                break;
        //            }
        //            case LogLevel::Warn:
        //            {
        //                std::cout << ESCAPE_BOLD_RED_FOREGROUND;
        //                printDate(std::cout);
        //                std::cout << "[Error]" << frameToString(logMessageFrame) << std::endl;
        //                break;
        //            }
        //            case LogLevel::Info:
        //            {
        //                std::cout << "DU1";
        //                break;
        //            }
        //            case LogLevel::Debug:
        //            {
        //                std::cout << "DU2";
        //                break;
        //            }
        //            case LogLevel::Trace:
        //            {
        //                std::cout << "DU3";
        //                break;
        //            }
        //            default:
        //            {
        //                std::cout << "DU4";
        //                break;
        //            }
        //        }
        std::cout << std::endl;
        zmsg_destroy(&msg);
    }
}

/**
 * Starts a new thread that executes the start() function
 */
void LogServer::startInNewThread() {
    thread = new std::thread(std::mem_fun(&LogServer::start), this);
}

void COLD LogServer::setLogLevel(LogLevel logLevel) {
    this->logLevel = logLevel;
}

LogLevel COLD LogServer::getLogLevel() {
    return logLevel;
}