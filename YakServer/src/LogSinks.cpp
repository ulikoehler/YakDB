#include "LogSinks.hpp"
#include "macros.hpp"
#include <unistd.h>
#include <iostream>
#include <ctime>
#include <sstream>

static const char* const ESCAPE_BOLD = "\x1B[1m";
static const char* const ESCAPE_NORMALFONT = "\x1B[0m";
static const char* const ESCAPE_BLACK_FOREGROUND = "\x1B[30m";
static const char* const ESCAPE_RED_FOREGROUND = "\x1B[31m";
static const char* const ESCAPE_GREEN_FOREGROUND = "\x1B[32m";
static const char* const ESCAPE_YELLOW_FOREGROUND = "\x1B[33m";
static const char* const ESCAPE_BLUE_FOREGROUND = "\x1B[34m";
static const char* const ESCAPE_MAGENTA_FOREGROUND = "\x1B[35m";
static const char* const ESCAPE_CYAN_FOREGROUND = "\x1B[36m";
static const char* const ESCAPE_WHITE_FOREGROUND = "\x1B[37m";

static void HOT printDateTime(uint64_t timestamp, std::ostream& stream) {
    //Convert timestamp to timeval
    struct timeval tv;
    tv.tv_sec = timestamp / 1000;
    tv.tv_usec = (timestamp % 1000) * 1000;
    //Format the tm data
    char dateBuffer[32];
    size_t formattedLength = strftime(dateBuffer, 32, "%F %T", localtime(&(tv.tv_sec)));
    assert(formattedLength > 0);
    //Format the subsecond part
    snprintf(dateBuffer + formattedLength, 32 - formattedLength, ".%03lu", (unsigned long) (tv.tv_usec / 1000));
    stream << '[' << dateBuffer << ']';
}


std::string logLevelToString(LogLevel logLevel) {
    switch (logLevel) {
        case LogLevel::Critical: {
            return "Critical";
        }
        case LogLevel::Error: {
            return "Error";
        }
        case LogLevel::Warn: {
            return "Warn";
        }
        case LogLevel::Info: {
            return "Info";
        }
        case LogLevel::Debug: {
            return "Debug";
        }
        case LogLevel::Trace: {
            return "Trace";
        }
        default: {
            return "Unknown";
        }
    }
}

LogSink::~LogSink() {
    
}

StderrLogSink::StderrLogSink() : coloredLogging(isatty(fileno(stderr))) {

}

void StderrLogSink::setColoredLogging(bool value) {
    this->coloredLogging = value;
}

void HOT StderrLogSink::log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage) {

    switch (logLevel) {
        case LogLevel::Critical: {
            if(coloredLogging) {
                std::cerr << ESCAPE_BOLD << ESCAPE_RED_FOREGROUND;
            }
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Error] " << senderName << " - " << logMessage;
            if(coloredLogging) {
                std::cerr << ESCAPE_NORMALFONT << ESCAPE_BLACK_FOREGROUND;
            }
            std::cerr << std::endl;
            break;
        }
        case LogLevel::Error: {
            if(coloredLogging) {
                std::cerr << ESCAPE_RED_FOREGROUND;
            }
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Error] " << senderName << " - ";
            if(coloredLogging) {
                std::cerr <<  ESCAPE_BLACK_FOREGROUND;
            }
            std::cerr << logMessage << std::endl;
            break;
        }
        case LogLevel::Warn: {
            if(coloredLogging) {
                std::cerr << ESCAPE_YELLOW_FOREGROUND;
            }
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Warning] " << senderName << " - ";
            if(coloredLogging) {
                std::cerr << ESCAPE_BLACK_FOREGROUND;
            }
            std::cerr << logMessage << std::endl;
            break;
        }
        case LogLevel::Info: {
            if(coloredLogging) {
                std::cerr << ESCAPE_GREEN_FOREGROUND;
            }
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Info] " << senderName << " - ";
            if(coloredLogging) {
                std::cerr << ESCAPE_BLACK_FOREGROUND;
            }
            std::cerr << logMessage << std::endl;
            break;
        }
        case LogLevel::Debug: {
            if(coloredLogging) {
                std::cerr << ESCAPE_BLUE_FOREGROUND;
            }
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Debug] " << senderName << " - ";
            if(coloredLogging) {
                std::cerr << ESCAPE_BLACK_FOREGROUND;
            }
            std::cerr << logMessage << std::endl;
            break;
        }
        case LogLevel::Trace: {
            if(coloredLogging) {
                std::cerr << ESCAPE_CYAN_FOREGROUND;
            }
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Trace] " << senderName << " - ";
            if(coloredLogging) {
                std::cerr << ESCAPE_BLACK_FOREGROUND;
            }
            std::cerr << logMessage << std::endl;
            break;
        }
        default: {
            printDateTime(timestamp, std::cerr);
            std::cerr << "[Unknown] " << senderName << " - " << logMessage << std::endl;
            break;
        }
    }
}

FileLogSink::FileLogSink(const std::string& filename) : fout(filename.c_str()), filename(filename) {
}

FileLogSink::~FileLogSink() {
    fout.close();
}

void FileLogSink::log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage) {
    printDateTime(timestamp, fout);
    //Not flushing (endl) would be faster, but log msgs before a crash might be lost
    fout << " [" << logLevelToString(logLevel) << "] " << senderName << " - " << logMessage << std::endl;
}

BufferLogSink::LogMessage::LogMessage(LogLevel level, uint64_t timestamp, const std::string& message, const std::string& sender) : level(level), timestamp(timestamp), message(message), sender(sender) {
}

BufferLogSink::BufferLogSink(size_t maxBufferSize) : maxBufferSize(maxBufferSize), bufferMutex() {
    
}

BufferLogSink::~BufferLogSink() {
    
}

std::deque<BufferLogSink::LogMessage>& BufferLogSink::getLogMessages() {
    bufferMutex.lock();
    return buffer;
}

void BufferLogSink::unlock() {
    bufferMutex.unlock();
}

void BufferLogSink::log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage) {
    //Create the entry
    bufferMutex.lock();
    buffer.emplace_back(logLevel, timestamp, logMessage, senderName);
    //Expunge the first msgs if buffer is full
    if(buffer.size() > maxBufferSize) {
        buffer.pop_front();
    }
    bufferMutex.unlock();
}