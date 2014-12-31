/**
 * LogSinks.hpp 
 * 
 * Provides serveral standard logsink implementation
 */

#ifndef LOGSINKS_HPP
#define LOGSINKS_HPP
#include "Logger.hpp"
#include <string>
#include <fstream>
#include <deque>
#include <mutex>

/**
 * A LogSink instance represents the final destination of a log message,
 * e.g. a rotating file log sink or a email log sink
 * 
 * The virtual function overhead seems reasonable here because logging is ZMQ-async.
 */
class LogSink {
public:
    virtual void log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage) = 0;
    virtual ~LogSink();
};


/**
 * Convert a LogLevel to a string describing it,
 * e.g. LogLevel.Info --> "[Info]"
 */
std::string logLevelToString(LogLevel logLevel);

/**
 * A stderr logsink that uses ANSI terminal logging
 * if stderr is a TTY.
 */
class StderrLogSink : public LogSink {
public:
    StderrLogSink();
    void log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage);
    
    /**
     * Enable or disable ANSI-colored logging
     */
    void setColoredLogging(bool value);
private:
    bool coloredLogging; //Set to true
};

/**
 * A log sink that logs to a log file.
 * TODO: Auto-rotate log files
 */
class FileLogSink : public LogSink {
public:
    FileLogSink(const std::string& filename);
    ~FileLogSink();
    void log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage);
private:
    std::ofstream fout;
    std::string filename;
};

/**
 * A log sink that uses a ring buffer to store a predefined amount
 * of log messages
 */
class BufferLogSink : public LogSink {
public:
    struct LogMessage {
        LogMessage(LogLevel level, uint64_t timestamp, const std::string& message, const std::string& sender);
        LogLevel level;
        uint64_t timestamp;
        std::string message;
        std::string sender;
    };
    /**
     * Construct a new buffer log sink with a given maximum sice
     */
    BufferLogSink(size_t maxBufferSize);
    ~BufferLogSink();
    void log(LogLevel logLevel, uint64_t timestamp, const std::string& senderName, const std::string& logMessage);
    /**
     * Lock the instance and get the instance buffer.
     * Remember to call unlock() as soon as possible
     */
    std::deque<LogMessage>& getLogMessages();
    void unlock();
private:
    std::deque<LogMessage> buffer;
    size_t maxBufferSize;
    /**
     * The log buffer can be accessed from multiple threads,
     * so this is used for locking.
     */
    std::mutex bufferMutex;
};

#endif //LOGSINKS_HPP