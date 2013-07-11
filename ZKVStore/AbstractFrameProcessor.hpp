/* 
 * File:   AbstractFrameProcessor.hpp
 * Author: uli
 *
 * Created on 11. Juli 2013, 03:16
 */

#ifndef ABSTRACTFRAMEPROCESSOR_HPP
#define	ABSTRACTFRAMEPROCESSOR_HPP
#include <czmq.h>
#include <string>
#include "Logger.hpp"
#include <leveldb/status.h>

/**
 * An abstract class that provides basic parsing
 * and checking methods
 * to subclasses
 */
class AbstractFrameProcessor {
public:
    AbstractFrameProcessor(zctx_t* ctx,
            int inputSocketType,
            int outputSocketType,
            const std::string& loggerName);
    ~AbstractFrameProcessor();
protected:
    zctx_t* context;
    void* processorInputSocket;
    void* processorOutputSocket;
    Logger logger;
    /**
     * Parse a table ID frame, as little endian 32 bit unsigned integer in one frame.
     * Automatically receives the frame from replyProxySocket.
     * Automatically checks if there is a frame available
     * Input is read from processorInputSocket,
     * output is read from processorOutputSocket.
     * @param tableIdDst Where the table ID shall be placed
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @param errorResponseCode A 4-long response code to use if generating an error response
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool parseUint32Frame(uint32_t& tableIdDst,
            const std::string& frameDesc,
            bool generateResponse,
            const char* errorResponseCode);
    /**
     * Parse a 64-bit little-endian unsigned integer in one frame.
     * Automatically receives the frame from workPullSocket.
     * Automatically checks if there is a frame available.
     * @param tableIdDst Where the table ID shall be placed
     * @param frameDesc A string describing the frame, for meaningful debug messages
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @param errorResponseCode A 4-long response code to use if generating an error response
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool parseUint64Frame(uint64_t& tableIdDst,
            const std::string& frameDesc,
            bool generateResponse,
            const char* errorResponseCode);
    /**
     * Equivalent to parseUint64Frame(), but assumes a given default
     * value if the frame is empty.
     */
    bool parseUint64FrameOrAssumeDefault(uint64_t& tableIdDst,
            uint64_t defaultValue,
            const std::string& frameDesc,
            bool generateResponse,
            const char* errorResponseCode);
    /**
     * Ensure the work pull socket has a next message part in the current message
     * @param errString A descriptive error string logged and sent to the client
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @param errorResponseCode A 4-long response code to use if generating an error response
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool expectNextFrame(const char* errString,
            bool generateResponse,
            const char* errorResponseCode);
    /**
     * Ensure the given LevelDB status code indicates success
     * @param errString A descriptive error string logged and sent to the client. status error string will be appended.
     * @param generateResponse Set this to true if an error message shall be sent on error.
     * @param errorResponseCode A 4-long response code to use if generating an error response
     * @return True on success, false if an error has been handled and the caller shall stop processing.
     */
    bool checkLevelDBStatus(const leveldb::Status& status,
            const char* errString,
            bool generateResponse,
            const char* errorResponseCode);
};

#endif	/* ABSTRACTFRAMEPROCESSOR_HPP */

