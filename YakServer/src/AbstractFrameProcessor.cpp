/* 
 * File:   AbstractFrameProcessor.cpp
 * Author: uli
 * 
 * Created on 11. Juli 2013, 03:16
 */

#include <zmq.h>
#include "AbstractFrameProcessor.hpp"
#include "macros.hpp"
#include "zutil.hpp"

AbstractFrameProcessor::AbstractFrameProcessor(void* ctx,
        int inputSocketType,
        int outputSocketType,
        const std::string& loggerName) :
context(ctx),
processorInputSocket(zmq_socket(ctx, inputSocketType)),
processorOutputSocket(zmq_socket(ctx, outputSocketType)),
logger(context, loggerName) {
}

AbstractFrameProcessor::~AbstractFrameProcessor() {
    zmq_close(processorInputSocket);
    zmq_close(processorOutputSocket);
}

bool AbstractFrameProcessor::parseUint32Frame(uint32_t& dst,
        const char* frameDesc,
        bool generateResponse,
        const char* errorResponseCode,
        zmq_msg_t* headerFrame) {
    if (unlikely(!socketHasMoreFrames(processorInputSocket))) {
        std::string errstr = "Trying to read a 32-bit uint frame ("
                + std::string(frameDesc) + "), but no frame was available";
        logger.warn(errstr);
        if (generateResponse) {
            sendResponseHeader(headerFrame, errorResponseCode, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, frameDesc);
        }
        return false;
    }
    //Parse table ID, release frame immediately
    zmq_msg_t tableIdFrame;
    zmq_msg_init(&tableIdFrame);
    if (unlikely(!receiveMsgHandleError(&tableIdFrame, frameDesc, errorResponseCode, generateResponse))) {
        return false;
    }
    if (unlikely(zmq_msg_size(&tableIdFrame) != sizeof (uint32_t))) {
        std::string errstr = "uint32 frame ("
                + std::string(frameDesc)
                + ") was expected to have a length of 4 bytes, but size is "
                + std::to_string(zmq_msg_size(&tableIdFrame)) + " bytes";
        logger.warn(errstr);
        if (generateResponse) {
            sendResponseHeader(headerFrame, errorResponseCode, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, frameDesc);
        }
        return false;
    }
    dst = extractBinary<uint32_t>(&tableIdFrame);
    zmq_msg_close(&tableIdFrame);
    return true;
}

bool AbstractFrameProcessor::parseUint64Frame(uint64_t& valueDest,
        const char* frameDesc,
        bool generateResponse,
        const char* errorResponseCode, zmq_msg_t* headerFrame) {
    if (unlikely(!socketHasMoreFrames(processorInputSocket))) {
        std::string errstr = "Trying to read 64-bit unsigned integer frame ("
                + std::string(frameDesc) + "), but no frame was available";
        logger.warn(errstr);
        if (generateResponse) {
            sendResponseHeader(headerFrame, errorResponseCode, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, frameDesc);
        }
        return false;
    }
    //Parse table ID, release frame immediately
    zmq_msg_t uint64Frame;
    zmq_msg_init(&uint64Frame);
    if (unlikely(!receiveMsgHandleError(&uint64Frame, frameDesc, errorResponseCode, generateResponse))) {
        return false;
    }
    if (unlikely(zmq_msg_size(&uint64Frame) != sizeof (uint64_t))) {
        std::string errstr = "Uint64 frame ("
                + std::string(frameDesc)
                + ") was expected to have a length of 8 bytes, but size is "
                + std::to_string(zmq_msg_size(&uint64Frame)) + " bytes";
        logger.warn(errstr);
        if (generateResponse) {
            sendResponseHeader(headerFrame, errorResponseCode, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, frameDesc);
        }
        return false;
    }
    valueDest = extractBinary<uint64_t>(&uint64Frame);
    zmq_msg_close(&uint64Frame);
    return true;
}

bool AbstractFrameProcessor::parseUint64FrameOrAssumeDefault(uint64_t& valueDest,
        uint64_t defaultValue,
        const char* frameDesc,
        bool generateResponse,
        const char* errorResponseCode, zmq_msg_t* headerFrame) {
    if (unlikely(!socketHasMoreFrames(processorInputSocket))) {
        std::string errstr = "Trying to read 64-bit unsigned integer frame ("
                + std::string(frameDesc)
                + ") with default value, but no frame was available";
        logger.warn(errstr);
        if (generateResponse) {
            sendResponseHeader(headerFrame, errorResponseCode, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, frameDesc);
        }
        return false;
    }
    //Parse table ID, release frame immediately
    zmq_msg_t uint64Frame;
    zmq_msg_init(&uint64Frame);
    if (unlikely(!receiveMsgHandleError(&uint64Frame, frameDesc, errorResponseCode, generateResponse))) {
        return false;
    }
    size_t frameSize = zmq_msg_size(&uint64Frame);
    if (unlikely(frameSize != sizeof (uint64_t) && frameSize != 0)) {
        std::string errstr = "Uint64 frame ("
                + std::string(frameDesc)
                + ") was expected to have a length of 8 bytes, but size is "
                + std::to_string(zmq_msg_size(&uint64Frame)) + " bytes";
        logger.warn(errstr);
        if (generateResponse) {
            sendResponseHeader(headerFrame, errorResponseCode, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, frameDesc);
        }
        return false;
    }
    if (frameSize == 0) {
        valueDest = defaultValue;
    } else {
        valueDest = extractBinary<uint64_t>(&uint64Frame);
    }
    zmq_msg_close(&uint64Frame);
    return true;
}

bool AbstractFrameProcessor::parseUint32FrameOrAssumeDefault(uint32_t& valueDest,
            uint32_t defaultValue,
            const char* frameDesc,
            bool generateResponse,
            const char* errorResponseCode, zmq_msg_t* headerFrame) {
    if (unlikely(!socketHasMoreFrames(processorInputSocket))) {
        
        std::string errstr = "Trying to read 32-bit unsigned integer frame ("
                + std::string(frameDesc)
                + ") with default value, but no frame was available";
        logger.warn(errstr);
        if (generateResponse) {
            sendResponseHeader(headerFrame, errorResponseCode, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, frameDesc);
        }
        return false;
    }
    //Parse table ID, release frame immediately
    zmq_msg_t uint32Frame;
    zmq_msg_init(&uint32Frame);
    if (unlikely(!receiveMsgHandleError(&uint32Frame, frameDesc, errorResponseCode, generateResponse))) {
        return false;
    }
    size_t frameSize = zmq_msg_size(&uint32Frame);
    if (unlikely(frameSize != sizeof (uint32_t) && frameSize != 0)) {
        std::string errstr = "Uint32 frame ("
                + std::string(frameDesc)
                + ") was expected to have a length of 4 bytes, but size is "
                + std::to_string(zmq_msg_size(&uint32Frame)) + " bytes";
        logger.warn(errstr);
        if (generateResponse) {
            sendResponseHeader(headerFrame, errorResponseCode, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, frameDesc);
        }
        return false;
    }
    if (frameSize == 0) {
        valueDest = defaultValue;
    } else {
        valueDest = extractBinary<uint32_t>(&uint32Frame);
    }
    zmq_msg_close(&uint32Frame);
    return true;
}

bool AbstractFrameProcessor::expectNextFrame(const char* errString, bool generateResponse, const char* errorResponseCode, zmq_msg_t* headerFrame) {
    if (unlikely(!socketHasMoreFrames(processorInputSocket))) {
        logger.warn(errString);
        if (generateResponse) {
            sendResponseHeader(headerFrame, errorResponseCode, ZMQ_SNDMORE);
            sendFrame(errString, strlen(errString), processorOutputSocket, logger, errString);
        }
        return false;
    }
    return true;
}

bool AbstractFrameProcessor::checkLevelDBStatus(const leveldb::Status& status,
    const char* errString,
    bool generateResponse,
    const char* errorResponseCode,
    zmq_msg_t* headerFrame) {
    if (unlikely(!status.ok() && !status.IsNotFound())) {
        std::string statusErr = status.ToString();
        std::string completeErrorString = std::string(errString) + statusErr;
        logger.error(completeErrorString);
        if (generateResponse) {
            //Send DB error code
            sendResponseHeader(headerFrame, errorResponseCode, ZMQ_SNDMORE);
            sendFrame(completeErrorString, processorOutputSocket, logger, errString);
            return false;
        }
    }
    return true;
}

bool AbstractFrameProcessor::parseRangeFrames(std::string& startSlice,
        std::string& endSlice,
        const char* errName,
        const char* errorResponse,
        bool generateResponse,
        zmq_msg_t* headerFrame) {
    //Parse the start/end frame
    zmq_msg_t rangeStartFrame, rangeEndFrame;
    zmq_msg_init(&rangeStartFrame);
    if (unlikely(!receiveMsgHandleError(&rangeStartFrame, errName, errorResponse, generateResponse))) {
        return false;
    }
    if (!expectNextFrame(("Only range start frame found in '"
            + std::string(errName) + "', range end frame missing").c_str(),
            generateResponse,
            errorResponse)) {
        zmq_msg_close(&rangeStartFrame);
        return false;
    }
    zmq_msg_init(&rangeEndFrame);
    if (unlikely(!receiveMsgHandleError(&rangeEndFrame, errName, errorResponse, generateResponse))) {
        return false;
    }
    //Convert frames to slices, or use nullptr if empty
    startSlice = std::string((char*) zmq_msg_data(&rangeStartFrame), zmq_msg_size(&rangeStartFrame));
    endSlice = std::string((char*) zmq_msg_data(&rangeEndFrame), zmq_msg_size(&rangeEndFrame));
    //Cleanup
    zmq_msg_close(&rangeStartFrame);
    zmq_msg_close(&rangeEndFrame);
    return true;
}

bool AbstractFrameProcessor::receiveMsgHandleError(zmq_msg_t* msg,
        const char* errName,
        const char* errorResponse,
        bool generateResponse,
        zmq_msg_t* headerFrame) {
    if (unlikely(zmq_msg_recv(msg, processorInputSocket, 0) == -1)) {
        std::string errstr = "Error while receiving message part: "
                + std::string(zmq_strerror(zmq_errno()))
                + " in " + std::string(errName);
        logger.warn(errstr);
        if (generateResponse) {
            sendResponseHeader(headerFrame, errorResponse, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, errName);
        }
        return false;
    }
    return true;
}

bool AbstractFrameProcessor::receiveStringFrame(std::string& frame,
            const char* errName,
            const char* errorResponse,
            bool generateResponse,
            zmq_msg_t* headerFrame) {
    zmq_msg_t msg;
    zmq_msg_init(&msg);
    bool rc = receiveMsgHandleError(&msg, errName, errorResponse, generateResponse);
    if(unlikely(!rc)) {
        return false;
    }
    frame = std::string((char*)zmq_msg_data(&msg), zmq_msg_size(&msg));
    return true;
}

bool AbstractFrameProcessor::sendMsgHandleError(zmq_msg_t* msg,
        int flags,
        const char* errName,
        const char* errorResponse,
        bool generateResponse,
        zmq_msg_t* headerFrame) {
    if (unlikely(zmq_msg_send(msg, processorOutputSocket, flags) == -1)) {
        std::string errstr = "Error while sending message part: "
                + std::string(zmq_strerror(zmq_errno()))
                + " in " + std::string(errName);
        logger.warn(errstr);
        if (generateResponse) {
            sendResponseHeader(headerFrame, errorResponse, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, errName);
        }
        return false;
    }
    return true;
}

void AbstractFrameProcessor::disposeRemainingMsgParts() {
    int numErrors = 0;
    const int errorLimit = 5; //After this number of errors the function exits
    zmq_msg_t msg;
    zmq_msg_init(&msg);
    while (socketHasMoreFrames(processorInputSocket)) {
        if (unlikely(zmq_msg_recv(&msg, processorInputSocket, 0) == -1)) {
            logger.warn("ZMQ error while trying to clear remaining messages from queue: "
                    + std::string(zmq_strerror(zmq_errno())));
            numErrors++;
            if (numErrors >= errorLimit) {
                logger.debug("Exiting disposeRemainingMsgParts() because error limit has been reached");
                break;
            }
        }
        zmq_msg_close(&msg);
    }
}

bool AbstractFrameProcessor::expectExactFrameSize(zmq_msg_t* msg,
            size_t expectedSize,
            const char* errName,
            const char* errorResponse,
            bool generateResponse,
            zmq_msg_t* headerFrame) {
    size_t actualMsgSize = zmq_msg_size(msg);
    if(unlikely(actualMsgSize != expectedSize)) {
        std::string errstr = "Error while checking ZMQ frame length of "
                + std::string(errName) + ": Expected length was "
                + std::to_string(expectedSize) + " bytes but actual length was "
                + std::to_string(actualMsgSize)
                + " in " + std::string(errName);
        logger.warn(errstr);
        if(generateResponse) {
            sendResponseHeader(headerFrame, errorResponse, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, errName);
        }
        return false;
    }
    return true;
}

bool AbstractFrameProcessor::sendUint64Frame(uint64_t value, const char* frameDesc, int flags) {
    zmq_msg_t msg;
    if(unlikely(zmq_msg_init_size(&msg, sizeof(uint64_t)) == -1)) {
        logMessageInitializationError(frameDesc, logger);
        return false;
    }
    memcpy(zmq_msg_data(&msg), &value, sizeof(uint64_t));
    return sendMessage(&msg, frameDesc, flags);
}

bool AbstractFrameProcessor::sendUint32Frame(uint32_t value, const char* frameDesc, int flags) {
    zmq_msg_t msg;
    if(unlikely(zmq_msg_init_size(&msg, sizeof(uint32_t)) == -1)) {
        logMessageInitializationError(frameDesc, logger);
        return false;
    }
    memcpy(zmq_msg_data(&msg), &value, sizeof(uint64_t));
    return sendMessage(&msg, frameDesc, flags);
}

bool AbstractFrameProcessor::sendMessage(zmq_msg_t* msg, const char* frameDesc, int flags) {
    if(unlikely(zmq_msg_send(msg, processorOutputSocket, flags) == -1)){
        logMessageSendError(frameDesc, logger);
        return false;
    }
    return true;
}

bool AbstractFrameProcessor::sendResponseHeader(zmq_msg_t* headerFrame,
    const char* responseHeader,
    int flags,
    size_t responseSize,
    size_t requestExpectedSize) {
    /*
     * Essentially this code handles request IDs which are arbitrary binary
     * strings appended to a request, for identification of async responses.
     * This code ensures that the response header contains the request ID
     * from the request, if any.
     * 
     * If, however, the request size is equal to the response size,
     * we can simple reuse the header frame without needing to copy data around.
     */
    size_t headerFrameSize = (headerFrame == nullptr ? 0 : zmq_msg_size(headerFrame));
    if(headerFrameSize <= requestExpectedSize) {
        //No request ID
        sendFrame(responseHeader, responseSize, processorOutputSocket,
            logger, "Response header", flags);
    } else if (requestExpectedSize == responseSize) {
        //The size allows reusing the existing header frame.
        //We can just replace the request with the response
        char* responseData = (char*) zmq_msg_data(headerFrame);
        memcpy(responseData, responseHeader, responseSize);
        //Send the frame
        if(unlikely(zmq_msg_send(headerFrame, processorOutputSocket, flags) == -1)) {
            logMessageSendError("Response header", logger);
        }
    } else {
        //There is a request ID
        //Copy both the response and the request ID.
        zmq_msg_t msg;
        zmq_msg_init_size(&msg, requestExpectedSize + headerFrameSize - requestExpectedSize);
        char* responseData = (char*) zmq_msg_data(&msg);
        char* headerFrameData = (char*) zmq_msg_data(headerFrame);
        //Assemble: response header frame = response header + request ID
        memcpy(responseData, responseHeader, responseSize);
        memcpy(responseData + responseSize,
               headerFrameData + requestExpectedSize,
               headerFrameSize - requestExpectedSize);
        //Send the frame
        if(unlikely(zmq_msg_send(&msg, processorOutputSocket, flags) == -1)) {
            logMessageSendError("Response header", logger);
        }
    }
}