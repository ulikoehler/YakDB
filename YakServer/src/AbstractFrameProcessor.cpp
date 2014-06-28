/*
 * File:   AbstractFrameProcessor.cpp
 * Author: uli
 *
 * Created on 11. Juli 2013, 03:16
 */

#include <zmq.h>
#include <limits>
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
logger(context, loggerName),
errorResponse(nullptr),
requestExpectedSize(std::numeric_limits<size_t>::max() /* As invalid as possible */) {
}

AbstractFrameProcessor::AbstractFrameProcessor(void* ctx,
        int socketType,
        const std::string& loggerName) :
context(ctx),
processorInputSocket(zmq_socket(ctx, socketType)),
processorOutputSocket(processorInputSocket),
logger(context, loggerName),
errorResponse(nullptr),
requestExpectedSize(std::numeric_limits<size_t>::max() /* As invalid as possible */) {
}

AbstractFrameProcessor::~AbstractFrameProcessor() {
    zmq_close(processorInputSocket);
    zmq_close(processorOutputSocket);
}

bool COLD AbstractFrameProcessor::sendErrorResponseHeader(int flags) {
    return sendResponseHeader(errorResponse, flags, 4);
}

bool AbstractFrameProcessor::parseUint32Frame(uint32_t& dst,
        const char* frameDesc,
        bool generateResponse) {
    return parseBinaryFrame(&dst, sizeof(uint32_t), frameDesc, generateResponse);
}

bool AbstractFrameProcessor::parseUint8Frame(uint8_t& dst,
        const char* frameDesc,
        bool generateResponse) {
    return parseBinaryFrame(&dst, sizeof(uint8_t), frameDesc, generateResponse);
}

bool AbstractFrameProcessor::parseBinaryFrame(void* dst,
        size_t size,
        const char* frameDesc,
        bool generateResponse,
        bool acceptFirstFrame,
        void* defaultValue) {
    if (!acceptFirstFrame && unlikely(!socketHasMoreFrames(processorInputSocket))) {
        std::string errstr = "Trying to read a " +
                std::to_string(size) + "-byte frame ("
                + std::string(frameDesc) + "), but no frame was available";
        logger.warn(errstr);
        if (generateResponse) {
            sendErrorResponseHeader(ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, frameDesc);
        }
        return false;
    }
    //Parse table ID, release frame immediately
    zmq_msg_t valueFrame;
    zmq_msg_init(&valueFrame);
    if (unlikely(!receiveMsgHandleError(&valueFrame, frameDesc, generateResponse))) {
        return false;
    }
    size_t frameSize = zmq_msg_size(&valueFrame);
    //If default value is available and frame is 0-sized, use default value
    if(defaultValue != nullptr && frameSize == 0) {
        memcpy(dst, defaultValue, size);
        return true;
    }
    //If we haven't returned yet, we expect a regularly-sized frame
    if (unlikely(frameSize != size)) {
        std::string errstr = "Frame ("
                + std::string(frameDesc)
                + ") was expected to have a length of "
                + std::to_string(size) + " byte(s), but actual size is "
                + std::to_string(zmq_msg_size(&valueFrame)) + " byte(s)";
        logger.warn(errstr);
        if (generateResponse) {
            sendErrorResponseHeader(ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, frameDesc);
        }
        return false;
    }
    //Copy to destination
    memcpy(dst, zmq_msg_data(&valueFrame), size);
    zmq_msg_close(&valueFrame);
    return true;
}

bool AbstractFrameProcessor::parseUint64Frame(uint64_t& dst,
        const char* frameDesc,
        bool generateResponse) {
    return parseBinaryFrame(&dst, sizeof(uint64_t), frameDesc, generateResponse);
}

bool AbstractFrameProcessor::parseUint64FrameOrAssumeDefault(uint64_t& dst,
        uint64_t defaultValue,
        const char* frameDesc,
        bool generateResponse) {
    return parseBinaryFrame(&dst, sizeof(uint64_t), frameDesc, generateResponse, &defaultValue);
}

bool AbstractFrameProcessor::parseUint32FrameOrAssumeDefault(uint32_t& dst,
        uint32_t defaultValue,
        const char* frameDesc,
        bool generateResponse) {
    return parseBinaryFrame(&dst, sizeof(uint32_t), frameDesc, generateResponse, &defaultValue);
}

bool AbstractFrameProcessor::expectNextFrame(const char* errString,
        bool generateResponse) {
    if (unlikely(!socketHasMoreFrames(processorInputSocket))) {
        logger.warn(errString);
        if (generateResponse) {
            sendErrorResponseHeader(ZMQ_SNDMORE);
            sendFrame(errString, strlen(errString), processorOutputSocket, logger, errString);
        }
        return false;
    }
    return true;
}

bool AbstractFrameProcessor::checkLevelDBStatus(const rocksdb::Status& status,
    const char* errMsg,
    bool generateResponse) {
    if (unlikely(!status.ok() && !status.IsNotFound())) {
        std::string statusErr = status.ToString();
        std::string completeErrorString = std::string(errMsg) + statusErr;
        logger.error(completeErrorString);
        if (generateResponse) {
            //Send DB error code
            sendErrorResponseHeader(ZMQ_SNDMORE);
            sendFrame(completeErrorString, processorOutputSocket, logger, errMsg);
            return false;
        }
    }
    return true;
}

bool AbstractFrameProcessor::parseRangeFrames(std::string& startSlice,
        std::string& endSlice,
        const char* errName,
        bool generateResponse) {
    //Parse the start/end frame
    zmq_msg_t rangeStartFrame, rangeEndFrame;
    zmq_msg_init(&rangeStartFrame);
    if (unlikely(!receiveMsgHandleError(&rangeStartFrame, errName, generateResponse))) {
        return false;
    }
    if (!expectNextFrame(("Only range start frame found in '"
            + std::string(errName) + "', range end frame missing").c_str(),
            generateResponse)) {
        zmq_msg_close(&rangeStartFrame);
        return false;
    }
    zmq_msg_init(&rangeEndFrame);
    if (unlikely(!receiveMsgHandleError(&rangeEndFrame, errName, generateResponse))) {
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
        bool generateResponse) {
    if (unlikely(zmq_msg_recv(msg, processorInputSocket, 0) == -1)) {
        std::string errstr = "Error while receiving message part: "
                + std::string(zmq_strerror(zmq_errno()))
                + " in " + std::string(errName);
        logger.warn(errstr);
        if (generateResponse) {
            sendErrorResponseHeader(ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, errName);
        }
        return false;
    }
    return true;
}

bool AbstractFrameProcessor::receiveStringFrame(std::string& frame,
        const char* errName,
        bool generateResponse) {
    zmq_msg_t msg;
    zmq_msg_init(&msg);
    bool rc = receiveMsgHandleError(&msg, errName, generateResponse);
    if(unlikely(!rc)) {
        return false;
    }
    frame = std::string((char*)zmq_msg_data(&msg), zmq_msg_size(&msg));
    return true;
}

bool AbstractFrameProcessor::receiveMap(std::map<std::string, std::string>& target,
        const char* errName,
        bool generateResponse) {
    //Maybe there are no key/value frames at all!?!?
    if(!socketHasMoreFrames(processorInputSocket)) {
        return true;
    }
    //Receive frame pairs until nothing is left
    std::string key;
    std::string value;
    while(true) {
        if(!receiveStringFrame(key, errName, generateResponse)) {
            return false;
        }
        //If there is a trailing key frame (with no value frame), d
        if(!expectNextFrame("Expected value frame while receiving alternating key/value frame map", false)) {
            return true;
        }
        if(!receiveStringFrame(value, errName, generateResponse)) {
            return false;
        }
        //Insert into result map
        target[key] = value;
        //Stop if there is no frame left
        if (!socketHasMoreFrames(processorInputSocket)) {
            return true;
        }
    }
}

bool AbstractFrameProcessor::sendMap(std::map<std::string, std::string>& values,
                                     const char* errName,
                                     bool generateResponse,
                                     bool more) {
    /**
     * We need to ensure the last frame is set
     * However, inside the iterating loop we don't know if we're currently looking
     * at the last k/v pair. Therefore we need to queue the value.
     * The algorithm for this is similar to the one used in scan-response generation.
     */
    bool haveFrameLeft = false;
    std::string queuedValue;
    for(auto p : values) {
        if(haveFrameLeft) {
            //We now know that the last k/v pair was not the last one
            if(!sendMsgHandleError(queuedValue, ZMQ_SNDMORE, errName, generateResponse)) {
                return false;
            }
        }
        //After any key, there is ALWAYS a frame (the corresponding value)
        if(!sendMsgHandleError(p.first, ZMQ_SNDMORE, errName, generateResponse)) {
            return false;
        }
        //Queue the value
        haveFrameLeft = true;
        queuedValue = std::move(p.second); //rvalue assignment *might* avoid copying
    }
    /*
     * We might have one last queued frame. Send it using the appropriate flags.
     * It will cause application errors if haveFrameLeft = false
     *  because the last frame wasn't sent with ZMQ_SNDMORE. As for empty maps
     *  this function sends no frames at all, there is no way we can avoid this
     *  issue here.
     *
     * If the more flag is set to true, this is not an error because the caller will
     *  send more frames. Else, we will fail an assertion to avoid hard-to-debug
     *  intermixed messages. In case the assertion fails, ensure you handle empty
     *  maps differently.
     */
    if(likely(haveFrameLeft)) {
        if(!sendMsgHandleError(queuedValue, (more ? ZMQ_SNDMORE : 0), errName, generateResponse)) {
            return false;
        }
    } else {
        assert(more); //If this fails, read the block comment above;
    }
}

bool AbstractFrameProcessor::sendMsgHandleError(const std::string& str,
        int flags,
        const char* errName,
        bool generateResponse) {
    zmq_msg_t msg;
    if (unlikely(zmq_msg_init_size(&msg, str.size()) == -1)) {
        std::string errstr = "Error while initializing message of size: "
                + std::to_string(str.size()) + ": "
                + std::string(zmq_strerror(zmq_errno()));
        logger.error(errstr);
        return false;
    }
    memcpy(zmq_msg_data(&msg), str.data(), str.size());
    return sendMsgHandleError(&msg, flags, errName, generateResponse);
}

bool AbstractFrameProcessor::sendMsgHandleError(zmq_msg_t* msg,
        int flags,
        const char* errName,
        bool generateResponse) {
    if (unlikely(zmq_msg_send(msg, processorOutputSocket, flags) == -1)) {
        std::string errstr = "Error while sending message part: "
                + std::string(zmq_strerror(zmq_errno()))
                + " in " + std::string(errName);
        logger.warn(errstr);
        if (generateResponse) {
            sendErrorResponseHeader(ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, errName);
        }
        return false;
    }
    return true;
}

void AbstractFrameProcessor::disposeRemainingMsgParts() {
    int numErrors = 0;
    const int errorLimit = 5; //After this number of errors the function exits to prevent infinite loops
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
            bool generateResponse) {
    size_t actualMsgSize = zmq_msg_size(msg);
    if(unlikely(actualMsgSize != expectedSize)) {
        std::string errstr = "Error while checking ZMQ frame length of "
                + std::string(errName) + ": Expected length to be equal to "
                + std::to_string(expectedSize) + " bytes but actual length was "
                + std::to_string(actualMsgSize)
                + " in " + std::string(errName);
        logger.warn(errstr);
        if(generateResponse) {
            sendErrorResponseHeader(ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger, errName);
        }
        return false;
    }
    return true;
}

bool AbstractFrameProcessor::expectMinimumFrameSize(zmq_msg_t* msg,
            size_t expectedSize,
            const char* errName,
            bool generateResponse) {
    size_t actualMsgSize = zmq_msg_size(msg);
    if(unlikely(actualMsgSize < expectedSize)) {
        std::string errstr = "Error while checking ZMQ frame length of "
                + std::string(errName) + ": Expected length to be >= "
                + std::to_string(expectedSize) + " bytes but actual length was "
                + std::to_string(actualMsgSize)
                + " in " + std::string(errName);
        logger.warn(errstr);
        if(generateResponse) {
            sendErrorResponseHeader(ZMQ_SNDMORE);
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

bool AbstractFrameProcessor::sendResponseHeader(const char* responseHeader,
                                                int flags,
                                                size_t responseSize) {
    return AbstractFrameProcessor::sendResponseHeader(processorOutputSocket,
            logger,
            &headerFrame,
            responseHeader,
            flags,
            requestExpectedSize,
            responseSize);
}

//This function is static inside AbstractFrameProcessor!
bool AbstractFrameProcessor::sendResponseHeader(void* socket,
    Logger& logger,
    zmq_msg_t* headerFrame,
    const char* responseHeader,
    int flags,
    size_t requestExpectedSize,
    size_t responseSize) {
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
        if(headerFrame != nullptr) {
            zmq_msg_close(headerFrame);
        }
        if(unlikely(sendFrame(responseHeader, responseSize, socket,
            logger, "Response header", flags) == -1)) {
            return false;
        }
    } else if (requestExpectedSize == responseSize) {
        //The size allows reusing the existing header frame.
        //We can just replace the request with the response
        char* responseData = (char*) zmq_msg_data(headerFrame);
        memcpy(responseData, responseHeader, responseSize);
        //Send the frame
        if(unlikely(zmq_msg_send(headerFrame, socket, flags) == -1)) {
            logMessageSendError("Response header", logger);
            return false;
        }
    } else {
        //There is a request ID
        //Copy both the response and the request ID.
        zmq_msg_t msg;
        zmq_msg_init_size(&msg, requestExpectedSize + headerFrameSize - requestExpectedSize + 1);
        char* responseData = (char*) zmq_msg_data(&msg);
        char* headerFrameData = (char*) zmq_msg_data(headerFrame);
        //Assemble: response header frame = response header + request ID
        memcpy(responseData, responseHeader, responseSize);
        memcpy(responseData + responseSize,
               headerFrameData + requestExpectedSize,
               headerFrameSize - requestExpectedSize + 1);
        //Send the frame
        if(unlikely(zmq_msg_send(&msg, socket, flags) == -1)) {
            logMessageSendError("Response header", logger);
            if(headerFrame != nullptr) {
                zmq_msg_close(headerFrame);
            }
            return false;
        }
        if(headerFrame != nullptr) {
            zmq_msg_close(headerFrame);
        }
    }
    return true;
}

bool AbstractFrameProcessor::bindInputSocket(const char* target) {
    return bindSocket(processorInputSocket, target);

}

bool AbstractFrameProcessor::bindOutputSocket(const char* target) {
    return bindSocket(processorOutputSocket, target);
}

bool AbstractFrameProcessor::connectInputSocket(const char* target) {
    return connectSocket(processorInputSocket, target);
}

bool AbstractFrameProcessor::connectOutputSocket(const char* target) {
    return connectSocket(processorOutputSocket, target);
}

bool AbstractFrameProcessor::connectSocket(void* sock, const char* target) {
    if(unlikely(zmq_connect(sock, target) == -1)) {
        std::string errstr = "Error while connecting socket: "
            + std::string(zmq_strerror(zmq_errno()));
        logger.error(errstr);
        return false;
    }
    return true;
}

bool AbstractFrameProcessor::bindSocket(void* sock, const char* target) {
    if(unlikely(zmq_bind(sock, target) == -1)) {
        std::string errstr = "Error while binding socket: "
            + std::string(zmq_strerror(zmq_errno()));
        logger.error(errstr);
        return false;
    }
    return true;
}
