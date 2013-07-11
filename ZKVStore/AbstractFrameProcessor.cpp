/* 
 * File:   AbstractFrameProcessor.cpp
 * Author: uli
 * 
 * Created on 11. Juli 2013, 03:16
 */

#include "AbstractFrameProcessor.hpp"
#include "macros.hpp"
#include "zutil.hpp"

AbstractFrameProcessor::AbstractFrameProcessor(zctx_t* ctx,
        int inputSocketType,
        int outputSocketType,
        const std::string& loggerName) :
context(ctx),
processorInputSocket(zsocket_new(ctx, inputSocketType)),
processorOutputSocket(zsocket_new(ctx, outputSocketType)),
logger(context, loggerName) {
}

AbstractFrameProcessor::~AbstractFrameProcessor() {
    zsocket_destroy(context, processorInputSocket);
    zsocket_destroy(context, processorOutputSocket);
}

bool AbstractFrameProcessor::parseUint32Frame(uint32_t& dst,
        const std::string& frameDesc,
        bool generateResponse, const char* errorResponseCode) {
    if (unlikely(!socketHasMoreFrames(processorInputSocket))) {
        std::string errstr = "Trying to read a 32-bit uint frame ("
                + frameDesc + "), but no frame was available";
        logger.warn(errstr);
        if (generateResponse) {
            sendFrame(errorResponseCode, 4, processorOutputSocket, logger, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger);
        }
        return false;
    }
    //Parse table ID, release frame immediately
    zmq_msg_t tableIdFrame;
    zmq_msg_init(&tableIdFrame);
    receiveLogError(&tableIdFrame, processorInputSocket, logger);
    if (unlikely(zmq_msg_size(&tableIdFrame) != sizeof (uint32_t))) {
        std::string errstr = "Uint32 frame ("
                + frameDesc
                + ") was expected to have a length of 4 bytes, but size is "
                + std::to_string(zmq_msg_size(&tableIdFrame)) + " bytes";
        logger.warn(errstr);
        if (generateResponse) {
            sendFrame(errorResponseCode, 4, processorOutputSocket, logger, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger);
        }
        return false;
    }
    dst = extractBinary<uint32_t>(&tableIdFrame);
    zmq_msg_close(&tableIdFrame);
    return true;
}

bool AbstractFrameProcessor::parseUint64Frame(uint64_t& valueDest,
        const std::string& frameDesc,
        bool generateResponse,
        const char* errorResponseCode) {
    if (unlikely(!socketHasMoreFrames(processorInputSocket))) {
        std::string errstr = "Trying to read 64-bit unsigned integer frame ("
                + frameDesc + "), but no frame was available";
        logger.warn(errstr);
        if (generateResponse) {
            sendFrame(errorResponseCode, 4, processorOutputSocket, logger, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger);
        }
        return false;
    }
    //Parse table ID, release frame immediately
    zmq_msg_t uint64Frame;
    zmq_msg_init(&uint64Frame);
    receiveLogError(&uint64Frame, processorInputSocket, logger);
    if (unlikely(zmq_msg_size(&uint64Frame) != sizeof (uint64_t))) {
        std::string errstr = "Uint64 frame ("
                + frameDesc + ") was expected to have a length of 8 bytes, but size is "
                + std::to_string(zmq_msg_size(&uint64Frame)) + " bytes";
        logger.warn(errstr);
        if (generateResponse) {
            sendFrame(errorResponseCode, 4, processorOutputSocket, logger, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger);
        }
        return false;
    }
    valueDest = extractBinary<uint64_t>(&uint64Frame);
    zmq_msg_close(&uint64Frame);
    return true;
}

bool AbstractFrameProcessor::parseUint64FrameOrAssumeDefault(uint64_t& valueDest,
        uint64_t defaultValue,
        const std::string& frameDesc,
        bool generateResponse,
        const char* errorResponseCode) {
    if (unlikely(!socketHasMoreFrames(processorInputSocket))) {
        std::string errstr = "Trying to read 64-bit unsigned integer frame ("
                + frameDesc + "), but no frame was available";
        logger.warn(errstr);
        if (generateResponse) {
            sendFrame(errorResponseCode, 4, processorOutputSocket, logger, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger);
        }
        return false;
    }
    //Parse table ID, release frame immediately
    zmq_msg_t uint64Frame;
    zmq_msg_init(&uint64Frame);
    size_t frameSize = zmq_msg_size(&uint64Frame);
    receiveLogError(&uint64Frame, processorInputSocket, logger);
    if (unlikely(frameSize != sizeof (uint64_t) && frameSize != 0)) {
        std::string errstr = "Uint64 frame ("
                + frameDesc + ") was expected to have a length of 8 bytes, but size is "
                + std::to_string(zmq_msg_size(&uint64Frame)) + " bytes";
        logger.warn(errstr);
        if (generateResponse) {
            sendFrame(errorResponseCode, 4, processorOutputSocket, logger, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger);
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

bool AbstractFrameProcessor::expectNextFrame(const char* errString, bool generateResponse, const char* errorResponseCode) {
    if (unlikely(!socketHasMoreFrames(processorInputSocket))) {
        logger.warn(errString);
        if (generateResponse) {
            sendFrame(errorResponseCode, 4, processorOutputSocket, logger, ZMQ_SNDMORE);
            sendFrame(errString, strlen(errString), processorOutputSocket, logger);
        }
        return false;
    }
}

bool AbstractFrameProcessor::checkLevelDBStatus(const leveldb::Status& status, const char* errString, bool generateResponse, const char* errorResponseCode) {
    if (unlikely(!status.ok())) {
        std::string statusErr = status.ToString();
        std::string completeErrorString = std::string(errString) + statusErr;
        logger.error(completeErrorString);
        if (generateResponse) {
            //Send DB error code
            sendFrame(errorResponseCode, 4, processorOutputSocket, logger, ZMQ_SNDMORE);
            sendFrame(completeErrorString, processorOutputSocket, logger);
            return false;
        }
    }
    return true;
}