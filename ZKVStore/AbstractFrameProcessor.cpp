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
    if (unlikely(!receiveMsgHandleError(&tableIdFrame, frameDesc.c_str(), errorResponseCode, generateResponse))) {
        return false;
    }
    if (unlikely(zmq_msg_size(&tableIdFrame) != sizeof (uint32_t))) {
        std::string errstr = "uint32 frame ("
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
    if (unlikely(!receiveMsgHandleError(&uint64Frame, frameDesc.c_str(), errorResponseCode, generateResponse))) {
        return false;
    }
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
    if (unlikely(!receiveMsgHandleError(&uint64Frame, frameDesc.c_str(), errorResponseCode, generateResponse))) {
        return false;
    }
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
    return true;
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

bool AbstractFrameProcessor::parseLevelDBRange(leveldb::Slice** startSlice,
        leveldb::Slice** endSlice,
        const char* errName,
        const char* errorResponse,
        bool generateResponse) {
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
    size_t rangeStartFrameSize = zmq_msg_size(&rangeStartFrame);
    size_t rangeEndFrameSize = zmq_msg_size(&rangeEndFrame);
    if (rangeStartFrameSize == 0) {
        startSlice = nullptr;
    } else {
        *startSlice = new leveldb::Slice((char*) zmq_msg_data(&rangeStartFrame), rangeStartFrameSize);
    }
    if (rangeEndFrameSize == 0) {
        endSlice = nullptr;
    } else {
        *endSlice = new leveldb::Slice((char*) zmq_msg_data(&rangeEndFrame), rangeEndFrameSize);
    }
    //Cleanup
    zmq_msg_close(&rangeStartFrame);
    zmq_msg_close(&rangeEndFrame);
    return true;
}

bool AbstractFrameProcessor::receiveMsgHandleError(zmq_msg_t* msg,
        const char* errName,
        const char* errorResponse,
        bool generateResponse) {
    if (unlikely(zmq_msg_recv(msg, processorInputSocket, 0) == -1)) {
        std::string errstr = "Error while receiving message part: "
                + std::string(zmq_strerror(zmq_errno()))
                + " in " + std::string(errName);
        logger.warn(errstr);
        if (generateResponse) {
            sendFrame(errorResponse, 4, processorOutputSocket, logger, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger);
        }
        return false;
    }
    return true;
}

bool AbstractFrameProcessor::sendMsgHandleError(zmq_msg_t* msg,
        int flags,
        const char* errName,
        const char* errorResponse,
        bool generateResponse) {
    if (unlikely(zmq_msg_send(msg, processorOutputSocket, flags) == -1)) {
        std::string errstr = "Error while sending message part: "
                + std::string(zmq_strerror(zmq_errno()))
                + " in " + std::string(errName);
        logger.warn(errstr);
        if (generateResponse) {
            sendFrame(errorResponse, 4, processorOutputSocket, logger, ZMQ_SNDMORE);
            sendFrame(errstr, processorOutputSocket, logger);
        }
        return false;
    }
    return true;
}