/* 
 * File:   protocol.hpp
 * Author: uli
 *
 * Created on 17. April 2013, 17:17
 */

/**
 * DKV protocol header, version 1.0
 */


#ifndef PROTOCOL_HPP
#define	PROTOCOL_HPP
#include <cassert>
#include <iostream>
#include <string>
#include <zmq.h>
#include "macros.hpp"

const uint8_t magicByte = 0x31;
const uint8_t protocolVersion = 0x01;

/**
 * Checks if the magic byte and protocol version match.
 * @param data A pointer to the first byte of the packet
 * @param size The length of the dataset pointed to by the data pointer
 * @param errorDescription A string reference that is set to an error description if false is returned
 * @return true if and only if the magic byte and the protocol version matches
 */
inline static bool COLD checkProtocolVersion(const char* data, size_t size, std::string& errorDescription) {
    if (size < 3) {
        errorDescription = "Protocol error: Header frame size too small: " + std::to_string(size);
        return false;
    }
    if (data[0] != 0x31) {
        errorDescription = "Protocol error: Invalid magic byte (expecting 0x31): ";
        errorDescription += (uint8_t) data[0];
        return false;
    }
    if (data[1] != 0x01) {
        errorDescription = "Protocol error: Invalid protocol version (expecting 0x01): ";
        errorDescription += (uint8_t) data[1];
        return false;
    }
    return true;
}

enum RequestType : uint8_t {
    ServerInfoRequest = 0x00,
    OpenTableRequest = 0x01,
    CloseTableRequest = 0x02,
    CompactTableRequest = 0x03,
    TruncateTableRequest = 0x04,
    StopServerRequest = 0x05,
    ReadRequest = 0x10,
    CountRequest = 0x11,
    ExistsRequest = 0x12,
    ScanRequest = 0x13,
    ListRequest = 0x14,
    PutRequest = 0x20,
    DeleteRequest = 0x21,
    DeleteRangeRequest = 0x22,
    ForwardRangeToSocketRequest = 0x40,
    ServerSideTableSinkedMapInitializationRequest = 0x41,
    ClientSidePassiveTableMapInitializationRequest = 0x42,
    ClientDataRequest = 0x50
};

enum ResponseType : uint8_t {
    ServerInfoResponse = 0x00,
    OpenTableResponse = 0x01,
    CloseTableResponse = 0x02,
    CompactTableResponse = 0x03,
    ReadResponse = 0x10,
    CountResponse = 0x11,
    ExistsResponse = 0x11,
    PutDeleteResponse = 0x20
};

enum ServerFeatureFlag : uint64_t {
    SupportOnTheFlyTableOpen = 0x01,
    SupportPARTSYNC = 0x02,
    SupportFULLSYNC = 0x04
};

enum WriteFlag : uint8_t {
    WriteFlagPARTSYNC = 0x01,
    WriteFlagFULLSYNC = 0x02
};

enum ScanFlag : uint8_t {
    ScanFlagInvertDirection = 0x01
};

/**
 * Check if a given frame is a header frame.
 * 
 * For an error-reporting version of this function, check checkProtocolVersion()
 */
static inline bool HOT isHeaderFrame(zmq_msg_t* frame) {
    size_t size = zmq_msg_size(frame);
    if (size < 3) {
        return false;
    }
    uint8_t* data = (uint8_t*)zmq_msg_data(frame);
    return (data[0] == magicByte && data[1] == protocolVersion);
}

/**
 * @return A string that describes why the header frame is malformed, or "" if it is not malformed
 */
static inline std::string COLD describeMalformedHeaderFrame(zmq_msg_t* frame) {
    size_t size = zmq_msg_size(frame);
    if (size < 3) {
        return "Frame contains " + std::to_string(size) + " characters, but at least 3 are required";
    }
    uint8_t* data = (uint8_t*)zmq_msg_data(frame);
    if (data[0] != magicByte) {
        return "Magic byte should be 0x31 but it is (dec)" + std::to_string((int) data[0])
               + ". Frame size: " + std::to_string(size);
    }
    if (data[1] != protocolVersion) {
        return "Protocol version should be 0x01 but it is (dec)" + std::to_string((int) data[1])
               + ". Frame size: " + std::to_string(size);
    }
    return "[Unknown header frame problem. This is considered a bug.]";
}

static inline RequestType getRequestType(zmq_msg_t* frame) {
    assert(zmq_msg_size(frame) >= 3);
    return (RequestType) ((char*)zmq_msg_data(frame))[2];
}

static inline uint8_t getWriteFlags(zmq_msg_t* frame) {
    //Write flags are optional and default to 0x00
    return (zmq_msg_size(frame) >= 4 ? ((uint8_t*)zmq_msg_data(frame))[3] : 0x00);
}

static inline bool isPartsync(uint8_t writeFlags) {
    return (writeFlags & WriteFlagPARTSYNC);
}

static inline bool isFullsync(uint8_t writeFlags) {
    return (writeFlags & WriteFlagFULLSYNC);
}

#endif	/* PROTOCOL_HPP */

