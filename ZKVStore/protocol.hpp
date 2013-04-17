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
#include <string>

/**
 * Checks if the magic byte and protocol version match.
 * @param data A pointer to the first byte of the packet
 * @param size The length of the dataset pointed to by the data pointer
 * @param errorDescription A string reference that is set to an error description if false is returned
 * @return true if and only if the magic byte and the protocol version matches
 */
inline static bool checkProtocolVersion(const char* data, size_t size, std::string& errorDescription) {
    if (size < 3) {
        errorDescription = "Protocol error: Header frame size too small: " + size;
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

enum RequestType {
    ServerInfo = 0x00,
    OpenTable = 0x01,
    CloseTable = 0x02,
    CompactTable = 0x03,
    Read = 0x10,
    Count = 0x11,
    Put = 0x20,
    Delete = 0x21
};

enum ResponseType {
    ServerInfo = 0x00,
    OpenTable = 0x01,
    CloseTable = 0x02,
    CompactTable = 0x03,
    Read = 0x10,
    Count = 0x11,
    PutDelete = 0x20
};

#endif	/* PROTOCOL_HPP */

