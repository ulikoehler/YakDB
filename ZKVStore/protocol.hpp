/* 
 * File:   protocol.hpp
 * Author: uli
 *
 * Created on 17. April 2013, 17:17
 */

#ifndef PROTOCOL_HPP
#define	PROTOCOL_HPP

enum RequestType {
    ServerInfo = 0x00,
    OpenTable = 0x01,
    CloseTable = 0x02,
    CompactTable = 0x03,
    Read = 0x10,
    Put = 0x20,
    Delete = 0x21
};

enum ResponseType {
    ServerInfo = 0x00,
    OpenTable = 0x01,
    CloseTable = 0x02,
    CompactTable = 0x03,
    Read = 0x10,
    PutDelete = 0x20
};

#endif	/* PROTOCOL_HPP */

