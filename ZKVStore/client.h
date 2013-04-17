/* 
 * File:   client.h
 * Author: uli
 * 
 * Provides DKV client functionality
 *
 * Created on 17. April 2013, 19:08
 */

#ifndef CLIENT_H
#define	CLIENT_H

zmsg_t* buildSingleReadRequest(uint32_t tableNum, const char* key, size_t keyLength) ;
zmsg_t* buildSinglePutRequest(uint32_t tableNum, const char* key, size_t keyLength, const char* value, size_t valueLength);

#endif	/* CLIENT_H */

