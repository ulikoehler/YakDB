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
#include <czmq.h>
#include <vector>
#include <string>

//Functions for arbitrary data
zmsg_t* buildSingleReadRequest(uint32_t tableNum, const char* key, size_t keyLength) ;
zmsg_t* buildSinglePutRequest(uint32_t tableNum, const char* key, size_t keyLength, const char* value, size_t valueLength);
//Functions that work on cstrings (just wrappers using strlen)
zmsg_t* buildSingleReadRequest(uint32_t tableNum, const char* key) ;
zmsg_t* buildSinglePutRequest(uint32_t tableNum, const char* key, const char* value);
//Incremental functions

//
//Other/unsorted
//

/**
 * Extract read results into a vector
 * @param readRequest
 * @param 
 */
void parseReadRequestResult(zmsg_t* readRequest, std::vector<std::string>& dataRef);

#endif	/* CLIENT_H */

