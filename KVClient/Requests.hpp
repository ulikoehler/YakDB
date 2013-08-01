/* 
 * File:   Requests.hpp
 * Author: uli
 *
 * Created on 18. Juli 2013, 21:55
 */

#ifndef REQUESTS_HPP
#define	REQUESTS_HPP
#include <string>
#include <cstdint>

namespace ZettaCrunchDB {
    namespace MetaRequests {

        
    }

    namespace ReadRequests {

        /**
         * A request to read one or multiple keys.
         * In order to correctly write the request, write the header first,
         * then send an arbitrary amount of keys. Ensure that the last key
         * is sent with the last argument set to true.
         * 
         * Then, receive the header and, if no error occured, receive the values
         * (in the same order as the keys), until the return code of
         * receiveResponseValue() is 1.
         */
        class ReadRequest {
            static int sendHeader(void* socket, uint32_t table);
            static int sendKey(void* socket,
                    const std::string& key,
                    bool last = false);
            static int sendKey(void* socket,
                    const char* key,
                    bool last = false);
            static int sendKey(void* socket,
                    const char* key,
                    size_t keyLength,
                    bool last = false);
            static int receiveResponseHeader(void* socket, std::string& errorMessage);
            /**
             * Receive the next response value.
             * @return -1 on error, 0 == (success, there are more keys to retrieve), 1 == (success, no more keys to retrieve)
             */
            static int receiveResponseValue(void* socket, std::string& target);
        };
        
        /**
         * A request to count a range of keys
         */
        class CountRequest {
            static int sendHeader(void* socket, uint32_t table);
            static int sendKey(void* socket,
                    const std::string& key,
                    bool last = false);
            static int sendKey(void* socket,
                    const char* key,
                    bool last = false);
            static int sendKey(void* socket,
                    const char* key,
                    size_t keyLength,
                    bool last = false);
            static int receiveResponseHeader(void* socket, std::string& errorMessage);
            /**
             * Receive the next response value.
             * @return -1 on error, 0 == (success, there are more keys to retrieve), 1 == (success, no more keys to retrieve)
             */
            static int receiveResponseValue(void* socket, std::string& target);
        };
        /**
         * A request to check if one or multiple keys exist
         */
        class ExistsRequest {
            static int sendHeader(void* socket, uint32_t table);
            static int sendKey(void* socket,
                    const std::string& key,
                    bool last = false);
            static int sendKey(void* socket,
                    const char* key,
                    bool last = false);
            static int sendKey(void* socket,
                    const char* key,
                    size_t keyLength,
                    bool last = false);
            static int receiveResponseHeader(void* socket, std::string& errorMessage);
            /**
             * Receive the next response value.
             * @return -1 on error, 0 == (success, there are more keys to retrieve), 1 == (success, no more keys to retrieve)
             */
            static int receiveResponseValue(void* socket, std::string& target);
        };
        /**
         * A request to scan a range of keys and return all key-value-pairs
         * in the given request at once.
         */
        class ScanRequest {
            
        };
        /**
         * A request to scan up to a limited amount of key/value pairs from a start key
         */
        class LimitedScanRequest {
            
        };
    }

    namespace WriteRequests {

        /**
         * An update request that writes key-value pairs to the database.
         */
        class PutRequest {
        public:
            static const uint8_t PARTSYNC = 0x01;
            static const uint8_t FULLSYNC = 0x02;
            /**
             * Send the header for the current request type
             * @param socket
             * @param table The table number to write to
             * @return 0 on success, errno else
             */
            static int sendHeader(void* socket, uint32_t table, uint8_t flags = 0x00);
            /**
             * Write a single key-value pair.
             * If this is not the last key-value pair you want to send,
             * you must set the 'last' parameter to false!
             * @param socket
             * @param key The key to writes
             * @param value The value to write
             * @param last Whether this is the last key to send. Determines ZMQ_SNDMORE flag
             * @return 0 on success, errno else
             */
            static int sendKeyValue(void* socket,
                    const std::string& key,
                    const std::string& value,
                    bool last = false);
            static int sendKeyValue(void* socket,
                    const char* key,
                    const char* value,
                    bool last = false);
            static int sendKeyValue(void* socket,
                    const char* key,
                    size_t keyLength,
                    const char* value,
                    size_t valueLength,
                    bool last = false);
        };

        /**
         * A delete request that writes 
         */
        class DeleteRequest {
        public:
            static const uint8_t PARTSYNC = 0x01;
            static const uint8_t FULLSYNC = 0x02;
            /**
             * Send the header for the current request type
             * @param socket
             * @param table The table number to write to
             * @return 0 on success, errno else
             */
            static int sendHeader(void* socket, uint32_t table, uint8_t flags = 0x00);
            /**
             * Write a single key-value pair.
             * If this is not the last key-value pair you want to send,
             * you must set the 'last' parameter to false!
             * @param socket
             * @param key The key to writes
             * @param value The value to write
             * @param last Whether this is the last key to send. Determines ZMQ_SNDMORE flag
             * @return 0 on success, errno else
             */
            static int sendKey(void* socket,
                    const std::string& key,
                    bool last = false);
            static int sendKey(void* socket,
                    const char* key,
                    bool last = false);
            static int sendKey(void* socket,
                    const char* key,
                    size_t keyLength,
                    bool last = false);
        };        
    }

}

#endif	/* REQUESTS_HPP */

