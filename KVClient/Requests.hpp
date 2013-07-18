/* 
 * File:   Requests.hpp
 * Author: uli
 *
 * Created on 18. Juli 2013, 21:55
 */

#ifndef REQUESTS_HPP
#define	REQUESTS_HPP
#include <string>

namespace ZettaCrunchDB {
    namespace MetaRequests {

        /**
         * An update request that writes 
         */
        class ServerInfoRequest {
        public:
            /**
             * Send a server info request
             */
            static void sendRequest(void* socket);
            static int receiveFeatureFlags(void* socket, uint64_t& flags);
            static int receiveVersion(void* socket, std::string& serverVersion);
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
         * An update request that writes 
         */
//        class DeleteRequest {
//        public:
//            static const uint8_t PARTSYNC = 0x01;
//            static const uint8_t FULLSYNC = 0x02;
//            /**
//             * Send the header for the current request type
//             * @param socket
//             * @param table The table number to write to
//             * @return 0 on success, errno else
//             */
//            static int sendHeader(void* socket, uint32_t table, uint8_t flags = 0x00);
//            /**
//             * Write a single key-value pair.
//             * If this is not the last key-value pair you want to send,
//             * you must set the 'last' parameter to false!
//             * @param socket
//             * @param key The key to writes
//             * @param value The value to write
//             * @param last Whether this is the last key to send. Determines ZMQ_SNDMORE flag
//             * @return 0 on success, errno else
//             */
//            static int sendKey(void* socket,
//                    const std::string& key,
//                    bool last = false);
//            static int sendKey(void* socket,
//                    const char* key,
//                    bool last = false);
//            static int sendKey(void* socket,
//                    const char* key,
//                    size_t keyLength,
//                    bool last = false);
//        };        
    }

}

#endif	/* REQUESTS_HPP */

