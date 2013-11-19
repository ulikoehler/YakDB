/* 
 * File:   dbclient.cpp
 * Author: uli
 * 
 * Created on 30. April 2013, 20:17
 */

#include "yakclient/YakClient.hpp"
#include "yakclient/ReadRequests.hpp"
#include "yakclient/WriteRequests.hpp"
#include "yakclient/MetaRequests.hpp"


YakClient::YakClient() : context(zmq_ctx_new()), destroyContextOnExit(true), socketType(SocketType::None) {
    //Nothing to be done here
}

YakClient::YakClient(const char* endpoint) : context(zmq_ctx_new()), destroyContextOnExit(true), socketType(SocketType::None) {
    connectRequestReply(endpoint);
}

YakClient::YakClient(void* ctx) : context(ctx), destroyContextOnExit(false) {
    //Nothing to be done here
}

YakClient::~YakClient() {
    if (socket) {
        zmq_close(socket);
    }
    if (destroyContextOnExit && context) {
        zmq_ctx_destroy(context);
    }
}

void YakClient::connectRequestReply(const char* url) {
    socket = zmq_socket(context, ZMQ_REQ);
    zmq_connect(socket, url);
    socketType = SocketType::ReqRep;
}

void YakClient::connectPushPull(const char* url) {
    socket = zmq_socket(context, ZMQ_PUSH);
    zmq_connect(socket, url);
    socketType = SocketType::PushPull;
}

int YakClient::put(uint32_t table, const std::string& key, const std::string& value, uint8_t flags) {
    if (PutRequest::sendHeader(socket, table, flags) == -1) {
        return -1;
    }
    if (PutRequest::sendKeyValue(socket, key, value, true) == -1) {
        return -1;
    }
    if (isRequestReply()) {
        std::string errorString;
        return PutRequest::receiveResponse(socket, errorString);
    }
    return 0;
}

int YakClient::put(uint32_t table, const char* key, size_t keySize, const char* value, size_t valueSize, uint8_t flags) {
    if (PutRequest::sendHeader(socket, table, flags) == -1) {
        return -1;
    }
    if (PutRequest::sendKeyValue(socket, key, keySize, value, valueSize, true) == -1) {
        return -1;
    }
    if (isRequestReply()) {
        std::string errorString;
        return PutRequest::receiveResponse(socket, errorString);
    }
    return 0;
}

int YakClient::put(uint32_t table, const char* key, const char* value, uint8_t flags) {
    if (PutRequest::sendHeader(socket, table, flags) == -1) {
        return -1;
    }
    if (PutRequest::sendKeyValue(socket, key, value, true) == -1) {
        return -1;
    }
    if (isRequestReply()) {
        std::string errorString;
        return PutRequest::receiveResponse(socket, errorString);
    }
    return 0;
}

int YakClient::read(uint32_t table, const std::string& key, std::string& value) {
    if(!isRequestReply()) {
        return -1;
    }
    //Send the request
    if(ReadRequest::sendHeader(socket, table) == -1) {
        return -2;
    }
    if(ReadRequest::sendKey(socket, key, true) == -1) {
        return -2;
    }
    //Receive the response
    std::string errorMessage;
    if(ReadRequest::receiveResponseHeader(socket, errorMessage) == -1) {
        return -3;
    }
    if(ReadRequest::receiveResponseValue(socket, value) == -1) {
        return -4;
    }
    return 0;
}

int YakClient::read(uint32_t table, const char* key, std::string& value) {
    if(!isRequestReply()) {
        return -1;
    }
    //Send the request
    if(ReadRequest::sendHeader(socket, table) == -1) {
        return -2;
    }
    if(ReadRequest::sendKey(socket, key, true) == -1) {
        return -3;
    }
    //Receive the response
    std::string errorMessage;
    if(ReadRequest::receiveResponseHeader(socket, errorMessage) == -1) {
        return -4;
    }
    if(ReadRequest::receiveResponseValue(socket, value) == -1) {
        return -5;
    }
    return 0;
}

int YakClient::read(uint32_t table, const std::vector<std::string>& keys, std::vector<std::string>& values) {
    if(!isRequestReply()) {
        return -1;
    }
    if(keys.empty()) {
        return -2;
    }
    //Send the request
    if(ReadRequest::sendHeader(socket, table) == -1) {
        return -3;
    }
    //Send the first n-1 keys first, then the last key without ZMQ_SNDMORE
    for(int i = 0; i < keys.size() - 1; i++) {
        if(ReadRequest::sendKey(socket, keys[i], false) == -1) {
            return -4;
        }
    }
    if(ReadRequest::sendKey(socket, keys[keys.size()-1], true) == -1) {
        return -5;
    }
    //Receive the response
    std::string errorMessage;
    if(ReadRequest::receiveResponseHeader(socket, errorMessage) == -1) {
        return -6;
    }
    //Receive the values one-by-one
    for(int i = 0; i < keys.size(); i++) {
        std::string value;    
        if(ReadRequest::receiveResponseValue(socket, value) == -1) {
            return -7;
        }
        values.push_back(value);
    }
    return 0;
}

int YakClient::exists(uint32_t table, const std::string& key) {
    if(!isRequestReply()) {
        return -1;
    }
    //Send the request
    if(ExistsRequest::sendHeader(socket, table)) {
        return -2;
    }
    if(ExistsRequest::sendKey(socket, key, true) == -1) {
        return -3;
    }
    //Receive the response
    std::string errorMessage;
    if(ExistsRequest::receiveResponseHeader(socket, errorMessage) == -1) {
        return -4;
    }
    ExistsRequest::receiveResponseValue(socket);
}

int YakClient::exists(uint32_t table, const std::vector<std::string>& keys, std::vector<bool>& result) {

}

int64_t YakClient::count(uint32_t table, const std::string& from, const std::string& to) {

}
