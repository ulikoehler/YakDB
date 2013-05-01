/* 
 * File:   dbclient.cpp
 * Author: uli
 * 
 * Created on 30. April 2013, 20:17
 */

#include "dbclient.hpp"
#include "macros.hpp"

DKVClient::DKVClient() noexcept : context(zctx_new()), destroyContextOnExit(true) {

}

DKVClient::DKVClient(zctx_t* ctx) noexcept : context(ctx), destroyContextOnExit(false) {

}

DKVClient::~DKVClient() noexcept {
    if (destroyContextOnExit && context) {
        zctx_destroy(&context);
    }
}

zctx_t* DKVClient::getContext() const noexcept {
    return context;
}

void DKVClient::setDestroyContextOnExit(bool param) noexcept {
    this->destroyContextOnExit = param;
}

Status DKVClient::put(uint32_t table, const std::string& key, const std::string& value) noexcept {
    PutRequest request(key, value, table);
    return execute(request);
}

Status DKVClient::put(uint32_t table, const char* key, size_t keySize, const char* value, size_t valueSize) noexcept {
    PutRequest request(key, keySize, value, valueSize, table);
    return execute(request);
}

Status DKVClient::put(uint32_t table, const char* key, const char* value) noexcept {
    PutRequest request(key, value, table);
    return execute(request);
}

Status DKVClient::execute(CountRequest& request, uint64_t& count) noexcept {
    return request.execute(socket, count);
}

Status DKVClient::execute(DeleteRequest& request) noexcept {
    return request.execute(socket);
}

Status DKVClient::execute(ExistsRequest& request, std::vector<bool>& resultRef) noexcept {
    return request.executeMultiple(socket, resultRef);
}

Status DKVClient::execute(ExistsRequest& request, bool& resultRef) noexcept {
    return request.executeSingle(socket, resultRef);
}

Status DKVClient::execute(PutRequest& request) noexcept {
    return request.execute(socket);
}

Status DKVClient::execute(ReadRequest& request, std::vector<std::string>& resultRef) noexcept {
    return request.executeMultiple(socket, resultRef);
}

Status DKVClient::execute(ReadRequest& request, std::string& resultRef) noexcept {
    return request.executeSingle(socket, resultRef);
}

std::string DKVClient::read(uint32_t table, const std::string& key) noexcept {
    ReadRequest request(key, table);
    std::string value;
    Status status = request.executeSingle(socket, value);
    if (unlikely(!status.ok())) {
        return "";
    }
    return value;
}

std::vector<std::string> DKVClient::read(uint32_t table, const std::vector<std::string>& keys) noexcept {
    ReadRequest request(keys, table);
    std::vector<std::string> value;
    Status status = request.executeMultiple(socket, value);
    //The value vector will be empty in case of errors
    return value;
}

bool DKVClient::exists(uint32_t table, const std::string& key) noexcept {
    ExistsRequest request(key, table);
    bool value = false;
    Status status = request.executeSingle(socket, value);
    //The value defaults to false in case of errors
    return value;
}

std::vector<bool> DKVClient::exists(uint32_t table, const std::vector<std::string>& keys) noexcept {
    ExistsRequest request(keys, table);
    std::vector<bool> value;
    Status status = request.executeMultiple(socket, value);
    //The value vector will be empty in case of errors
    return value;
}