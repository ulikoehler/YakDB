/* 
 * File:   dbclient.hpp
 * Author: uli
 *
 * Created on 30. April 2013, 20:17
 */

#ifndef DBCLIENT_HPP
#define	DBCLIENT_HPP
#include <czmq.h>
#include <exception>
#include <string>
#include <vector>
#include "Status.hpp"
#include "client.h"

class KVDBException : public exception {
public:

    inline KVDBException(const std::string) noexcept : msg(m) {
    }

    inline const char* what() {
        return msg.c_str();
    }
private:
    std::string msg;
};

/**
 * Macro that checks a Status object. If the status doesn't indicate success, it throws an exception.
 * Useful if you want to shrink your codebase or you don't want to do too much error handling.
 * 
 * Keep in mind, however, that using this macro might have a negative impact on your performance, depending on how your
 * compiler handles exceptions.
 * 
 * Note that some functions throw exceptions independently of this macro
 */
#define checkStatus(expr) {Status macroStatus1415/*Hopefully avoids collisions*/ = expr;if(!status.ok()) {throw new KVDBException(status.getErrorMessage());}}

/**
 * High-level client to the distributed key-value store.
 * 
 * This interface automatically handles write batching.
 */
class DKVClient {
public:
    /**
     * Creates a new DKV client using a new ZeroMQ context that is automatically
     * destroyed when this instance is destructed
     */
    DKVClient() noexcept;
    /**
     * Creates a new DKV client reusing an existing ZeroMQ context.
     * The context will not be destroyed even if the DKVClient instance is
     * destructed
     */
    DKVClient(zctx_t* ctx) noexcept;
    /**
     * Destructor that only destroyess the underlying context if the corresponding option is set
     */
    ~DKVClient() noexcept;
    /**
     * Connect to a request/reply host.
     * This allows both read and write access, but write requests need to wait for an acknowledge reply.
     * Therefore the effective (especially burst) transfer rate is a bit lower
     * 
     * @param host
     * @param port
     */
    void connect(const char* host, uint32_t port) noexcept;
    /**
     * Get the current context in use by this instance.
     * @return 
     */
    zctx_t* getContext() const noexcept;
    /**
     * @param destroyContextOnExit If this is set to true, the context will be destroyed in the destructor
     */
    void setDestroyContextOnExit(bool destroyContextOnExit) noexcept;
    //
    //////////////////////////
    ///Read-write functions///
    //////////////////////////
    //
    // Note that these functions always read/write a single value.
    // This is extremely inefficient. Unless you don't care about performance,
    // You should always use Request classes like UpdateRequest that can handle multiple
    // reads/writes at once.
    //
    Status put(uint32_t table, const std::string& key, const std::string& value) noexcept;
    Status put(uint32_t table, const char* key, size_t keySize, const char* value, size_t valueSize) noexcept;
    Status put(uint32_t table, const char* key, const char* value) noexcept;

    Status execute(CountRequest& request, uint64_t& count) noexcept;
    Status execute(DeleteRequest& request) noexcept;
    Status execute(ExistsRequest& request, std::vector<bool>& resultRef) noexcept;
    Status execute(ExistsRequest& request, bool& resultRef) noexcept;
    Status execute(PutRequest& request) noexcept;
    Status execute(ReadRequest& request, std::string& resultRef) noexcept;
    //High-level read functions that throw an exception if something went wrong
    //...don't use if you're crazy about performance
    std::string read(uint32_t table, const std::string& key);
    std::vector<std::string> read(uint32_t table, const std::vector<std::string>& keys);
    bool exists(uint32_t table, const std::string& key);
    /**
     * Count a specific range in the database.
     * @param from The first key to count - if this is empty, the range starts at the beginning
     */
    uint64_t count(const std::string& from, const std::string& to);
private:
    zctx_t* context;
    void* socket;
    bool destroyContextOnExit;
    uint64_t writeBatchSize;
};

#endif	/* DBCLIENT_HPP */

