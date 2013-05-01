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
#include "client.hpp"

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
    void connectRequestReply(const char* host) noexcept;
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
    Status execute(ReadRequest& request, std::vector<std::string>& resultRef) noexcept;
    /**
     * High-level read function.
     * Don't use this unless you don't really care about errors.
     * Use the ReadRequest class together with execute() instead to be able to
     * recognize errors.
     * @return The value for the given key, or an empty string if an error occured or the string hasn't been found
     */
    std::string read(uint32_t table, const std::string& key) noexcept;
    /**
     * High-level read function.
     * Don't use this unless you don't really care about error details.
     * Use the ReadRequest class together with execute() instead to be able to recognize errors properly.
     * @return The values for the given keys (in the same order), or an empty vector if an error occured.
     */
    std::vector<std::string> read(uint32_t table, const std::vector<std::string>& keys) noexcept;
    /**
     * High-level exists function.
     * Don't use this unless you don't really care about errors.
     * Use the ExistsRequest class together with execute() instead to be able to
     * recognize errors.
     * @return False if the given key does not exist or any error occured, true else
     */
    bool exists(uint32_t table, const std::string& key) noexcept;
    /**
     * High-level exists function.
     * Don't use this unless you don't really care about error details.
     * Use the ExistsRequest class together with execute() instead to be able to
     * recognize what error occured.
     * @return A vector that contains a bool (true if the key exists) for every given key (in the same order), or an empty vector if any error occured
     */
    std::vector<bool> exists(uint32_t table, const std::vector<std::string>& keys) noexcept;
    /**
     * Count a specific range in the database.
     * @param from The first key to count - if this is empty, the range starts at the beginning
     * @param to The first key to count - if this is empty, the range starts at the beginning
     * @return The count or -1 in case of error
     */
    int64_t count(uint32_t table, const std::string& from, const std::string& to) noexcept;
private:
    zctx_t* context;
    void* socket;
    bool destroyContextOnExit;
    uint64_t writeBatchSize;
};

#endif	/* DBCLIENT_HPP */

