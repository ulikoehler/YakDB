/* 
 * File:   dbclient.hpp
 * Author: uli
 *
 * Created on 30. April 2013, 20:17
 */

#ifndef DBCLIENT_HPP
#define DBCLIENT_HPP

#include <zmq.h>
#include <exception>
#include <string>
#include <vector>

/**
 * The socket type a DKVClient class is currently connected to.
 * 
 * Depending on the socket type, different features are supported,
 * e.g. read requests are only supported on the ReqRep socket type
 */
enum class SocketType : uint8_t {
    None,
    ReqRep,
    PushPull,
    PubSub
};

/**
 * High-level client to the distributed key-value store.
 * 
 * This interface automatically handles write batching.
 */
class YakClient {
    static const int PARTSYNC = 0x01;
    static const int FULLSYNC = 0x02;
public:
    /**
     * Creates a new client using a new ZeroMQ context that is automatically
     * destroyed when this instance is destructed
     */
    YakClient();
    /**
     * Creates a new client using a new ZeroMQ context that is automatically
     * destroyed when this instance is destructed.
     * 
     * This constructor automatically connects to a REQ endpoint.
     */
    YakClient(const char* endpoint);
    /**
     * Creates a new DKV client reusing an existing ZeroMQ context.
     * The context will not be destroyed even if the DKVClient instance is
     * destructed
     */
    YakClient(void* ctx);
    /**
     * Destructor that only destroyess the underlying context if the corresponding option is set
     */
    ~YakClient();
    /**
     * Connect to a request/reply host.
     * This allows both read and write access, but write requests need to wait for an acknowledge reply.
     * Therefore the effective (especially burst) transfer rate is                                                                                                    a bit lower
     * 
     * @param endpoint The endpoint URI, e.g. "tcp://10.10.1.1:7100"
     */
    void connectRequestReply(const char* endpoint);
    /**
     * Connect to a pull host.
     * This connection method only allows write requests, but is able to achieve
     * higher performance than req/rep (with basically unlimited write rate),
     * but doesn't provide any hard guarantees that the remote server won't be overloaded
     * by a massive amount of requests or when (and, more importantly, in which order)
     * requests will be received.
     * 
     * void connectRequestReply(const char* host);
     * @param port
     */
    void connectPushPull(const char* host);
    /**
     * Get the current context in use by this instance.
     * If the current instance initialized its own context, the context is only
     * valid until 
     * @return 
     */
    inline void* getContext() {
        return context;
    }
    /**
     * Get the internal socket. This may be used to use request classes directly.
     * @param table
     * @param keys
     * @return 
     */
    inline void* getSocket() {
        return socket;
    }
    /**
     * @param destroyContextOnExit If this is set to true, the context will be destroyed in the destructor
     */
    inline void setDestroyContextOnExit(bool newValue) {
        this->destroyContextOnExit = newValue;
    }
    inline bool isRequestReply() {return this->socketType == SocketType::ReqRep;}
    inline bool isPushPull() {return this->socketType == SocketType::PushPull;}
    inline bool isPubSub() {return this->socketType == SocketType::PubSub;}
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
    /**
     * Put a single key-value pair into the database (std::string version)
     * @param table The table number to put into
     * @param key The key to write
     * @param value The value to write
     * @return 0 on success, 1 on error (--> error description can be received using zmq_strerror(errno))
     */
    int put(uint32_t table, const std::string& key, const std::string& value, uint8_t flags = 0);
    /**
     * Put a single key-value pair into the database (length-delimited data version)
     * @param table The table number to put into
     * @param key The key to write
     * @param value The value to write
     * @return 0 on success, 1 on error (--> error description can be received using zmq_strerror(errno))
     */
    int put(uint32_t table, const char* key, size_t keySize, const char* value, size_t valueSize, uint8_t flags = 0);
    /**
     * Put a single key-value pair into the database (cstring version)
     * @param table The table number to put into
     * @param key The key to write
     * @param value The value to write
     * @return 0 on success, 1 on error (--> error description can be received using zmq_strerror(errno))
     */
    int put(uint32_t table, const char* key, const char* value, uint8_t flags = 0);
    /**
     * Single-key read function.
     * @param table The table number to read from
     * @param key The key to read
     * @return 0 on success, < 0 on error (take a look at the source code!)
     */
    int read(uint32_t table, const std::string& key, std::string& value);
    /**
     * Single-key read function.
     * @param table The table number to read from
     * @param key The key to read
     * @return 0 on success, < 0 on error (take a look at the source code!)
     */
    int read(uint32_t table, const char* key, std::string& value);
    /**
     * Multiple-key read.
     * Values are placed in the output in the same order as in the input.
     * 
     * @note Using this function is not recommended ifs performance matters.
     * It is provided to aid system integration and to provide an easy API.
     * 
     * @param keys The keys to read. Must not be empty.
     * @param values A reference to a vector where the values will be stored
     * @return 0 on success, < 0 on error (take a look at the source code!)
     */
    int read(uint32_t table, const std::vector<std::string>& keys, std::vector<std::string>& values);
    /**
     * Single-key exists function.
     * @return -1 on error, 0 on "does not exist", 1 on "does exist".
     */
    int exists(uint32_t table, const std::string& keys);
    /**
     * Multi-key exists function.
     * 
     * @return A vector that contains a bool (true if the key exists) for every given key (in the same order), or an empty vector if any error occured
     */
    int exists(uint32_t table, const std::vector<std::string>& keys, std::vector<bool>& result);
    /**
     * Count a specific range in the database.
     * @param from The first key to count - if this is empty, the range starts at the beginning
     * @param to The first key to count - if this is empty, the range starts at the beginning
     * @return The count or -1 in case of error
     */
    int64_t count(uint32_t table, const std::string& from, const std::string& to);
private:
    void* context;
    void* socket;
    bool destroyContextOnExit;
    uint64_t writeBatchSize;
    SocketType socketType;
};

#endif	/* DBCLIENT_HPP */