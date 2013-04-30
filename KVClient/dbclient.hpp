/* 
 * File:   dbclient.hpp
 * Author: uli
 *
 * Created on 30. April 2013, 20:17
 */

#ifndef DBCLIENT_HPP
#define	DBCLIENT_HPP
#include <czmq.h>

class DKVClient {
public:
    /**
     * Creates a new DKV client using a new ZeroMQ context that is automatically
     * destroyed when this instance is destructed
     */
    DKVClient();
    /**
     * Creates a new DKV client reusing an existing ZeroMQ context.
     * The context will not be destroyed even if the DKVClient instance is
     * destructed
     */
    DKVClient(zctx_t* ctx);
    /**
     * Connect 
     * @param host
     * @param port
     */
    void connect(const char* host, uint32_t port);
    ~DKVClient();
    /**
     * Get the current context in use by this instance.
     * @return 
     */
    zctx_t* getContext() const;
    /**
     * @param destroyContextOnExit If this is set to true, the context will be destroyed in the destructor
     */
    void setDestroyContextOnExit(bool destroyContextOnExit);
private:
    zctx_t* context;
    void* reqRepSocket;
    bool destroyContextOnExit;
};

#endif	/* DBCLIENT_HPP */

