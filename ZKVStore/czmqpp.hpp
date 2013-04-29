/* 
 * File:   czmqpp.hpp
 * Author: uli
 *
 * Created on 23. April 2013, 19:15
 */

#ifndef CZMQPP_HPP
#define	CZMQPP_HPP
#include <czmq.h>

//UNFINISHED DO NOT USE !!!

class ZCtx {
public:
    ZCtx();
    ~ZCtx();
    zctx_t* getUnderyling();
    /**
     * Set the underlying ZMQ context. Setting this to NULL ensures
     * the underlying implementation is not destroyed when the current class
     * instance is destroyed.
     */
    void setUnderyling();
private:
    zctx_t* underlying;
};

#endif	/* CZMQPP_HPP */

