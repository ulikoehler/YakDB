/* 
 * File:   Log.cpp
 * Author: uli
 * 
 * Created on 24. April 2013, 04:29
 */

#include "Log.hpp"
#include "endpoints.hpp"
#include "zutil.hpp"

LogSource::LogSource(zctx_t* ctx, const std::string& name) : ctx(ctx) {
    socket = zsocket_new_connect(ctx, ZMQ_PUB, logEndpoint);
}

LogSource::~LogSource() {
    zsocket_destroy(ctx, socket);
}