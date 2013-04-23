/* 
 * File:   czmqpp.cpp
 * Author: uli
 * 
 * Created on 23. April 2013, 19:15
 */

#include "czmqpp.hpp"

ZCtx::ZCtx() {
    underlying = zctx_new();
}

ZCtx::~ZCtx() {
    if (underlying != NULL) {
        zctx_destroy(&underlying);
    }
}

zctx_t* ZCtx::getUnderyling() {
    return underlying;
}