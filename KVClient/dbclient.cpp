/* 
 * File:   dbclient.cpp
 * Author: uli
 * 
 * Created on 30. April 2013, 20:17
 */

#include "dbclient.hpp"

DKVClient::DKVClient() : context(zctx_new()), destroyContextOnExit(true) {

}

DKVClient::~DKVClient() {
    if (destroyContextOnExit && context) {
        zctx_destroy(context);
    }
}

zctx_t* DKVClient::getContext() const {
    return context;
}

void DKVClient::setDestroyContextOnExit(bool param) {
    this->destroyContextOnExit = param;
}