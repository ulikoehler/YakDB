/* 
 * File:   Status.cpp
 * Author: uli
 * 
 * Created on 1. Mai 2013, 00:51
 */

#include "Status.hpp"

Status::Status() : errorMessage(nullptr) {

}

Status::Status(const std::string& msg, int errorCode) : errorCode(errorCode) {
    errorMessage = new std::string(msg);
}

Status::Status(Status&& other) : errorMessage(other.errorMessage), errorCode(other.errorCode) {
    other.errorMessage = nullptr;
}

Status::~Status() {
    if (unlikely(errorMessage != nullptr)) {
        delete errorMessage;
    }
}

bool Status::ok() const {
    return unlikely(errorMessage == nullptr);
}

std::string Status::getErrorMessage() const {
    if (likely(errorMessage == nullptr)) {
        return "";
    } else {
        return *errorMessage;
    }
}
