/* 
 * File:   Status.hpp
 * Author: uli
 *
 * Created on 1. Mai 2013, 00:51
 */

#ifndef STATUS_HPP
#define	STATUS_HPP

/**
 * A status representation that stores information about if
 * an operation has been executed successfully, and, if not,
 * an error message
 */
class Status {
public:
    /**
     * Construct a status that indicates success
     */
    Status();
    /**
     * Construct a status that indicates an error, defined by the given error string
     * @return 
     */
    Status(const std::string& string, int errorCode = 1);
    /**
     * Construct a status from another status object, C++11 Zero-Copy version
     * @param other
     */
    Status(Status&& other);
    /**
     * @return true if and only if this status indicates success,
     */
    bool ok() const;
    ~Status();
    /**
     * @return The error message. Empty if it doesn't exist
     */
    std::string getErrorMessage() const;
private:
    std::string* errorMessage; //Set to an error message, or NULL if no error occured
    int errorCode;
};

#endif	/* STATUS_HPP */

