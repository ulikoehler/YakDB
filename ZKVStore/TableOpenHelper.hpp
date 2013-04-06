/* 
 * File:   TableOpenHelper.hpp
 * Author: uli
 *
 * Created on 6. April 2013, 18:15
 */

#ifndef TABLEOPENHELPER_HPP
#define	TABLEOPENHELPER_HPP
#include <czmq.h>

/**
 * This class provides lock-free concurrent table opener by using
 * a ZMQ inproc transport
 * 
 * Msg format: A single sizeof(IndexType)-sized frame containing the index
 * 
 * @param context
 */
class TableOpenHelper {
public:
    typedef leveldb::DB** LevelDBArray;
    typedef uint32_t IndexType;
    TableOpenHelper(zctx_t* context, LevelDBArray* databases);
    void openTable(IndexType index);
private:
    void* reqSocket; //This ZMQ socket is used to send requests
};

#endif	/* TABLEOPENHELPER_HPP */

