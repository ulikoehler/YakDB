/* 
 * File:   Tablespace.hpp
 * Author: uli
 *
 * Created on 23. April 2013, 13:06
 */

#ifndef TABLESPACE_HPP
#define	TABLESPACE_HPP
#include <vector>
#include <cstdlib>
#include <zmq.h>
#include <rocksdb/db.h>

#include "TableOpenHelper.hpp"

/**
 * Encapsulates multiple key-value tables in one interface.
 * The tables are addressed by number and 
 */
class Tablespace {
public:
    typedef uint32_t IndexType;
    typedef rocksdb::DB* TableType;
    typedef typename std::vector<TableType> TableCollectionType;

    Tablespace(IndexType defaultTablespaceSize = 128);
    Tablespace(const Tablespace& other) = delete;
    
    ~Tablespace();
    /**
     * Close all tables and stop the table open server.
     * 
     * This can't be done in the destructor because it needs to be done before
     * the context is deallocated
     */
    void cleanup();

    /**
     * Get or create a table by index.
     * 
     * The supplied TableOpenHelper instance must only be used by the current
     * thread.
     * @param index
     * @param openHelper
     * @return 
     */
    TableType getTable(IndexType index, TableOpenHelper& openHelper);
    
    /**
     * Get or create a table by index.
     * 
     * This version instantiates a temporary TableOpenHelper.
     * Initialization is relatively expensive but objects like
     * asynchronous processes that only need it once, should use
     * this method that destroys it as early as possible.
     * @param index
     * @param openHelper
     * @return 
     */
    TableType getTable(IndexType index, void* ctx);

    /**
     * Close a table immediately.
     * It is the caller's responsibility to ensure that the table it not in use
     * currently. If it is in use, unexpected stuff might happen.
     * This method is NOT reentrant and NOT threadsafe.
     * @param index
     */
    void closeTable(IndexType index);

    inline TableType getExistingTable(IndexType index) {
        return databases[index];
    }

    inline TableCollectionType& getDatabases() {
        return databases;
    }
    
    /**
     * Checks if a given table is opened.
     * This method is reentrant and thread-safe.
     */
    bool isTableOpen(IndexType index) {
        return databases[index] != nullptr;
    }
    
    
private:
    /**
     * The databases vector.
     * Any method in this class has read-only access (tables may be closed by this class however)
     * 
     * Write acess is serialized using the TableOpenHelper class.
     * 
     * Therefore locks and double initialization are avoided.
     */
    TableCollectionType databases; //Indexed by table num
    uint32_t databasesSize;
};

#endif	/* TABLESPACE_HPP */

