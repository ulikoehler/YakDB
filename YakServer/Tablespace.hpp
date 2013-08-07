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
#include <czmq.h>
#include <leveldb/db.h>

#include "TableOpenHelper.hpp"

/**
 * Encapsulates multiple key-value tables in one interface.
 * The tables are addressed by number and 
 */
class Tablespace {
public:
    typedef uint32_t IndexType;
    typedef leveldb::DB* TableType;
    typedef typename std::vector<TableType> TableCollectionType;

    Tablespace(IndexType defaultTablespaceSize = 16);
    Tablespace(const Tablespace& other) = delete;

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
     * Close a table immediately.
     * It is the caller's responsibility to ensure that the table it not in use
     * currently. If it is in use, unexpected stuff will happen.
     * @param index
     */
    void closeTable(IndexType index);

    inline TableType getExistingTable(IndexType index) {
        return databases[index];
    }

    inline TableCollectionType& getDatabases() {
        return databases;
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

