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

    Tablespace(ConfigParser& cfg, IndexType defaultTablespaceSize = 128);
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


    /*
     * Get the pointer to a table, but don't create / open the table if it is not
     * open yet
     * @return A pointer to the table or nullptr if it is not open
     */
    TableType getTableIfOpen(IndexType index);

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
     * Get the maximum 0-based table index i so that t_i is currently open
     * so there is no j > i so that t_j is open (or -1 if no table is open)
     */
    inline int32_t getMaximumOpenTableNumber() {
        int32_t ret = -1;
        for(uint32_t i = 0; i < databases.size(); i++) {
            if(getExistingTable(i) != nullptr) {
                ret = i;
            }
        }
        return ret;
    }

    /**
     * Checks if a given table is opened.
     * This method is reentrant and thread-safe.
     */
    inline bool isTableOpen(IndexType index) {
        return getExistingTable(index) != nullptr;
    }

    /**
     * Ensure the internal vectors have at least an appropriate size
     * to store a table at the given index
     */
    inline void ensureSize(IndexType tableIndex) {
        //Avoid scaling superlinearly by only reserving a small margin
        // beyond the required size
        databases.reserve(tableIndex + 16);
        mergeRequired.reserve(tableIndex + 16);
    }

    /**
     * Erase a table entry and get the (now erased)
     * table entry. Might return nullptr if the table
     * wasn't open in the first place.
     * This operation is not executed atomically.
     */
    inline TableType eraseAndGetTableEntry(IndexType index) {
        TableType db = databases[index];
        databases[index] = nullptr;
        return db;
    }

    /**
     * Get a pointer to the memory location in the databases vector
     * where the caller may store a pointer to a newly opened table.
     */
    inline TableType* getTablePointer(IndexType index) {
        return &databases[index];
    }

    /**
     * Set the merge required flag for a given table index.
     * Does not automatically resize the vectors.
     */
    inline void setMergeRequired(IndexType index, bool newValue) {
        mergeRequired[index] = newValue;
    }

    /**
     * Get the merge required flag for a given table index.
     * Does not automatically resize the vectors.
     */
    inline bool isMergeRequired(IndexType index) {
        return mergeRequired[index];
    }

private:
    /**
     * The databases vector.
     * Any method in this class has read-only access (tables may be closed by this class however)
     *
     * Write acess is serialized using the TableOpenHelper/TableOpenServer classes.
     *
     * Therefore locks and double initialization are avoided.
     */
    TableCollectionType databases; //Indexed by table num
    /**
     * This vector stores true for any table if it is required to use the
     * Merge operation instead of the Put operation.
     * This is true exactly if a non-REPLACE merge operator is selected.
     * The details of selecting either PUT o
     */
    std::vector<bool> mergeRequired; //Indexed by table num
    ConfigParser& cfg;
};

#endif	/* TABLESPACE_HPP */
