/* 
 * File:   Tablespace.cpp
 * Author: uli
 * 
 * Created on 23. April 2013, 13:06
 */

#include "Tablespace.hpp"

Tablespace::Tablespace(ConfigParser& cfg, IndexType defaultTablespaceSize)
    : databases(defaultTablespaceSize), cfg(cfg) {
    //Initialize the table array with 16 tables.
    //This avoids early re-allocation
    databasesSize = defaultTablespaceSize;
    //Use malloc here to allow usage of realloc
    //Initialize all pointers to zero
    for (int i = 0; i < databases.size(); i++) {
        databases[i] = nullptr;
    }
}

void Tablespace::cleanup() {
    //Flush & delete all databases
    for (int i = 0; i < databasesSize; i++) {
        if (databases[i] != nullptr) {
            delete databases[i];
        }
    }
    databases.clear();
    databasesSize = 0;
}


Tablespace::TableType Tablespace::getTable(IndexType index, void* ctx) {
    TableOpenHelper helper(ctx, cfg);
    return getTable(index, helper);
}

Tablespace::~Tablespace() {
    cleanup();
}

Tablespace::TableType Tablespace::getTable(IndexType index, TableOpenHelper& openHelper) {
    //Check if the database has already been opened
    if (databases[index] == nullptr || index >= databases.size()) {
        openHelper.openTable(index);
    }
    Tablespace::TableType ret = databases[index];
    assert(ret != nullptr); //If this fails, the database could not be opened properly
    return ret;
}

void Tablespace::closeTable(IndexType index) {
    if (databases[index] != nullptr) {
        delete databases[index];
        databases[index] = nullptr;
    }
}