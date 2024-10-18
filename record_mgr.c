#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "expr.h"

// Structure of the record manager
typedef struct RecordManager {
    BM_PageHandle pg_handler;        // Page handler
    BM_BufferPool poolconfig;        // Buffer pool configuration
    RID rec_id;                      // Unique record id
    Expr *condition;                 // Condition expression used for scanning
    int num_tuples;                  // Number of tuples
    int start_page;                  // Starting page in the record manager
    int last_page;                   // Last page number
    int max_slots;                   // Maximum number of slots
    int num_scanned;                 // Number of scanned records
} RecordManager;

// Structure for scan functions
typedef struct RM_ScanManager {
    Expr *cond;                     // Condition for scanning
    Record *current_record;         // Current record being scanned
    int current_page;               // Current page number during scan
    int current_slot;               // Current slot in the page during scan
    int scanned_count;              // Count of records scanned so far
} RM_ScanManager;

// Max pages and attribute length constants
const int maxPages = 100;
const int max_Attr_length = 15;

// Pointer to record manager
RecordManager *record_mgr;

// Function to find free slot in a page
int findFreeSlot(char *data, int recordSize) {
    int slotindex = 0;
    int total_slots_in_page = PAGE_SIZE / recordSize;

    while (slotindex < total_slots_in_page) {
        if (data[slotindex * recordSize] == '\0') return slotindex;
        else slotindex++;
    }
    return -1;  // No free slots found
}

/*************************** Record Manager *******************************************/
// Initialize record manager
extern RC initRecordManager(void *mgmtData) {
    initStorageManager();
    return RC_OK;
}

// Shut down record manager
extern RC shutdownRecordManager() {
    if (record_mgr != NULL) {
        free(record_mgr);
        record_mgr = NULL;
    }
    return RC_OK;
}

/***************************** Table Functions *****************************************/
// Create table
extern RC createTable(char *name, Schema *schema) {
    if (name == NULL || schema == NULL) return RC_FILE_NOT_FOUND;

    record_mgr = (RecordManager *)malloc(sizeof(RecordManager));
    RC status = initBufferPool(&record_mgr->poolconfig, name, maxPages, RS_LRU, NULL);
    
    if (status != RC_OK) {
        free(record_mgr);
        return status;
    }

    int slotSize = 0;
    for (int i = 0; i < schema->numAttr; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT:   slotSize += sizeof(int); break;
            case DT_FLOAT: slotSize += sizeof(float); break;
            case DT_STRING: slotSize += schema->typeLength[i]; break;
            case DT_BOOL:  slotSize += sizeof(bool); break;
            default: return RC_ERROR;
        }
    }
    
    int maxSlots = PAGE_SIZE / slotSize;
    record_mgr->num_tuples = 0;
    record_mgr->start_page = 1;
    record_mgr->last_page = 1;
    record_mgr->max_slots = maxSlots;

    // Create page file for table
    status = createPageFile(name);
    if (status != RC_OK) {
        shutdownBufferPool(&record_mgr->poolconfig);
        free(record_mgr);
        return status;
    }

    SM_FileHandle fh;
    status = openPageFile(name, &fh);
    if (status != RC_OK) {
        shutdownBufferPool(&record_mgr->poolconfig);
        free(record_mgr);
        return status;
    }

    // Serialize schema and write to disk
    char *serialized_data = serializeSchema(schema);
    status = writeBlock(0, &fh, serialized_data);

    if (status != RC_OK) {
        shutdownBufferPool(&record_mgr->poolconfig);
        free(serialized_data);
        closePageFile(&fh);
        free(record_mgr);
        return status;
    }

    closePageFile(&fh);
    free(serialized_data);
    return RC_OK;
}

// Open table
extern RC openTable(RM_TableData *rel, char *name) {
    RecordManager *record_mgr = (RecordManager *)malloc(sizeof(RecordManager));
    if (record_mgr == NULL) return RC_ERROR;

    BM_BufferPool *bufferpool = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    rel->mgmtData = record_mgr;
    rel->name = name;

    RC status = initBufferPool(&record_mgr->poolconfig, name, maxPages, RS_FIFO, NULL);
    if (status != RC_OK) {
        free(record_mgr);
        return status;
    }

    SM_FileHandle fh;
    status = openPageFile(name, &fh);
    if (status != RC_OK) {
        shutdownBufferPool(&record_mgr->poolconfig);
        free(record_mgr);
        return status;
    }

    BM_PageHandle pH;
    status = pinPage(&record_mgr->poolconfig, &pH, 0); 
    if (status != RC_OK) {
        shutdownBufferPool(&record_mgr->poolconfig);
        free(record_mgr);
        return status;
    }

    char *page_data = pH.data;
    int num_tuples, start_page, last_page, max_slots;
    
    memcpy(&num_tuples, page_data, sizeof(int));
    page_data += sizeof(int);
    memcpy(&start_page, page_data, sizeof(int));
    page_data += sizeof(int);
    memcpy(&last_page, page_data, sizeof(int));
    page_data += sizeof(int);
    memcpy(&max_slots, page_data, sizeof(int));

    record_mgr->num_tuples = num_tuples;
    record_mgr->start_page = start_page;
    record_mgr->last_page = last_page;
    record_mgr->max_slots = max_slots;

    Schema *schema = deserializeSchema(page_data);
    rel->schema = schema;
    rel->name = name;
    rel->mgmtData = (void *)record_mgr;

    status = unpinPage(bufferpool, &pH);
    if (status != RC_OK) {
        free(record_mgr);
        free(schema);
        shutdownBufferPool(bufferpool);
        free(bufferpool);
        return status;
    }
    return RC_OK;
}

// Close table
extern RC closeTable(RM_TableData *rel) {
    RecordManager *record_mgr = rel->mgmtData;
    shutdownBufferPool(&record_mgr->poolconfig);
    free(record_mgr);
    return RC_OK;
}

// Delete table
extern RC deleteTable(char *name) {
    return destroyPageFile(name) == RC_OK ? RC_OK : RC_FILE_NOT_FOUND;
}

// Get number of tuples in the table
extern int getNumTuples(RM_TableData *rel) {
    return ((RecordManager *)rel->mgmtData)->num_tuples;
}

/******************************** Record Functions ************************************/
// Insert record into table
extern RC insertRecord(RM_TableData *rel, Record *record) {
    RecordManager *record_mgr = (RecordManager *)rel->mgmtData;
    BM_PageHandle pH;

    RC status = pinPage(&record_mgr->poolconfig, &pH, record_mgr->last_page);
    if (status != RC_OK) return status;

    int free_slot_in_page = findFreeSlot(pH.data, getRecordSize(rel->schema));
    if (free_slot_in_page == -1) {
        status = unpinPage(&record_mgr->poolconfig, &pH);
        if (status != RC_OK) return status;
        record_mgr->last_page += 1;
        status = pinPage(&record_mgr->poolconfig, &pH, record_mgr->last_page);
        if (status != RC_OK) return status;
        free_slot_in_page = findFreeSlot(pH.data, getRecordSize(rel->schema));
    }

    int record_size = getRecordSize(rel->schema);
    char *slot_pointer = pH.data + (free_slot_in_page * record_size);
    memcpy(slot_pointer, record->data, record_size);

    record->id.page = record_mgr->last_page;
    record->id.slot = free_slot_in_page;

    status = markDirty(&record_mgr->poolconfig, &pH);
    if (status != RC_OK) return status;

    status = unpinPage(&record_mgr->poolconfig, &pH);
    if (status != RC_OK) return status;

    record_mgr->num_tuples++;
    return RC_OK;
}

// Delete a record from the table
extern RC deleteRecord(RM_TableData *rel, RID id) {
    RecordManager *record_mgr = (RecordManager *)rel->mgmtData;
    BM_PageHandle pH;

    RC delete_page = pinPage(&record_mgr->poolconfig, &pH, id.page);
    if (delete_page != RC_OK) return delete_page;

    int record_size = getRecordSize(rel->schema);
    char *slot_pointer = pH.data + (id.slot * record_size);
    memset(slot_pointer, '\0', record_size);

    delete_page = markDirty(&record_mgr->poolconfig, &pH);
    if (delete_page != RC_OK) return delete_page;

    delete_page = unpinPage(&record_mgr->poolconfig, &pH);
    return delete_page;
}

// Update a record in the table
extern RC updateRecord(RM_TableData *rel, Record *record) {
    RecordManager *record_mgr = (RecordManager *)rel->mgmtData;
    BM_PageHandle pH;

    RC update_page = pinPage(&record_mgr->poolconfig, &pH, record->id.page);
    if (update_page != RC_OK) return update_page;

    int record_size = getRecordSize(rel->schema);
    char *slot_pointer = pH.data + (record->id.slot * record_size);
    memcpy(slot_pointer, record->data, record_size);

    update_page = markDirty(&record_mgr->poolconfig, &pH);
    if (update_page != RC_OK) return update_page;

    return unpinPage(&record_mgr->poolconfig, &pH);
}

// Get a record by its ID
extern RC getRecord(RM_TableData *rel, RID id, Record *record) {
    RecordManager *record_mgr = (RecordManager *)rel->mgmtData;
    BM_PageHandle pH;

    RC record_page = pinPage(&record_mgr->poolconfig, &pH, id.page);
    if (record_page != RC_OK) return record_page;

    int record_size = getRecordSize(rel->schema);
    char *slot_pointer = pH.data + (id.slot * record_size);
    memcpy(record->data, slot_pointer, record_size);

    record_page = unpinPage(&record_mgr->poolconfig, &pH);
    return record_page;
}