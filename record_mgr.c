#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "expr.h"
//Structure of the record Manager:
typedef struct RecordManager{
    //Page handler
    BM_PageHandle pg_handler;
    //Buffer pool configuration
    BM_BufferPool poolconfig;
    //Unique record id
    RID rec_id;
    //Condition expression used for scanning
    Expr *condition;
    // Number of tuples
    int num_tuples;
    //Starting page in the record manger
    int start_page;
    int last_page;
    int max_slots;
    //Number of scanned records
    int num_scanned;
} RecordManager;

//Strture for scan functions
typedef struct RM_ScanManager{
    Expr *cond;
    Record *current_record;
    int current_page;
    int current_slot;
    int scanned_count;
} RM_ScanManager;
//Now, the maximum number of pages is defined in permanent manner
const int maxPages = 100;
//Maximum Attribute name length
const int max_Attr_length = 15;

//Pointer to record manager
RecordManager *record_mgr;

//findFreeSlot -> Function to check if a free slot is found in a page
//Parameters: data, recordSize -> size of the record
int findFreeSlot(char *data, int recordSize){
    int slotindex = 0;
    //Finding the number of slots in a page
    int total_slots_in_page = PAGE_SIZE / recordSize;
    //iterate to find an empty slot:
    while(slotindex < total_slots_in_page){
        if(data[slotindex * recordSize] == '\0')  return slotindex;
        else{
            slotindex = slotindex + 1;
        }
    }
    //If the there are no vacant slots, return -1
    return -1;
}


/***************************Record Manager*******************************************/
//initRecordManager -> Function to initialize the record manager
//The record Manager can be initialized with the help of intitializer of Storage Manager
//So, we just have to call the function
extern RC initRecordManager (void *mgmtData){
    initStorageManager();
    return RC_OK;
}

//shutdownRecordManager -> Function to shut down the record manager
//This is done by cleaning up and deallocating the resources of the record
extern RC shutdownRecordManager(){
    //Free the allocated memory
    if(record_mgr!=NULL){
        free(record_mgr);
        //Set the record manager pointer to NULL
        record_mgr = NULL;
    }
    return RC_OK;
}
/*****************************Table Functions*********************************************/

//createTable-> function to create a table with name and schema
//Buffer pool is also initialized for this with the help of buffer manager
//creattion of : buffer pool, empty page, page file with given name
extern RC createTable (char *name, Schema *schema){
    if(name==NULL || schema==NULL){
        return RC_FILE_NOT_FOUND;
    }
    record_mgr = (RecordManager *)malloc(sizeof(RecordManager));
    //Initializing the buffer pool 
    RC status = initBufferPool(&record_mgr->poolconfig, name, maxPages, RS_LRU, NULL);
    //If buffer pool initialization fails, the memory allocated should be freed
    if(status!=RC_OK){
        //Free memory if the buffer pool initialization fails
        free(record_mgr);
        return status;
    }
    //Calculate the slot size based on schema's data type
    //Each attribute is responsible for contributing to total record size
    int slotSize = 0;
    for(int i = 0; i<schema->numAttr; i++){
        switch (schema->dataTypes[i])
        {
        case DT_INT:
            slotSize += sizeof(int);
            break;
        case DT_FLOAT:
            slotSize += sizeof(float);
            break;
        case DT_STRING:
            slotSize += schema->typeLength[i];
            break;
        case DT_BOOL:
            slotSize+=sizeof(bool);
            break;
        
        default:
            return RC_ERROR;
        }
    }
    int maxSlots = PAGE_SIZE/ slotSize;
    //Initializing the management information of record manager
    record_mgr->num_tuples = 0;
    record_mgr->start_page = 1;
    record_mgr->last_page = 1; //Initially, last page will be the first page
    record_mgr-> max_slots = maxSlots;
    //Create a page file to represent table on disk
    status = createPageFile(name);
    if(status!=RC_OK){
        //shutdown th buffer and free the memory
        shutdownBufferPool(&record_mgr->poolconfig);
        free(record_mgr);
        return status;
    }
    SM_FileHandle fh;
    //Open the page file with the help of file handler
    status = openPageFile(name, &fh);
    {
        if(status!=RC_OK){
            shutdownBufferPool(&record_mgr->poolconfig);
            free(record_mgr);
            return status;
        }

    }
    //Serialize the schema - Done to convert the schema in string format
    char* serialized_data = serializeSchema(schema);

    //Write the schema in the first block of file
    status = writeBlock(0, &fh, serialized_data);
    if(status!=RC_OK){
        shutdownBufferPool(&record_mgr->poolconfig);
        //Freeing the serialized data as it can cause memory leak
        free(serialized_data);
        closePageFile(&fh);
        free(record_mgr);
        return status;
    }

    //closePageFile to close the page after writing in the record manager
    status = closePageFile(&fh);
    if(status!=RC_OK){
        shutdownBufferPool(&record_mgr->poolconfig);
        free(record_mgr);
        return status;
    }
    free(serialized_data);
    // IF EVERYTHING GOES WELL, RETURN RC_OK
    return RC_OK;
    
}   

//OPEN TABLE:
//Steps to perform:
//Open page file with the help of initializing buffer pool,
// Pin the first page and deserialize Schema after reading the serialized schema
// Store table's metadata and then unpin after deserialization
extern RC openTable(RM_TableData *rel, char *name){
    //Allocating memory for record manager struct
    RecordManager *record_mgr = (RecordManager *) malloc(sizeof(RecordManager));
    if(record_mgr == NULL)  return RC_ERROR;
    BM_BufferPool *bufferpool = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    rel->mgmtData = record_mgr;
    rel->name = name;
    //Initializing the buffer pool
    RC status = initBufferPool(&record_mgr->poolconfig, name, maxPages, RS_FIFO, NULL);
    if(status!=RC_OK){
        free(record_mgr);
        return status;
    }

    //Open the page file corresponding to the table
    SM_FileHandle fh;
    status = openPageFile(name, &fh);
    if(status != RC_OK){
        shutdownBufferPool(&record_mgr->poolconfig);
        free(record_mgr);
        return status;
    }
    //Pin the first block
    //Pinning refers to loading a specific page form the disk into memory
    BM_PageHandle pH;
    status = pinPage(&record_mgr->poolconfig, &pH, 0); // 0 represents the page number
    if(status != RC_OK){
        shutdownBufferPool(&record_mgr->poolconfig);
        free(record_mgr);
        return status;
    }
    //Extract data from  pinned page
    char *page_data = pH.data;
    int num_tuples, start_page, last_page, max_slots;
    
    memcpy(&num_tuples, page_data, sizeof(int));
    page_data += sizeof(int);
    memcpy(&start_page, page_data, sizeof(int));
    page_data += sizeof(int);
    memcpy(&last_page, page_data, sizeof(int));
    page_data += sizeof(int);
    memcpy(&max_slots, page_data, sizeof(int));

    // Set buffer pool configuration in RecordManager
    record_mgr->poolconfig = *bufferpool;  // Store buffer pool in RecordManager
    record_mgr->num_tuples = num_tuples;
    record_mgr->start_page = start_page;
    record_mgr->last_page = last_page;
    record_mgr->max_slots = max_slots;

    // Deserialize schema
    Schema *schema = deserializeSchema(page_data);

    // Assign the schema and name to the RM_TableData structure
    rel->schema = schema;
    rel->name = name;
    rel->mgmtData = (void *)record_mgr;

    // Unpin the page and release resources
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

//Close Table
extern RC closeTable(RM_TableData *rel){
    RecordManager *record_mgr = rel->mgmtData;
    shutdownBufferPool(&record_mgr->poolconfig);
    free(record_mgr);
    return RC_OK;
}

//Delete table
extern RC deleteTable (char *name){
    if(destroyPageFile(name)!=RC_OK){
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}

//Number of tuples in a table
extern int getNumTuples (RM_TableData *rel){
    RecordManager *record_mgr = rel->mgmtData;
    return record_mgr->num_tuples;
}

/********************************  RECORD FUNCTIONS  **************************************************/
//Series of steps to be done in insertRecord function
//Pin the last page of buffer pool and find free slot to write record into the page
//Serialize the record into page and then unpin after insertion

extern RC insertRecord (RM_TableData *rel, Record *record){
    RecordManager *record_mgr = (RecordManager *)rel->mgmtData;
    BM_PageHandle pH;
    //Pin the last page of the record manager
    RC status = pinPage(&record_mgr->poolconfig, &pH, record_mgr->last_page);
    if(status!=RC_OK){
        return status;
    }
    //Finding free slot in the pin page using findFreeSlot function
    int free_slot_in_page = findFreeSlot(pH.data, getRecordSize(rel->schema));
    //In case free slot is not found, the pinned page is unpinned
    if(free_slot_in_page == -1){
        status = unpinPage(&record_mgr->poolconfig, &pH);
        if(status != RC_OK){
            return status;
        }
        //Pin a new page and then increment the last page
        record_mgr->last_page += 1;
        status = pinPage(&record_mgr->poolconfig, &pH, record_mgr->last_page);
        if(status != RC_OK){
            return status;
        }
        free_slot_in_page = findFreeSlot(pH.data, getRecordSize(rel->schema));
    }
    //Claculate the record's position in page
    int record_size = getRecordSize(rel->schema);
    char *slot_pointer = pH.data + (free_slot_in_page * record_size);

    //Serialize the record into the page
    memcpy(slot_pointer, record->data, record_size);

    //Set the record's id with page number and slot number
    record->id.page = record_mgr->last_page;
    record->id.slot = free_slot_in_page;

    //markDirty function should be used to understand that is has been modified
    status = markDirty(&record_mgr->poolconfig, &pH);
    if(status!=RC_OK){
        return status;
    }

    //As the record has been inserted now, the page can be unpinned
    status = unpinPage(&record_mgr->poolconfig, &pH);
    if(status != RC_OK){
        return status;
    }

    //Update the total number of tuples
    record_mgr->num_tuples += 1;

    return RC_OK;

}

//deleteRecord - This function is to delete a record
// RID -> this id contains the page and slot to be deleted
extern RC deleteRecord (RM_TableData *rel, RID id){
    RecordManager *record_mgr = (RecordManager *)rel->mgmtData;
    BM_PageHandle pH;

    //Pin the page to be deleted
    RC delete_page = pinPage(&record_mgr->poolconfig, &pH, id.page);
    if(delete_page != RC_OK){
        return delete_page;
    }

    //Locate the slot
    int record_size = getRecordSize(rel->schema);
    char *slot_pointer = pH.data + (id.slot * record_size);

    //Mark the slot as empty
    memset(slot_pointer, '\0', record_size);

    //As the page has been modified, mark the page as dirty
    delete_page = markDirty(&record_mgr->poolconfig, &pH);
    if(delete_page != RC_OK){
        return delete_page;
    }

    //After deletion, the page should be unpinned
    delete_page = unpinPage(&record_mgr->poolconfig, &pH);
    if(delete_page!=RC_OK){
        return delete_page;
    }
    
    //Decrement the number of tuples
    record_mgr->num_tuples = record_mgr->num_tuples - 1;

    return RC_OK;
}

//Update Record function
//Steps to be followed for this function:
//pin the required page, find the correct slot using RID.
//memcpy will be used to copy the data into slot
//after modifying the data, mark the page dirty
//Unpin the page
extern RC updateRecord (RM_TableData *rel, Record *record){
    RecordManager *record_mgr = (RecordManager *) rel->mgmtData;
    BM_PageHandle pH;
    //Pinning the page containing record
    RC status = pinPage(&record_mgr->poolconfig, &pH, record->id.page);
    if(status!=RC_OK){
        return status;
    }
    //Locate the record using correct slot with the help of RID
    int record_size = getRecordSize(rel->schema);
    char *slot_pointer = pH.data + (record->id.slot * record_size);

    //Copying the new data into slot
    memcpy(slot_pointer, record->data, record_size);

    //As the data has been modified now, we can mark the page as dirty
    status = markDirty(&record_mgr->poolconfig, &pH);
    if(status!=RC_OK){
        //Even though the unpinning will happen after marking the page dirty, 
        //it is better to unpin the page if marking page dirty fails
        unpin(&record_mgr->poolconfig, &pH);
        return status;
    }
    //Unpin page after modifying the data
    status= unpinPage(&record_mgr->poolconfig, &pH);
    if(status!=RC_OK)    return status;
    return RC_OK;
}

//getRecord fucntion is used to get a record with the help of RID
//Only read operation on data is going to be performed
//pin the page, locate the slot using RID and then copy data from page's slot into record structure
extern RC getRecord (RM_TableData *rel, RID id, Record *record){
    RecordManager *record_mgr = (RecordManager *) rel->mgmtData;

    //Pin the page where the record is stored
    BM_PageHandle pH;
    RC status = pinPage(&record_mgr->poolconfig, &pH, id.page);
    if(status!=RC_OK){
        return status;
    }
    //Calculate the size of record based on schema
    int record_size = getRecordSize(rel->schema);
    char *page_data = pH.data + (id.slot * record_size);
    //Check if the slot is used, where + is an active slot
    if(*page_data != "+"){
        unpinPage(&record_mgr->poolconfig, &pH);
        //Slot is empty and not used
        return RC_NO_TUPLE_RID;
    }
    //Copying the record data to record structure
    record->id = id;
    memcpy(record->data, page_data + 1 , record_size - 1 );

    //Unpin page after use
    status = unpinPage(&record_mgr->poolconfig, &pH);
    if(status!=RC_OK){
        return status;
    }

    return RC_OK;
}

/********************************  SCAN FUNCTIONS  **************************************************/
//startScan will be used to initialize scan operation on table
//A condition will be provided and scan operation will funciton only if the condition is satisfied
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    if(cond == NULL)    return RC_CONDITION_NOT_FOUND;
    RM_ScanManager *scan_manager = (RM_ScanManager *) malloc(sizeof(RM_ScanManager));
    if(scan_manager == NULL){
        return RC_FILE_NOT_FOUND;
    }
    scan->mgmtData = scan_manager;
    scan->rel = rel;

    //Initialize the scan with condition and start from first page
    scan_manager->cond = cond;
    scan_manager->current_page = 1;
    scan_manager->current_slot = 0;
    scan_manager->scanned_count = 0;

    //Allocate memory for current record
    scan_manager->current_record = (Record *) malloc(sizeof(Record));
    if(scan_manager->current_record == NULL){
        free(scan_manager);
        return RC_ERROR;
    }
    //Set management data in scan handle
    scan->mgmtData = scan_manager;
    //Set the relation in scan handle
    scan->rel = rel;
    return RC_OK;

}

//next function



//closeScan function 
extern RC closeScan (RM_ScanHandle *scan){
    if(scan == NULL || scan->mgmtData == NULL){
        return RC_ERROR;
    }
    ((RM_ScanManager *)scan->mgmtData)->current_record = NULL;
    free(((RM_ScanManager *)scan->mgmtData)->current_record);

    scan->mgmtData = NULL;
    free(scan->mgmtData);

    scan = NULL;
    free(scan);

    return RC_OK;
}

/********************************  Schema FUNCTIONS  **************************************************/

extern int getRecordSize(Schema *schema){
    int total_size = 0;
    for(int i = 0; i<schema->numAttr; i++){
        switch(schema->dataTypes[i]){
            case DT_INT:
                total_size +=sizeof(int);
                break;
            case DT_STRING:
                total_size += schema->typeLength[i];
                break;
            case DT_BOOL:
                total_size +=sizeof(bool);
                break;
            case DT_FLOAT:
                total_size +=sizeof(float);
                break;
            default:
                return RC_ERROR;
        }
    }
    return total_size;
}

//createSchema
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    Schema *create_schema = (Schema *)malloc(sizeof(Schema));

    create_schema -> numAttr = numAttr;
    create_schema ->attrNames = attrNames;
    create_schema->dataTypes = dataTypes;
    create_schema->keyAttrs = keys;
    create_schema->keySize = keySize;
    create_schema->typeLength = typeLength;

    //Check if the allocation worked good
    if(create_schema->attrNames == NULL || create_schema->dataTypes == NULL, create_schema->typeLength == NULL || create_schema->keyAttrs == NULL){
        return create_schema;
    }
    return create_schema;
}

//freeSchema
extern RC freeSchema (Schema *schema){
    free(schema);
    return RC_OK;

}