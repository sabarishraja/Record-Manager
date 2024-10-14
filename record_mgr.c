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





//Close Table
extern RC closeTable(RM_TableData *rel){
    RecordManager *record_mgr = rel->mgmtData;
    shutdownBufferPool(&record_mgr->poolconfig);
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

