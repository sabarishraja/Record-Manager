#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>

// dummy function, as it has no use we have left it empty
void initStorageManager (){ } // empty as we have no use for this

// creating a single page
RC createPageFile(char *fileName){
    FILE *file;
    file=fopen(fileName,"w+"); // open file in read and write mode
    //printf("File is opened!...........\n");

    if(file==NULL){ // checking whether the file exist or not
        //printf("File create failed!........\n");
        return RC_WRITE_FAILED;
    }

    // creating new a page in a fixed block size
    SM_PageHandle newPage=(SM_PageHandle)calloc(PAGE_SIZE,sizeof(char));

    if(newPage==NULL){
        fclose(file); // closing the file as page create failed
        //printf("Page create failed!.......");
        return RC_WRITE_FAILED;
    }

    //printf("Page is created!......");

    //adding the page into the file
    
    int writtenBlockSize=fwrite(newPage,sizeof(char),PAGE_SIZE,file);

    if(writtenBlockSize<PAGE_SIZE){ //out of space in file to write all data
        //printf("Out of space in file!......");
        free(newPage); // remove the allocated memory
        fclose(file); // close the file
        return RC_WRITE_FAILED;
    }

    fclose(file); //close the file
    free(newPage); // free the allocated space
    
    return RC_OK;
}

// opening page file
RC openPageFile(char *fileName, SM_FileHandle *fHandle){ 
    FILE *existfile=fopen(fileName,"rb+"); // opening the a binary file in read and write mode
    
    if(existfile==NULL){ // check whether the file exist or not
        //printf("File not found!");
        return RC_FILE_NOT_FOUND;
    }

    fseek(existfile,0,SEEK_END); // move the cursor to end of file
    fHandle->totalNumPages=ftell(existfile)/ PAGE_SIZE; // getting the total page number

    fseek(existfile,0,SEEK_SET); // moving the cursor to start of the file

    //setting other metadata
    fHandle->fileName=fileName;
    fHandle->mgmtInfo=existfile;
    fHandle->curPagePos=0;

    return RC_OK;
}

//closing page file
RC closePageFile(SM_FileHandle *fHandle){
    // check if the file is open using fHandle 
    if(fHandle==NULL || fHandle->mgmtInfo==NULL){
        //printf("file not intitalised!........");
        return RC_FILE_HANDLE_NOT_INIT;
    }
    FILE *file=(FILE *)fHandle->mgmtInfo; // getting the file
    fclose(file); // closing the file
    return RC_OK;
}

//delete page file
RC destroyPageFile(char *fileName){
    FILE *existFile;
    
    existFile=fopen(fileName,"r"); // opening the file in read mode, to check its existence 
    
    if(existFile==NULL){
        //printf("File Destroy: file not found!");
        return RC_FILE_NOT_FOUND;
    }

    //printf("File to delete:%s\n",fileName);
    // int delete_code=remove(fileName);
    // printf("Delete code:%d",delete_code);
    // if(delete_code!=0){ // if remove return other than zero then deletion is failed
    //     printf("File Destory failed!....");
    //     return RC_FILE_NOT_FOUND;
    // }
    if(fclose(existFile)==0){
        //printf("file closed\n");
        if(remove(fileName)!=0){
           perror("Delete failed!");
            return RC_FILE_NOT_FOUND;
        }
    }
    else{
        return RC_FILE_NOT_FOUND;
    }
    
    return RC_OK; 
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

    // Checking whether the file handle exists
    if(pageNum >= fHandle->totalNumPages || pageNum < 0 || fHandle == NULL || fHandle->mgmtInfo == NULL) {
        //printf("No proper file exists!");
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Get the file
    FILE *existFile = (FILE *)fHandle->mgmtInfo;

    // Get the position of the file to begin the read
    long pos = (long)pageNum * PAGE_SIZE;

    // Check if the read position is in right direction
    if(fseek(existFile, pos, SEEK_SET) != 0) {
        //printf("An error occured when attempting read");
        return RC_READ_NON_EXISTING_PAGE;
    }

    // add the read page data into mempage
    size_t bRead = fread(memPage, sizeof(char), PAGE_SIZE, existFile);

    if(bRead != PAGE_SIZE) {
        //printf("Incomplete Page Read");
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Update the read page position in the file handle
    fHandle->curPagePos = pageNum;

    return RC_OK;
}

RC getBlockPos(SM_FileHandle *fHandle) {
    // Check for the right fHandle
    if(fHandle->mgmtInfo == NULL || fHandle == NULL) {
        //printf("Something's wrong with the fHandle");
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Gets the block position in the fHandle
    return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Gets the block with page number 0 i.e First
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Gets the one before the current block in the fHandle i.e Previous
    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Gets the current block in the fHandle
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Gets the one after the current block in the fHandle i.e Next
    return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Gets the last block in the fHandle
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
    /*Verifying if the given pageNum is valid*/
    if(pageNum < 0 || pageNum >= fHandle -> totalNumPages)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }
    //printf("page data is valid\n");

    /*Verifying if file is open for writing*/
    if(fHandle -> mgmtInfo == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    //printf("file data is valid\n");

    /*Calculating the sum to required page*/
    long sum = (long)pageNum * PAGE_SIZE;

    /*Moving file pointer to sum*/
    //printf("Sum is %lld\n",sum);
    if(fseek(fHandle -> mgmtInfo, sum, SEEK_SET) != 0)
    {
        return RC_WRITE_FAILED;
    }

    //printf("file pointer is moved!......\n");

    // check data availability
    // for (int i=0; i < PAGE_SIZE; i++)
    //    printf("%c ", memPage[i]);

    /*Writing data from memPage to file*/   
    int writtenDataSize=fwrite(memPage, sizeof(char), PAGE_SIZE, (FILE*) (fHandle -> mgmtInfo));
    //printf("The written data size is %d\n",writtenDataSize);
    if( writtenDataSize!= PAGE_SIZE)
    {
        return RC_WRITE_FAILED;
    }

    //printf("Data is written!.......");

    /*Updating current page position*/
    fHandle -> curPagePos = pageNum;

    return RC_OK;
}


RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    /*Verifying if file is open for writing*/
    if(fHandle -> mgmtInfo == NULL) return RC_FILE_HANDLE_NOT_INIT;

    /*Calculating sum to current Page*/
    long sum = (long)(fHandle -> curPagePos) * PAGE_SIZE;

    /*Moving file pointer to sum*/
    if(fseek(fHandle -> mgmtInfo, sum, SEEK_SET) != 0) return RC_WRITE_FAILED;

    /*Writing 1 page data from memPage to file*/
    if(fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle -> mgmtInfo) != PAGE_SIZE) return RC_WRITE_FAILED;

    /*Updating current page position*/
    fHandle -> curPagePos++;

    return RC_OK;
}


RC appendEmptyBlock (SM_FileHandle *fHandle)
{
    /*Verifying if file is open for writing*/
    if(fHandle -> mgmtInfo == NULL) return RC_FILE_HANDLE_NOT_INIT;

    /*Calculating the sum to the end of the file*/
    long sum = (long) (fHandle -> curPagePos) * PAGE_SIZE;

    /*Moving file pointer to sum*/
    if(fseek(fHandle -> mgmtInfo, sum, SEEK_SET) != 0) return RC_WRITE_FAILED;

    /*Creating empty page written with \0 bytes*/
    SM_PageHandle emptPage = (SM_PageHandle) calloc (PAGE_SIZE, sizeof(char));
    
    if(emptPage == NULL) return RC_WRITE_FAILED;

    /*Writing empty page to file*/
    if (fwrite(emptPage, sizeof(char), PAGE_SIZE, fHandle -> mgmtInfo) != PAGE_SIZE)
    {
        free(emptPage);
        return RC_WRITE_FAILED;
    }

    // updating the file handler
    fHandle -> totalNumPages++;
    fHandle -> curPagePos++;

    /*Free allocated memory*/
    free(emptPage);

    return RC_OK;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
    /*Verifying if file is open for writing*/
    if(fHandle -> mgmtInfo == NULL) return RC_FILE_HANDLE_NOT_INIT;

    /*Calculating current number of pages in file*/
    int curNumPages = fHandle -> totalNumPages;

    /*Checking if the file has enough space*/
    if(numberOfPages <= curNumPages) return RC_OK;

    /*Calculating number of additional pages required*/
    int addtlPages = numberOfPages - curNumPages;

    /*Creating empty page*/
    SM_PageHandle emptPage = (SM_PageHandle) calloc (PAGE_SIZE, sizeof(char));
    if(emptPage == NULL) return RC_WRITE_FAILED;

    /*Moving to end of file*/
    if(fseek(fHandle -> mgmtInfo, 0, SEEK_END) != 0)
    {
        free(emptPage);
        return RC_WRITE_FAILED;
    }

    /*Adding empty pages to the file to meet requirement*/
    for(int j = 0; j < addtlPages; j++)
    {
        //writing empty page
        if(fwrite(emptPage, sizeof(char), PAGE_SIZE, fHandle -> mgmtInfo) != PAGE_SIZE)
        {
            free(emptPage);
            return RC_WRITE_FAILED;
        }
    curNumPages++; // incrementing current page as new page is added
    }
    
    /*Updating total number of pages in file*/
    fHandle -> totalNumPages = curNumPages;

    /*Free allocated memory*/
    free(emptPage);

    return RC_OK;
}