#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20

int HashFunction(char *key,int buckets){
	int sum=0;
	for(int i=0;i<strlen(key);i++){
		sum += key[i];
	}
	return sum % buckets;
}

HT_ErrorCode HT_Init() {
    HashTableBlock=NULL;
    return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int buckets) {
    CALL_OR_EXIT(BF_CreateFile(filename)); //Create the file
    int file_desc;
    CALL_OR_EXIT(BF_OpenFile(filename,&file_desc));  //Open the file
    //Create file info data
    FileInfo file_info;
    strcpy(file_info.file_type,"HashFile");
    strcpy(file_info.owner,"DatabaseAdmin");
    file_info.buckets_num=buckets;

    //Create the meta Data block to save file info
    BF_Block *metadataBlock;
    BF_Block_Init(&metadataBlock);
    CALL_OR_EXIT(BF_AllocateBlock(file_desc,metadataBlock));
    char *metaData = BF_Block_GetData(metadataBlock);
    
    //Create the HashTable block
    BF_Block_Init(&HashTableBlock);
    CALL_OR_EXIT(BF_AllocateBlock(file_desc,HashTableBlock));
    int num_of_blocks;
    CALL_OR_EXIT(BF_GetBlockCounter(file_desc,&num_of_blocks));
    file_info.ht_block_num = num_of_blocks-1;
    file_info.total_blocks = num_of_blocks;
    char *HashTableBlock_data = BF_Block_GetData(HashTableBlock);

    memset(HashTableBlock_data,0,BF_BLOCK_SIZE);  //Initialize HashTable Block data
    memcpy(metaData,&file_info,sizeof(file_info));  //Write file info in the metadata block

    //Write blocks to the disc
    BF_Block_SetDirty(metadataBlock);
    BF_Block_SetDirty(HashTableBlock);
    BF_UnpinBlock(metadataBlock);
    BF_UnpinBlock(HashTableBlock);
    BF_Block_Destroy(&metadataBlock);
    BF_Block_Destroy(&HashTableBlock);
    CALL_OR_EXIT(BF_CloseFile(file_desc));
    return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
    CALL_OR_EXIT(BF_OpenFile(fileName,indexDesc));    //Open the file
    //Get file info from metaDataBlock
    BF_Block *metadataBlock;
    BF_Block_Init(&metadataBlock);
    CALL_OR_EXIT(BF_GetBlock(*indexDesc,0,metadataBlock));
    char *metaData = BF_Block_GetData(metadataBlock);
    memcpy(&File_Info,metaData,sizeof(File_Info));
    printf("File info are:%s %s %d %d %d\n",File_Info.file_type,File_Info.owner,File_Info.ht_block_num,File_Info.total_blocks,File_Info.buckets_num);
    //Unpin and Destroy the block's memory
    BF_UnpinBlock(metadataBlock);
    BF_Block_Destroy(&metadataBlock);

    //Get the HashtableBlock
    BF_Block_Init(&HashTableBlock);
    CALL_OR_EXIT(BF_GetBlock(*indexDesc,File_Info.ht_block_num,metadataBlock));
    return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
    BF_Block_SetDirty(HashTableBlock);
    CALL_OR_EXIT(BF_UnpinBlock(HashTableBlock));
    BF_Block_Destroy(&HashTableBlock);
    CALL_OR_EXIT(BF_CloseFile(indexDesc));
    return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_DeleteEntry(int indexDesc, int id) {
  //insert code here
  return HT_OK;
}
