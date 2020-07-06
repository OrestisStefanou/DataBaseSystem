#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"



int hash_function(int id)
{
  return id % BUCKETS_NUM;
}

#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      return HP_ERROR;        \
    }                         \
  }

HT_ErrorCode HT_Init()
{
  for (int i = 0; i < MAX_OPEN_FILES; i++)
    information[i] = malloc(sizeof(Info));
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int buckets)
{
  if (buckets > BF_BLOCK_SIZE / sizeof(int))
  {
    return HT_ERROR;
  }
  Info temp;
  BF_Block *block, *block1; //block hashtable ptr,hashtable points to block1(block that keeps records)
  char *data;
  BF_CreateFile(filename);
  BF_OpenFile(filename, &temp.file_desc);
  strcpy(temp.str, "HASH FILE");
  temp.num_of_bucket = buckets;
  BF_Block_Init(&block);
  //BF_Block_Init(&block1);
  BF_AllocateBlock(temp.file_desc, block);
  data = BF_Block_GetData(block);
  memcpy(&data[0], &temp, sizeof(temp));
  BF_Block_SetDirty(block);
  BF_UnpinBlock(block);
  BF_Block_Destroy(&block);
  BF_Block_Init(&block);
  BF_AllocateBlock(temp.file_desc, block); //Allocate second block wich has the hashtable
  data = BF_Block_GetData(block);
  int index;
  int block_number;
  Block_Info temp_info;
  char *block_info_data;
  temp_info.counter = 0;
  temp_info.Next_Block_Num = -1;
  int zero=0;
  for (int i = 0; i < buckets; i++)
  {
    //BF_AllocateBlock(temp.file_desc, block1);
    //BF_GetBlockCounter(temp.file_desc, &block_number);
    //temp_info.Block_Num_of_File = block_number - 1;
    //block_info_data = BF_Block_GetData(block1);
    //memcpy(&block_info_data[0], &temp_info, sizeof(Block_Info));
    index = i * sizeof(int);
    memcpy(&data[index],&zero, sizeof(int));
    //BF_Block_SetDirty(block1);
    //BF_UnpinBlock(block1);
  }
  BF_Block_SetDirty(block);
  BF_UnpinBlock(block);
  BF_Block_Destroy(&block);
  //BF_Block_Destroy(&block1);
  BF_CloseFile(temp.file_desc);
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc)
{
  Info temp;
  BF_Block *block;
  char *data;
  int file_desc;
  BF_OpenFile(fileName, indexDesc);
  file_desc = *indexDesc;
  //printf("File desc is:%d\n",temp.file_desc);
  BF_Block_Init(&block);
  BF_GetBlock(file_desc, 0, block);
  data = BF_Block_GetData(block);
  // printf("data[0]=%d\n", data[0]);
  memcpy(&temp, &data[0], sizeof(Info));
  if(strcmp(temp.str,"HASH FILE")!=0)
    return HT_ERROR;
  //printf("Temp str %s\n",temp.str);
  memcpy(information[file_desc], &temp, sizeof(Info));
  //printf("INfo[filedesc] str is %s\n",information[file_desc]->str);
  BF_UnpinBlock(block);
  BF_Block_Destroy(&block);
  //printf("File desc is:%d ,string is %s buckets:%d",information[file_desc]->file_desc,information[file_desc]->str,information[file_desc]->num_of_bucket);
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc)
{
  //strcpy(information[indexDesc],"");
  free(information[indexDesc]);
  information[indexDesc] = malloc(sizeof(Info));
  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record)
{
  BF_Block *hashtable, *block, *chain_block,*block1;
  Block_Info bfb_info;
  int Records_Per_Block = BF_BLOCK_SIZE/sizeof(Record);
  BF_Block_Init(&block);
  char *bucket_data, *data;
  int record_block_data;
  int index = hash_function(record.id);
  BF_Block_Init(&hashtable);
  BF_GetBlock(indexDesc, 1, hashtable); //get hashindex block
  bucket_data = BF_Block_GetData(hashtable);
  //record_block_data = bucket_data[(index) * sizeof(int)];
  memcpy(&record_block_data,&bucket_data[index*sizeof(int)],sizeof(int));
  if(record_block_data==0){
    int block_number;
    char *block_info_data;
    BF_Block_Init(&block1);
    Block_Info temp_info;
    temp_info.counter = 0;
    temp_info.Next_Block_Num = -1;
    BF_AllocateBlock(indexDesc, block1);
    BF_GetBlockCounter(indexDesc, &block_number);
    temp_info.Block_Num_of_File = block_number - 1;
    block_info_data = BF_Block_GetData(block1);
    memcpy(&block_info_data[0], &temp_info, sizeof(Block_Info));
    memcpy(&bucket_data[index*sizeof(int)],&temp_info.Block_Num_of_File, sizeof(int));
    BF_Block_SetDirty(block1);
    BF_UnpinBlock(block1);
    BF_Block_SetDirty(hashtable);
    BF_Block_Destroy(&block1);
  }
  memcpy(&record_block_data,&bucket_data[index*sizeof(int)],sizeof(int));
  //printf("Record block data is:%d\n",record_block_data);
  BF_GetBlock(indexDesc, record_block_data, block);
  data = BF_Block_GetData(block);//data points to the start of blockk we are going to save the record
  memcpy(&bfb_info, &data[0], sizeof(Block_Info));
  //BF_Block_SetDirty(block);//useless??
  //BF_UnpinBlock(block);//useless
  //printf("counter = %d  next block num=%d\n",bfb_info.counter,bfb_info.Next_Block_Num); // den douleuoun sosta oi 2 epomenes while for

  while(bfb_info.Next_Block_Num!=-1){
    BF_Block_Init(&chain_block);
    BF_GetBlock(indexDesc,bfb_info.Next_Block_Num,chain_block);
    data=BF_Block_GetData(chain_block);
    memcpy(&bfb_info,&data[0],sizeof(Block_Info));
    if(bfb_info.Next_Block_Num==-1){
      break;
    }
    BF_Block_SetDirty(chain_block);
    BF_UnpinBlock(chain_block);
    BF_Block_Destroy(&chain_block);
  }

  if(bfb_info.counter<Records_Per_Block){
    int index_to_place=sizeof(Block_Info)+(bfb_info.counter *sizeof(Record));
    memcpy(&data[index_to_place],&record,sizeof(Record));
    bfb_info.counter++;
    memcpy(&data[0],&bfb_info,sizeof(Block_Info));//update block info
    //printf("Record %d with name %s inserted\n",record.id,record.name);
    BF_Block_SetDirty(block);
    BF_UnpinBlock(block);
    BF_UnpinBlock(hashtable);
    BF_Block_Destroy(&block);
    BF_Block_Destroy(&hashtable);
    return HT_OK;
  }
  else{
    if(bfb_info.Next_Block_Num==-1){
      Block_Info new_info;//info to insert in new block
      new_info.counter=0;
      new_info.Next_Block_Num=-1;
      new_info.Block_Num_of_File=bfb_info.Block_Num_of_File;
      char *next_block_data;
      BF_Block *next_block;
      BF_Block_Init(&next_block);
      BF_AllocateBlock(indexDesc,next_block);
      BF_GetBlockCounter(indexDesc, &bfb_info.Next_Block_Num);
      bfb_info.Next_Block_Num--;
      memcpy(&data[0],&bfb_info,sizeof(Block_Info));//update block info of first block
      next_block_data = BF_Block_GetData(next_block);
      memcpy(&next_block_data[0],&new_info,sizeof(Block_Info));
      int index_to_place=sizeof(Block_Info)+(new_info.counter*sizeof(Record));
      memcpy(&next_block_data[index_to_place],&record,sizeof(Record));
      new_info.counter++;
      memcpy(&next_block_data[0],&new_info,sizeof(Block_Info));//update block info of new block
      //printf("Record %d with name %s inserted\n",record.id,record.name);
      BF_Block_SetDirty(next_block);
      BF_UnpinBlock(next_block);
      BF_UnpinBlock(hashtable);
      BF_Block_Destroy(&block);
      BF_Block_Destroy(&hashtable);
      BF_Block_Destroy(&next_block);
      return HT_OK;
    }
  } 
  BF_UnpinBlock(hashtable);
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id)
{
  BF_Block *hashtable;
  BF_Block *block_to_print;
  Record record_to_print;
  Block_Info b_info;
  int block_number;
  int record_index;
  char *hashtable_data;
  char *block_data;
  BF_Block_Init(&hashtable);
  BF_GetBlock(indexDesc, 1, hashtable); //get hashindex block
  hashtable_data = BF_Block_GetData(hashtable);
  if(id==NULL){
    for(int i=0;i<BUCKETS_NUM;i++){
      BF_Block_Init(&block_to_print);
      memcpy(&block_number,&hashtable_data[i*sizeof(int)],sizeof(int));
      BF_GetBlock(indexDesc,block_number,block_to_print);
      block_data=BF_Block_GetData(block_to_print);
      memcpy(&b_info,&block_data[0],sizeof(Block_Info));
      while(1){
        if(b_info.Next_Block_Num==-1){//i am at last chain block
          for(int i=0;i<8;i++){//check all records
            record_index=sizeof(Block_Info)+(i*sizeof(Record));
            memcpy(&record_to_print,&block_data[record_index],sizeof(Record));
            if(record_to_print.id!=-1 && record_to_print.name[0]!=0){//check if is deleted record or if there is no record
              printf("ID:%-12d , name:%-12s ,surname:%-12s ,city:%-12s\n",record_to_print.id,record_to_print.name,record_to_print.surname,record_to_print.city);
            }
          }
          BF_UnpinBlock(block_to_print);
          BF_Block_Destroy(&block_to_print);
          break;//go to next bucket 
        }
        else{//i am not at last chain block 
          for(int i=0;i<8;i++){//check all records
            record_index=sizeof(Block_Info)+(i*sizeof(Record));
            memcpy(&record_to_print,&block_data[record_index],sizeof(Record));
            if(record_to_print.id!=-1 && record_to_print.name[0]!=0){//check if is deleted record or if there is no record
              printf("ID:%-12d , name:%-12s ,surname:%-12s ,city:%-12s\n",record_to_print.id,record_to_print.name,record_to_print.surname,record_to_print.city);
            }
          }
          BF_UnpinBlock(block_to_print);//finished with this block
          BF_GetBlock(indexDesc,b_info.Next_Block_Num,block_to_print);//get next block
          block_data=BF_Block_GetData(block_to_print);
          memcpy(&b_info,&block_data[0],sizeof(Block_Info));//get info of nextblock
        }
      }

    }
  }
  else{
    //int flag=0;
    int hash_index=hash_function(*id);
    BF_Block_Init(&block_to_print);
    memcpy(&block_number,&hashtable_data[hash_index*sizeof(int)],sizeof(int));//get bucket number of record
    BF_GetBlock(indexDesc,block_number,block_to_print);
    block_data=BF_Block_GetData(block_to_print);
    memcpy(&b_info,&block_data[0],sizeof(Block_Info));
      while(1){
        if(b_info.Next_Block_Num==-1){//i am at last chain block
          for(int i=0;i<8;i++){//check all records
            record_index=sizeof(Block_Info)+(i*sizeof(Record));
            memcpy(&record_to_print,&block_data[record_index],sizeof(Record));
            if(record_to_print.id!=-1 && record_to_print.name[0]!=0){//check if is deleted record or if there is no record
              if(record_to_print.id==*id){//record found
                printf("ID:%-12d , name:%-12s ,surname:%-12s ,city:%-12s\n",record_to_print.id,record_to_print.name,record_to_print.surname,record_to_print.city);
                BF_UnpinBlock(block_to_print);
                BF_Block_Destroy(&block_to_print);
                BF_Block_Destroy(&hashtable);
                return HT_OK;
              }
            }
          }
          BF_UnpinBlock(block_to_print);
          BF_Block_Destroy(&block_to_print);
          break;//record not found 
        }
        else{//i am not at last chain block 
          for(int i=0;i<8;i++){//check all records
            record_index=sizeof(Block_Info)+(i*sizeof(Record));
            memcpy(&record_to_print,&block_data[record_index],sizeof(Record));
            if(record_to_print.id!=-1 && record_to_print.name[0]!=0){//check if is deleted record or if there is no record
              if(record_to_print.id == *id){
                printf("ID:%-12d , name:%-12s ,surname:%-12s ,city:%-12s\n",record_to_print.id,record_to_print.name,record_to_print.surname,record_to_print.city);
                BF_UnpinBlock(block_to_print);
                BF_Block_Destroy(&block_to_print);
                BF_Block_Destroy(&hashtable);
                return HT_OK;
              }
            }
          }
          BF_UnpinBlock(block_to_print);//finished with this block
          BF_GetBlock(indexDesc,b_info.Next_Block_Num,block_to_print);//get next block
          block_data=BF_Block_GetData(block_to_print);
          memcpy(&b_info,&block_data[0],sizeof(Block_Info));//get info of nextblock
        }
      }
      BF_Block_Destroy(&hashtable);
      printf("Record not found\n");
      return HT_OK;
  }
  return HT_OK;
}

HT_ErrorCode HT_DeleteEntry(int indexDesc, int id)
{
  /*BF_Block *block, *hashtable;
  char *data, *bucket_data;
  Block_Info bfb_info;
  int record_block_data;
  Record *search_record;
  int flag = 0;
  int chain_Yes_No = 1;
  search_record = (Record *)malloc(sizeof(Record));
  BF_Block_Init(&block);
  BF_Block_Init(&hashtable);
  BF_GetBlock(indexDesc, 1, hashtable); //get hashindex block
  bucket_data = BF_Block_GetData(hashtable);
  BF_Block_SetDirty(hashtable);
  BF_UnpinBlock(hashtable);
  BF_Block_Destroy(&hashtable);
  int index = hash_function(id);
  memcpy(&record_block_data, &bucket_data[index * sizeof(int)], sizeof(int));
  BF_GetBlock(indexDesc, record_block_data, block);
  data = BF_Block_GetData(block); //data points to the start of blockk we are going to save the record
  memcpy(&bfb_info, &data[0], sizeof(Block_Info));
  while (flag == 0 && chain_Yes_No == 1)
  {
    if (bfb_info.Next_Block_Num == -1)
      chain_Yes_No = 0;
    for (int i = 0; i < bfb_info.counter; i++)
    {
      memcpy(search_record, &data[+sizeof(Block_Info) + i * sizeof(Record)], sizeof(Record)); //+
      if (search_record->id == id)
      {
        search_record->id = -1;
        strncpy(search_record->name, "DELETED", 8);
        strncpy(search_record->surname, "DELETED", 8);
        strncpy(search_record->city, "DELETED", 8);
        memcpy(&data[sizeof(Block_Info) + (i * sizeof(Record))], search_record, sizeof(Record)); // +
        bfb_info.counter--;
        memcpy(&data[0], &bfb_info, sizeof(Block_Info));
        BF_Block_SetDirty(block);
        BF_UnpinBlock(block);
        flag = 1;
      }
    }
    if(flag==0){ 
    BF_Block_SetDirty(block);
    BF_UnpinBlock(block);
    if (bfb_info.Next_Block_Num != -1)
      BF_GetBlock(indexDesc, bfb_info.Next_Block_Num, block);

    data = BF_Block_GetData(block);
    memcpy(&bfb_info, &data[0], sizeof(Block_Info));}
  }
  free(search_record);
  //BF_Block_SetDirty(block);
  //BF_UnpinBlock(block);
  BF_Block_Destroy(&block);

  if (flag == 0)
    printf("Record Not Found\n");
  else
    printf("Record DELETED\n");
  return HT_OK;*/
  BF_Block *hashtable;
  BF_Block *block_of_record;
  Record record_to_delete;
  Block_Info b_info;
  int block_number;
  int record_index;
  char *hashtable_data;
  char *block_data;
  BF_Block_Init(&hashtable);
  BF_GetBlock(indexDesc, 1, hashtable); //get hashindex block
  hashtable_data = BF_Block_GetData(hashtable);
  if(id<0){
    printf("Id must be positive integer\n");
    return HT_ERROR;
  }
  else{
    int hash_index=hash_function(id);
    BF_Block_Init(&block_of_record);
    memcpy(&block_number,&hashtable_data[hash_index*sizeof(int)],sizeof(int));//get bucket number of record
    BF_GetBlock(indexDesc,block_number,block_of_record);
    block_data=BF_Block_GetData(block_of_record);
    memcpy(&b_info,&block_data[0],sizeof(Block_Info));
      while(1){
        if(b_info.Next_Block_Num==-1){//i am at last chain block
          for(int i=0;i<8;i++){//check all records
            record_index=sizeof(Block_Info)+(i*sizeof(Record));
            memcpy(&record_to_delete,&block_data[record_index],sizeof(Record));
            if(record_to_delete.id!=-1 && record_to_delete.name[0]!=0){//check if is already deleted record or if there is no record
              if(record_to_delete.id==id){//record found
                //printf("ID:%-12d , name:%-12s ,surname:%-12s ,city:%-12s\n",record_to_delete.id,record_to_delete.name,record_to_delete.surname,record_to_delete.city);
                record_to_delete.id=-1;
                memcpy(&block_data[record_index],&record_to_delete,sizeof(Record));
                BF_Block_SetDirty(block_of_record);
                BF_UnpinBlock(block_of_record);
                BF_Block_Destroy(&block_of_record);
                BF_Block_Destroy(&hashtable);
                printf("Record with id:%d deleted succesfully\n",id);
                return HT_OK;
              }
            }
          }
          BF_UnpinBlock(block_of_record);
          BF_Block_Destroy(&block_of_record);
          break;//record not found 
        }
        else{//i am not at last chain block 
          for(int i=0;i<8;i++){//check all records
            record_index=sizeof(Block_Info)+(i*sizeof(Record));
            memcpy(&record_to_delete,&block_data[record_index],sizeof(Record));
            if(record_to_delete.id!=-1 && record_to_delete.name[0]!=0){//check if is already deleted record or if there is no record
              if(record_to_delete.id == id){//record found
                //printf("ID:%-12d , name:%-12s ,surname:%-12s ,city:%-12s\n",record_to_delete.id,record_to_delete.name,record_to_delete.surname,record_to_delete.city);
                record_to_delete.id=-1;
                memcpy(&block_data[record_index],&record_to_delete,sizeof(Record));
                BF_Block_SetDirty(block_of_record);
                BF_UnpinBlock(block_of_record);
                BF_Block_Destroy(&block_of_record);
                BF_Block_Destroy(&hashtable);
                printf("Record with id:%d deleted succesfully\n",id);
                return HT_OK;
              }
            }
          }
          BF_UnpinBlock(block_of_record);//finished with this block
          BF_GetBlock(indexDesc,b_info.Next_Block_Num,block_of_record);//get next block
          block_data=BF_Block_GetData(block_of_record);
          memcpy(&b_info,&block_data[0],sizeof(Block_Info));//get info of nextblock
        }
      }
      BF_Block_Destroy(&hashtable);
      printf("Record with id:%d not found\n",id);
      return HT_OK;
  }
  return HT_OK;
}
