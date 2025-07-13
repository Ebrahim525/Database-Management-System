#include "Disk.h"

#include <fstream>
#include <iostream>
#include <cstring>

#include "../Buffer/BlockBuffer.h"

#include "../define/constants.h"

/*
 * Used to make a temporary copy of the disk contents before the starting of a new session.
 * This ensures that if the system has a forced shutdown during the course of the session,
 * the previous state of the disk is not lost.
*/
Disk::Disk() {
  std::ifstream src(DISK_PATH, std::ios::binary);
  std::ofstream dst(DISK_RUN_COPY_PATH, std::ios::binary);

  dst << src.rdbuf();
  src.close();
  dst.close();
}

/*
 * Used to update the changes made to the disk on graceful termination of the latest session.
*/
Disk::~Disk() {
  std::ifstream src(DISK_RUN_COPY_PATH, std::ios::binary);
  std::ofstream dst(DISK_PATH, std::ios::binary);

  dst << src.rdbuf();
  src.close();
  dst.close();
}

/*
 * Used to Read a specified block from disk
 * block - Memory pointer of the buffer to which the block contents is to be loaded/read.
 *         (MUST be Allocated by caller)
 * blockNum - Block number of the disk block to be read.
*/
int Disk::readBlock(unsigned char *block, int blockNum) {
  FILE *disk = fopen(DISK_RUN_COPY_PATH, "rb");
  if (blockNum < 0 || blockNum > DISK_BLOCKS - 1) {
    return E_OUTOFBOUND;
  }
  const int offset = blockNum * BLOCK_SIZE;
  fseek(disk, offset, SEEK_SET);
  fread(block, BLOCK_SIZE, 1, disk);
  fclose(disk);
  return SUCCESS;
}

/*
 * Used to Write a specified block from disk
 * block - Memory pointer of the buffer to which contain the contents to be written.
 *         (MUST be Allocated by caller)
 * blockNum - Block number of the disk block to be written into.
*/
int Disk::writeBlock(unsigned char *block, int blockNum) {
  FILE *disk = fopen(DISK_RUN_COPY_PATH, "rb+");
  if (blockNum < 0 || blockNum > DISK_BLOCKS - 1) {
    return E_OUTOFBOUND;
  }
  const int offset = blockNum * BLOCK_SIZE;
  fseek(disk, offset, SEEK_SET);
  fwrite(block, BLOCK_SIZE, 1, disk);
  fclose(disk);
  return SUCCESS;
}

int Disk::createDisk() {
  FILE *disk = fopen(DISK_RUN_COPY_PATH, "wb+");
  
  if(disk == nullptr)
    return FAILURE;
  
  fseek(disk, 0, SEEK_SET);

  // 2048*8192 bytes = 16MB

  for(int i=0; i<DISK_SIZE; i++) {
    fputc(0, disk);
  }

  fclose(disk);

  return SUCCESS;
}


int Disk::formatDisk() {
  FILE *disk = fopen(DISK_RUN_COPY_PATH, "rb+");
  if(disk == nullptr)
    return FAILURE;
  
    fseek(disk, 0, SEEK_SET);

    unsigned char blockAllocationMap[BLOCK_SIZE*BLOCK_ALLOCATION_MAP_SIZE];

    //Revserved block entries
    int reservedBlocks = 6; // 4-BlockAllocationMap, 2-Ralation and Attribute Catalogs
    for(int i=0; i<reservedBlocks; i++) {
      if(i < 4)
        blockAllocationMap[i] = (unsigned char) BMAP;
      else
        blockAllocationMap[i] = (unsigned char) REC;
    }

    //Populating Reaming entries
    for(int i=reservedBlocks; i<BLOCK_SIZE*BLOCK_ALLOCATION_MAP_SIZE; i++) {
      blockAllocationMap[i] = (unsigned char) UNUSED_BLK;
    }

    //writing this to the disk
    fwrite(blockAllocationMap, BLOCK_SIZE*BLOCK_ALLOCATION_MAP_SIZE, 1, disk);

    //rest of the block is initialised in createDisk which is called with formatDisk

    fclose(disk);

    return SUCCESS;
}


int Disk::addMetaData() {
  Attribute record[ATTR_SIZE];


  //RELATIONCATALOG

  //Setting header for RELCATALOG
  unsigned char block[BLOCK_SIZE];
  Disk::readBlock(block, RELCAT_BLOCK);
  struct HeadInfo head;
  head.blockType = REC;
  head.pblock = -1;
  head.rblock = -1;
  head.lblock = -1;
  head.numEntries = 2;
  head.numAttrs = RELCAT_NO_ATTRS;
  head.numSlots = SLOTMAP_SIZE_RELCAT_ATTRCAT;
  memcpy(block, &head, HEADER_SIZE);

  //Settging slot allocation map for RELCATALOG
  unsigned char slotMap[SLOTMAP_SIZE_RELCAT_ATTRCAT];
  for(int i=0; i<SLOTMAP_SIZE_RELCAT_ATTRCAT; i++) {
    if(i < 2)
      slotMap[i] = SLOT_OCCUPIED;
    else
      slotMap[i] = SLOT_UNOCCUPIED;
  }
  memcpy(block+HEADER_SIZE, slotMap, SLOTMAP_SIZE_RELCAT_ATTRCAT);
  
  //Setting 2 record in RELCATALOG

  strcpy(record[0].sVal, "RELATIONCAT");
  record[1].nVal = 6;
  record[2].nVal = 2;
  record[3].nVal = 4;
  record[4].nVal = 4;
  record[5].nVal = 20;
  memcpy(block+HEADER_SIZE+SLOTMAP_SIZE_RELCAT_ATTRCAT, record, ATTR_SIZE*RELCAT_NO_ATTRS);
  //relCat.setRecord(record, RELCAT_SLOTNUM_FOR_RELCAT);

  strcpy(record[0].sVal, "ATTRIBUTECAT");
  record[1].nVal = 6;
  record[2].nVal = 12;
  record[3].nVal = 5;
  record[4].nVal = 5;
  record[5].nVal = 20;
  memcpy(block+HEADER_SIZE+SLOTMAP_SIZE_RELCAT_ATTRCAT+(ATTR_SIZE*RELCAT_NO_ATTRS), record, ATTR_SIZE*RELCAT_NO_ATTRS);
  // relCat.setRecord(record, RELCAT_SLOTNUM_FOR_ATTRCAT);
  Disk::writeBlock(block, RELCAT_BLOCK);


  //ATTRIBUTE CATALOG

  //Setting header for ATTRICAT
  Disk::readBlock(block, ATTRCAT_BLOCK);
  head.blockType = REC;
  head.pblock = -1;
  head.rblock = -1;
  head.lblock = -1;
  head.numEntries = 12;
  head.numAttrs = ATTRCAT_NO_ATTRS;
  head.numSlots = SLOTMAP_SIZE_RELCAT_ATTRCAT;
  memcpy(block, &head, HEADER_SIZE);

  //Settging slot allocation map for ATTRICAT
  for(int i=0; i<SLOTMAP_SIZE_RELCAT_ATTRCAT; i++) {
    if(i < 12)
      slotMap[i] = SLOT_OCCUPIED;
    else
      slotMap[i] = SLOT_UNOCCUPIED;
  }
  memcpy(block+HEADER_SIZE, slotMap, SLOTMAP_SIZE_RELCAT_ATTRCAT);

  //Setting Entries for ATTRICAT

  int i = 0;
  int recordSize = ATTRCAT_NO_ATTRS*ATTR_SIZE;
  //RelationCAT Entries
  strcpy(record[0].sVal, "RELATIONCAT");
  strcpy(record[1].sVal, "RelName");
  record[2].nVal = STRING;
  record[3].nVal = -1;
  record[4].nVal = -1;
  record[5].nVal = 0;
  memcpy(block+HEADER_SIZE + SLOTMAP_SIZE_RELCAT_ATTRCAT + (i*recordSize), record, recordSize);
  i++;

  strcpy(record[0].sVal, "RELATIONCAT");
  strcpy(record[1].sVal, "#Attributes");
  record[2].nVal = NUMBER;
  record[3].nVal = -1;
  record[4].nVal = -1;
  record[5].nVal = 1;
  memcpy(block+HEADER_SIZE + SLOTMAP_SIZE_RELCAT_ATTRCAT + (i*recordSize), record, recordSize);
  i++;

  strcpy(record[0].sVal, "RELATIONCAT");
  strcpy(record[1].sVal, "#Records");
  record[2].nVal = NUMBER;
  record[3].nVal = -1;
  record[4].nVal = -1;
  record[5].nVal = 2;
  memcpy(block+HEADER_SIZE + SLOTMAP_SIZE_RELCAT_ATTRCAT + (i*recordSize), record, recordSize);
  i++;

  strcpy(record[0].sVal, "RELATIONCAT");
  strcpy(record[1].sVal, "FirstBlock");
  record[2].nVal = NUMBER;
  record[3].nVal = -1;
  record[4].nVal = -1;
  record[5].nVal = 3;
  memcpy(block+HEADER_SIZE + SLOTMAP_SIZE_RELCAT_ATTRCAT + (i*recordSize), record, recordSize);
  i++;

  strcpy(record[0].sVal, "RELATIONCAT");
  strcpy(record[1].sVal, "LastBlock");
  record[2].nVal = NUMBER;
  record[3].nVal = -1;
  record[4].nVal = -1;
  record[5].nVal = 4;
  memcpy(block+HEADER_SIZE + SLOTMAP_SIZE_RELCAT_ATTRCAT + (i*recordSize), record, recordSize);
  i++;

  strcpy(record[0].sVal, "RELATIONCAT");
  strcpy(record[1].sVal, "#Slots");
  record[2].nVal = NUMBER;
  record[3].nVal = -1;
  record[4].nVal = -1;
  record[5].nVal = 5;
  memcpy(block+HEADER_SIZE + SLOTMAP_SIZE_RELCAT_ATTRCAT + (i*recordSize), record, recordSize);
  i++;


  //AttributeCAT Entries
  strcpy(record[0].sVal, "ATTRIBUTECAT");
  strcpy(record[1].sVal, "RelName");
  record[2].nVal = STRING;
  record[3].nVal = -1;
  record[4].nVal = -1;
  record[5].nVal = 0;
  memcpy(block+HEADER_SIZE + SLOTMAP_SIZE_RELCAT_ATTRCAT + (i*recordSize), record, recordSize);
  i++;

  strcpy(record[0].sVal, "ATTRIBUTECAT");
  strcpy(record[1].sVal, "AttributeName");
  record[2].nVal = STRING;
  record[3].nVal = -1;
  record[4].nVal = -1;
  record[5].nVal = 1;
  memcpy(block+HEADER_SIZE + SLOTMAP_SIZE_RELCAT_ATTRCAT + (i*recordSize), record, recordSize);
  i++;

  strcpy(record[0].sVal, "ATTRIBUTECAT");
  strcpy(record[1].sVal, "AttributeType");
  record[2].nVal = NUMBER;
  record[3].nVal = -1;
  record[4].nVal = -1;
  record[5].nVal = 2;
  memcpy(block+HEADER_SIZE + SLOTMAP_SIZE_RELCAT_ATTRCAT + (i*recordSize), record, recordSize);
  i++;

  strcpy(record[0].sVal, "ATTRIBUTECAT");
  strcpy(record[1].sVal, "PrimaryFlag");
  record[2].nVal = NUMBER;
  record[3].nVal = -1;
  record[4].nVal = -1;
  record[5].nVal = 3;
  memcpy(block+HEADER_SIZE + SLOTMAP_SIZE_RELCAT_ATTRCAT + (i*recordSize), record, recordSize);
  i++;

  strcpy(record[0].sVal, "ATTRIBUTECAT");
  strcpy(record[1].sVal, "RootBlock");
  record[2].nVal = NUMBER;
  record[3].nVal = -1;
  record[4].nVal = -1;
  record[5].nVal = 4;
  memcpy(block+HEADER_SIZE + SLOTMAP_SIZE_RELCAT_ATTRCAT + (i*recordSize), record, recordSize);
  i++;

  strcpy(record[0].sVal, "ATTRIBUTECAT");
  strcpy(record[1].sVal, "#Offset");
  record[2].nVal = NUMBER;
  record[3].nVal = -1;
  record[4].nVal = -1;
  record[5].nVal = 5;
  memcpy(block+HEADER_SIZE + SLOTMAP_SIZE_RELCAT_ATTRCAT + (i*recordSize), record, recordSize);
  i++;

  Disk::writeBlock(block, ATTRCAT_BLOCK);

  return SUCCESS;
}