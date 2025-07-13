#include "BlockBuffer.h"
#include <cstddef>
#include <cstdlib>
#include <cstring>


BlockBuffer::BlockBuffer(int blockNum) {
  
  this->blockNum=blockNum;
}

BlockBuffer::BlockBuffer(char blockType) {
  int blocktype;

  if(blockType == 'R') {
    blocktype = 0;
  }
  else if(blockType == 'I') {
    blocktype = 1;
  }
  else if(blockType == 'L') {
    blocktype = 2;
  } 

  int blockNum = BlockBuffer::getFreeBlock(blocktype);
  this->blockNum = blockNum;
}


int BlockBuffer::getHeader(struct HeadInfo *head) {

  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }


  memcpy(&head->numSlots, bufferPtr + 24, 4);
  memcpy(&head->numEntries, bufferPtr + 16, 4);
  memcpy(&head->numAttrs, bufferPtr + 20, 4);
  memcpy(&head->rblock, bufferPtr + 12, 4);
  memcpy(&head->lblock, bufferPtr + 8, 4);
  memcpy(&head->pblock, bufferPtr + 4, 4);
  memcpy(&head->blockType, bufferPtr, 4);

  return SUCCESS;
}


int RecBuffer::getRecord(union Attribute *rec, int slotNum) {

  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret != SUCCESS){
    return ret;
  }

  struct HeadInfo head;
  this->getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;



  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = bufferPtr + (32 + slotCount + (recordSize * slotNum));

  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {

  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  if(bufferNum != E_BLOCKNOTINBUFFER) {

    for(int i=0; i<BUFFER_CAPACITY; i++) {
      StaticBuffer::metainfo[i].timeStamp++;
    }
    StaticBuffer::metainfo[bufferNum].timeStamp = 0;
  }
  else {
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);
    if(bufferNum == E_OUTOFBOUND) {
      return bufferNum;
    }
    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }

  *buffPtr = StaticBuffer::blocks[bufferNum];

  return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {

  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  this->getHeader(&head);
  int numOfAttrs = head.numAttrs;
  int numOfSlots = head.numSlots;

  if(slotNum < 0 || slotNum >=numOfSlots) {
    return E_OUTOFBOUND;
  }

  int recordSize = numOfAttrs * ATTR_SIZE;
  unsigned char *slotPointer = bufferPtr + (32 + numOfSlots + (recordSize * slotNum));

   memcpy(slotPointer, rec, recordSize);

   ret = StaticBuffer::setDirtyBit(this->blockNum);

   return SUCCESS;
}

int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;

  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  this->getHeader(&head);

  int slotCount = head.numSlots;

  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

  memcpy(slotMap, slotMapInBuffer, slotCount);

  return SUCCESS;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {
  return attrType == NUMBER ? (attr1.nVal < attr2.nVal ? -1 : (attr1.nVal > attr2.nVal ? 1 : 0)) : strcmp(attr1.sVal, attr2.sVal) ;
}

int BlockBuffer::setHeader(struct HeadInfo *head) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;
  bufferHeader->blockType = head->blockType;
  bufferHeader->pblock = head->pblock;
  bufferHeader->lblock = head->lblock;
  bufferHeader->rblock = head->rblock;
  bufferHeader->numEntries = head->numEntries;
  bufferHeader->numAttrs = head->numAttrs;
  bufferHeader->numSlots = head->numSlots;

  ret = StaticBuffer::setDirtyBit(this->blockNum);
  return ret;
}

int BlockBuffer::setBlockType(int blockType) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret != SUCCESS) {
    return ret;
  }

  *((int32_t *)bufferPtr) = blockType;

  StaticBuffer::blockAllocMap[this->blockNum] = blockType;
  ret = StaticBuffer::setDirtyBit(this->blockNum);
  return ret;
}

int BlockBuffer::getFreeBlock(int blockType) {
  int blockNum=-1;
  for(int i=0; i<DISK_BLOCKS; i++) {
    if(StaticBuffer::blockAllocMap[i] == UNUSED_BLK) {
      blockNum = i;
      break;
    }
  }
  if(blockNum == -1) {
    return E_DISKFULL;
  }
  this->blockNum = blockNum;

  int bufferNum = StaticBuffer::getFreeBuffer(blockNum);
  if(bufferNum < 0 || bufferNum >= BUFFER_CAPACITY) {
    return bufferNum;
  }

  struct HeadInfo head;
  head.pblock = -1;
  head.lblock = -1;
  head.rblock = -1;
  head.numEntries = 0;
  head.numAttrs = 0;
  head.numSlots = 0;
  int ret = BlockBuffer::setHeader(&head);
  if(ret != SUCCESS) {
    return ret;
  }

  ret = BlockBuffer::setBlockType(blockType);
  if(ret != SUCCESS) {
    return ret;
  }

  return blockNum;
}

int RecBuffer::setSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  this->getHeader(&head);

  int numSlots = head.numSlots;

  memcpy(bufferPtr + HEADER_SIZE, slotMap, numSlots);

  ret = StaticBuffer::setDirtyBit(this->blockNum);

  return ret;
}

int BlockBuffer::getBlockNum() {
  if(this->blockNum < 0 || this->blockNum >= DISK_BLOCKS) {
    return E_DISKFULL;
  }
  return this->blockNum;
}

void BlockBuffer::releaseBlock() {
  if(this->blockNum == INVALID_BLOCKNUM || StaticBuffer::blockAllocMap[this->blockNum] == UNUSED_BLK) {
    return;
  }

  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);
  if(bufferNum != E_BLOCKNOTINBUFFER) {
    StaticBuffer::metainfo[bufferNum].free = true;
  }

  StaticBuffer::blockAllocMap[this->blockNum] = UNUSED_BLK;
  
  this->blockNum = INVALID_BLOCKNUM;
}



IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType) {}
IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum) {}

IndInternal::IndInternal() : IndBuffer('I') {}
IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum) {}

IndLeaf::IndLeaf() : IndBuffer('L') {}
IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum) {}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer(blockNum) {}
RecBuffer::RecBuffer() : BlockBuffer('R'){}

int IndInternal::getEntry(void *ptr, int indexNum) {
  if(indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) {
    return E_OUTOFBOUND;
  }

  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret != SUCCESS) {
    return ret;
  }

  struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

  unsigned char * entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);
  memcpy(&(internalEntry->lChild), entryPtr, 4);
  memcpy(&(internalEntry->attrVal), entryPtr +4, 16);
  memcpy(&(internalEntry->rChild), entryPtr + 20, 4);

  return SUCCESS;
}

int IndLeaf::getEntry(void *ptr, int indexNum) {
  if(indexNum < 0 || indexNum >= MAX_KEYS_LEAF) {
    return E_OUTOFBOUND;
  }

  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret != SUCCESS) {
    return ret;
  }

  unsigned char * entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 32);
  memcpy((struct Index *) ptr, entryPtr, LEAF_ENTRY_SIZE);

  return SUCCESS;
}

int IndInternal::setEntry(void *ptr, int indexNum) {
  if(indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL) {
    return E_OUTOFBOUND;
  }

  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret != SUCCESS) {
    return ret;
  }

  struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

  unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);
  memcpy(entryPtr, &(internalEntry->lChild), 4);
  memcpy(entryPtr + 4, &(internalEntry->attrVal), 16);
  memcpy(entryPtr + 20, &(internalEntry->rChild), 4);

  ret = StaticBuffer::setDirtyBit(this->blockNum);

  return ret;
}

int IndLeaf::setEntry(void *ptr, int indexNum) {
  if(indexNum < 0 || indexNum >= MAX_KEYS_LEAF) {
    return E_OUTOFBOUND;
  }

  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret != SUCCESS) {
    return ret;
  }

  unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
  memcpy(entryPtr, (struct Index*)ptr, LEAF_ENTRY_SIZE);

  ret = StaticBuffer::setDirtyBit(this->blockNum);

  return ret;
}