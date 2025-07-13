#include "StaticBuffer.h"
#include <stdio.h>

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

StaticBuffer::StaticBuffer() {
    StaticBuffer::init();
}

StaticBuffer::~StaticBuffer() {

    unsigned char buffer[BLOCK_SIZE];

    int slot = 0;
    for(int i=0; i<BLOCK_ALLOCATION_MAP_SIZE; i++) {
        for(int j=0; j<BLOCK_SIZE; j++) {
            buffer[j] = StaticBuffer::blockAllocMap[slot];
            slot++;
        }

        Disk::writeBlock(buffer, i);
    }

    for(int i=0; i<BUFFER_CAPACITY; i++) {
        if(metainfo[i].free == false && metainfo[i].dirty == true) {
            Disk::writeBlock(blocks[i], metainfo[i].blockNum);
        }
    }
}

int StaticBuffer::getFreeBuffer(int blockNum) {

    if(blockNum < 0 || blockNum >= DISK_BLOCKS){
        return E_OUTOFBOUND;
    }

    for(int i=0; i<BUFFER_CAPACITY; i++) {
        if(metainfo[i].free == false) {
            metainfo[i].timeStamp++;
        }
    }

    int bufferNum = -1;
    int timeStamp = -1;
    int timeIndex = -1;
    int i=0;
    for(i; i<BUFFER_CAPACITY; i++) {
        if(metainfo[i].free == true) {
            bufferNum = i;
            break;
        }
        if(metainfo[i].timeStamp>timeStamp) {
            timeStamp = metainfo[i].timeStamp;
            timeIndex = i;
        }
    }

    if(bufferNum == -1) {
        if(metainfo[timeIndex].dirty == true) {
            Disk::writeBlock(blocks[timeIndex], metainfo[timeIndex].blockNum);
        }
        bufferNum = timeIndex;
    }

    metainfo[bufferNum].free = false;
    metainfo[bufferNum].dirty = false;
    metainfo[bufferNum].timeStamp = 0;
    metainfo[bufferNum].blockNum = blockNum;

    return bufferNum;
}

int StaticBuffer::getBufferNum(int blockNum) {
    if(blockNum<0 || blockNum>=DISK_SIZE) {
        return E_OUTOFBOUND;
    }

    for(int i=0; i<BUFFER_CAPACITY; i++){
        if(metainfo[i].blockNum == blockNum){
            return i;
        }
    }

    return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum) {
    int bufferNum = StaticBuffer::getBufferNum(blockNum);
    if(bufferNum == E_BLOCKNOTINBUFFER || bufferNum == E_OUTOFBOUND) {
        return bufferNum;
    }

    metainfo[bufferNum].dirty = true;

    return SUCCESS;
}

int StaticBuffer::getStaticBlockType(int blockNum) {
    if(blockNum < 0 || blockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }

    int blockType = (int)blockAllocMap[blockNum];

    return blockType;
}

void StaticBuffer::init() {
    int k = 0;
    unsigned char buffer[BLOCK_SIZE];
    for(int i=0; i<4; i++) {
        Disk::readBlock(buffer, i);
        for(int j=0; j<BLOCK_SIZE; j++) {
            StaticBuffer::blockAllocMap[k] = buffer[j];
            k++;
        }
    }

    for(int bufferIndex=0; bufferIndex<BUFFER_CAPACITY; bufferIndex++) {
        metainfo[bufferIndex].free = true;
        metainfo[bufferIndex].dirty = false;
        metainfo[bufferIndex].timeStamp = -1;
        metainfo[bufferIndex].blockNum = -1;
    }
}