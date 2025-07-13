#include "BlockAccess.h"
#include <cstring>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>


RecId BlockAccess::linearSearch(int relId, char *attrName, Attribute attrVal, int op) {
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    int block = -1;
    int slot = -1;

    if(prevRecId.block == -1 && prevRecId.slot == -1) {
        RelCatEntry relCatBuf;
        RelCacheTable::getRelCatEntry(relId, &relCatBuf);
        block = relCatBuf.firstBlk;
        slot = 0;
    } 
    else {
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }
    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);
    int offset=attrCatBuf.offset;

    while(block != -1) {
        RecBuffer buffer(block);
        //Attribute bufRecord[RELCAT_NO_ATTRS];

        struct HeadInfo bufHead;
        buffer.getHeader(&bufHead);

        unsigned char slotMap[bufHead.numSlots];
        buffer.getSlotMap(slotMap);

        if(slot >= bufHead.numSlots) {
            block = bufHead.rblock;
            slot = 0;
            continue;
        }

        if(slotMap[slot] == SLOT_UNOCCUPIED) {
            slot++;
            continue;
        }
        
        /*AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);
        int offset=attrCatBuf.offset;*/
        Attribute record[bufHead.numAttrs];
        buffer.getRecord(record, slot);

        int cmpVal = compareAttrs(record[offset], attrVal, attrCatBuf.attrType);

        if((op == NE && cmpVal != 0) || (op == LT && cmpVal < 0) || (op == LE && cmpVal <= 0) || (op == EQ && cmpVal == 0) || (op == GT && cmpVal > 0) || (op == GE && cmpVal >= 0)) {
            RecId newSearchIndex = {block, slot};
            RelCacheTable::setSearchIndex(relId, &newSearchIndex);
            return newSearchIndex;
        }

        slot++;        
    }
    return RecId{-1, -1};
}


int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    char relNamee[16];
    strcpy(relNamee, RELCAT_ATTR_RELNAME); 

    Attribute newRelationName;
    strcpy(newRelationName.sVal, newName);
    RecId search = BlockAccess::linearSearch(RELCAT_RELID, relNamee, newRelationName, EQ);
    if(search.block != -1 && search.slot != -1) {
        return E_RELEXIST;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName;
    stpcpy(oldRelationName.sVal, oldName);
    search = BlockAccess::linearSearch(RELCAT_RELID, relNamee, oldRelationName, EQ);
    if(search.block == -1 && search.slot==-1) {
        return E_RELNOTEXIST;
    }

    RecBuffer CatBuffer(search.block);
    Attribute CatRecord[RELCAT_NO_ATTRS];
    CatBuffer.getRecord(CatRecord, search.slot);
    strcpy(CatRecord[RELCAT_REL_NAME_INDEX].sVal, newName);
    CatBuffer.setRecord(CatRecord, search.slot);


    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    for(int i=0; i<CatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal; i++) {
        search = BlockAccess::linearSearch(ATTRCAT_RELID, relNamee, oldRelationName, EQ);

        RecBuffer attrCatBuf(search.block);
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBuf.getRecord(attrCatRecord, search.slot);
        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, newName);
        attrCatBuf.setRecord(attrCatRecord, search.slot);
    }

    return SUCCESS;
}


int BlockAccess::renameAttribute(char *relName, char *oldName, char *newName) {

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);
    char relNamee[16];
    strcpy(relNamee, RELCAT_ATTR_RELNAME); 
    RecId search = BlockAccess::linearSearch(RELCAT_RELID, relNamee, relNameAttr, EQ);
    if(search.block == -1 && search.slot == -1) {
        return E_RELNOTEXIST;
    }

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];
    while(true) {
        search = BlockAccess::linearSearch(ATTRCAT_RELID, relNamee, relNameAttr, EQ);
        if(search.block == -1 && search.slot == -1) {
            break;
        }
        RecBuffer attrCatBuf(search.block);
        attrCatBuf.getRecord(attrCatEntryRecord, search.slot);

        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0) {
            return E_ATTREXIST;
        }
        
        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0) {
            attrToRenameRecId = search;
        }
        
    }

    if(attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1) {
        return E_ATTRNOTEXIST;
    }

    RecBuffer attrCatBuf(attrToRenameRecId.block);
    attrCatBuf.getRecord(attrCatEntryRecord, attrToRenameRecId.slot);
    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
    attrCatBuf.setRecord(attrCatEntryRecord, attrToRenameRecId.slot);

    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record) {
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int blockNum = relCatEntry.firstBlk;
    RecId recId = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk;
    int numOfAttrs = relCatEntry.numAttrs;

    int prevBlockNum = -1;

    while(blockNum != -1) {
        RecBuffer recBuffer(blockNum);

        struct HeadInfo head;
        recBuffer.getHeader(&head);

        unsigned char *slotMap = (unsigned char *)malloc(sizeof(unsigned char)*numOfSlots);
        recBuffer.getSlotMap(slotMap);

        int i=0;
        for(i; i<numOfSlots; i++) {
            if(slotMap[i] == SLOT_UNOCCUPIED) {
                recId.block = blockNum;
                recId.slot = i;
                break;
            }
        }
        if(i != numOfSlots) {
            break;
        }

        prevBlockNum = blockNum;
        blockNum = head.rblock; 
    }

    if(recId.block == -1 || recId.slot == -1) {
        if(relId == RELCAT_RELID) {
            return E_MAXRELATIONS;
        }

        RecBuffer recBuf;
        int ret = recBuf.getBlockNum();
        if(ret == E_DISKFULL) {
            return ret;
        }

        recId.block = ret;
        recId.slot = 0;

        struct HeadInfo head;
        head.blockType = REC;
        head.pblock = -1;
        head.lblock = prevBlockNum;
        head.rblock = -1;
        head.numEntries = 0;
        head.numSlots = numOfSlots;
        head.numAttrs = numOfAttrs;
        ret = recBuf.setHeader(&head);
        
        unsigned char *slotMap = (unsigned char *)malloc(sizeof(unsigned char)*numOfSlots);
        for(int i=0; i<numOfSlots; i++) {
            slotMap[i] = SLOT_UNOCCUPIED;
        }
        recBuf.setSlotMap(slotMap);

        if(prevBlockNum != -1) {
            RecBuffer prevBuf(prevBlockNum);

            struct HeadInfo prevHead;
            prevBuf.getHeader(&prevHead);
            prevHead.rblock = recId.block;
            prevBuf.setHeader(&prevHead);
        }
        else {
            relCatEntry.firstBlk = recId.block;
        }
        relCatEntry.lastBlk = recId.block;
        //RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }

    RecBuffer recBuf(recId.block);
    recBuf.setRecord(record, recId.slot);

    unsigned char *slotMap = (unsigned char *)malloc(sizeof(unsigned char)*numOfSlots);
    recBuf.getSlotMap(slotMap);
    slotMap[recId.slot] = SLOT_OCCUPIED;
    recBuf.setSlotMap(slotMap);
    
    struct HeadInfo head;
    recBuf.getHeader(&head);
    head.numEntries++;
    recBuf.setHeader(&head);

    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    int flag = SUCCESS;
    
    for(int i=0; i<relCatEntry.numAttrs; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);

        int rootBlock = attrCatEntry.rootBlock;
        if(rootBlock != -1) {
            int retVal = BPlusTree::bPlusInsert(relId, attrCatEntry.attrName, record[attrCatEntry.offset], recId);
            if(retVal == E_DISKFULL) {
                flag = E_INDEX_BLOCKS_RELEASED;
            }
        }
    }

    return flag;
}

int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    RecId recId;

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if(ret != SUCCESS) {
        return ret;
    }
    int rootBlock = attrCatEntry.rootBlock;
    if(rootBlock == -1) {
        recId = BlockAccess::linearSearch(relId, attrName, attrVal, op);
    }
    else {
        recId = BPlusTree::bPlusSearch(relId, attrName, attrVal, op);
    }

    if(recId.block == -1 && recId.slot == -1) {
        return E_NOTFOUND;
    }

    RecBuffer recBuf(recId.block);
    recBuf.getRecord(record, recId.slot);

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }
    
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    char qq[ATTR_SIZE] = "RelName";

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);
    RecId recId;
    recId = BlockAccess::linearSearch(RELCAT_RELID, qq, relNameAttr, EQ);
    if(recId.block == -1 && recId.slot == -1) {
        return E_RELNOTEXIST;
    }
    
    Attribute relCatEntryrecord[RELCAT_NO_ATTRS];
    RecBuffer relCatBuf(recId.block);
    int ret = relCatBuf.getRecord(relCatEntryrecord, recId.slot);
    int firstBlock = relCatEntryrecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
    int numOfAttrs = relCatEntryrecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

    int blockNum = firstBlock;
    while(blockNum != -1) {
        BlockBuffer buf(blockNum);
        struct HeadInfo head;
        buf.getHeader(&head);
        blockNum = head.rblock;
        buf.releaseBlock();
    }


    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    int numOfAttributesDetected = 0;

    while(true) {
        RecId attrCatRecId;
        attrCatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, qq, relNameAttr, EQ);
        if(attrCatRecId.block == -1 && attrCatRecId.slot == -1) {
            break;
        }
        numOfAttributesDetected++;

        RecBuffer attrCatBuf(attrCatRecId.block);
        struct HeadInfo attrhead;
        attrCatBuf.getHeader(&attrhead);
        Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];
        attrCatBuf.getRecord(attrCatEntryRecord, attrCatRecId.slot);

        int rootBlock = (int)attrCatEntryRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal;

        int numOfSlots = attrhead.numSlots;
        unsigned char slotMap[numOfSlots];
        attrCatBuf.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        attrCatBuf.setSlotMap(slotMap);

        attrhead.numEntries--;
        attrCatBuf.setHeader(&attrhead);

        if(attrhead.numEntries == 0) {
            RecBuffer left(attrhead.lblock);
            struct HeadInfo leftHead;
            left.getHeader(&leftHead);
            leftHead.rblock = attrhead.rblock;
            left.setHeader(&leftHead);

            if(attrhead.rblock != -1) {
                RecBuffer right(attrhead.rblock);
                struct HeadInfo rightHead;
                right.getHeader(&rightHead);
                rightHead.lblock = attrhead.lblock;
                right.setHeader(&rightHead);
            }
            else {
                RelCatEntry relCatEntry;
                RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntry);
                relCatEntry.lastBlk = attrhead.lblock;
                RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntry);
            }

            attrCatBuf.releaseBlock();
        }
        
        if(rootBlock != -1) {
            BPlusTree::bPlusDestroy(rootBlock);
        }
    }

    struct HeadInfo relCatHead;
    relCatBuf.getHeader(&relCatHead);
    relCatHead.numEntries--;
    relCatBuf.setHeader(&relCatHead);
    int numOfSlots = relCatHead.numSlots;
    unsigned char slotMap[numOfSlots];
    relCatBuf.getSlotMap(slotMap);
    slotMap[recId.slot] = SLOT_UNOCCUPIED;
    relCatBuf.setSlotMap(slotMap);

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntry);
    relCatEntry.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntry);

    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntry);
    relCatEntry.numRecs -= numOfAttributesDetected;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntry);

    return SUCCESS;    
}

int BlockAccess::project(int relId, Attribute *record) {
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    int block = -1, slot = -1;
    
    if(prevRecId.block == -1 & prevRecId.slot == -1) {
        RelCatEntry relcatBuf;
        RelCacheTable::getRelCatEntry(relId, &relcatBuf);
        block = relcatBuf.firstBlk;
        slot = 0;
    }
    else{
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    while(block != -1) {
        RecBuffer buf(block);
        struct HeadInfo bufHead;
        buf.getHeader(&bufHead);
        unsigned char slotMap[bufHead.numSlots];
        buf.getSlotMap(slotMap);

        if(slot >= bufHead.numSlots) {
            block = bufHead.rblock;
            slot = 0;
            continue;
        }
        if(slotMap[slot] == SLOT_UNOCCUPIED) {
            slot++;
            continue;
        }
        else {
            break;
        }
    }

    if(block == -1) {
        return E_NOTFOUND;
    }

    RecId nextRecId{block, slot};
    
    RelCacheTable::setSearchIndex(relId, &nextRecId);

    RecBuffer buf(block);
    buf.getRecord(record, slot);

    return SUCCESS;
}