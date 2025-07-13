#include "BPlusTree.h"
#include <stdio.h>
#include <cstring>

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    IndexId searchIndex;
    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    int block = -1;
    int index = -1;
    
    if(searchIndex.block == -1 || searchIndex.index == -1) {
        block = attrCatEntry.rootBlock;
        index = 0;

        if(attrCatEntry.rootBlock == -1) {
            return RecId{-1, -1};
        }
    }
    else {
        block = searchIndex.block;
        index = searchIndex.index + 1;

        IndLeaf leaf(block);
        
        HeadInfo leafHead;
        leaf.getHeader(&leafHead);

        if(index >= leafHead.numEntries) {
            block = leafHead.rblock;
            index = 0;

            if(block == -1) {
                return RecId{-1, -1};
            }
        }
    }

    while(StaticBuffer::getStaticBlockType(block) == IND_INTERNAL) {
        IndInternal internalBlk(block);

        HeadInfo intHead;
        internalBlk.getHeader(&intHead);

        struct InternalEntry intEntry;

        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

        if(op == NE || op == LT || op == LE) {
            internalBlk.getEntry(&intEntry, 0);
            block = intEntry.lChild;
        }
        else {
            int i=0;
            for(i; i<intHead.numEntries; i++) {
                internalBlk.getEntry(&intEntry, i);
                
                int x = compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType);

                if((op == EQ && x >= 0) ||
                    (op == GE && x >= 0) ||
                    (op == GT && x > 0)) {
                    break;
                }
            }

            if(i != intHead.numEntries) {
                block = intEntry.lChild;
            }
            else {
                block = intEntry.rChild;
            }
        }
    }

    while(block != -1) {
        IndLeaf leafBlk(block);

        HeadInfo leafHead;
        leafBlk.getHeader(&leafHead);

        Index leafEntry;

        while(index < leafHead.numEntries) {
            leafBlk.getEntry(&leafEntry, index);

            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType);

            if(
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0)
            ) {
                searchIndex = IndexId{block, index};
                AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);
                return RecId{leafEntry.block, leafEntry.slot};
            }
            else if((op == EQ || op == LE || op == LT) && cmpVal > 0) {
                return RecId{-1, -1};
            }
            index++;
        }

        if(op != NE) {
            break;
        }

        block = leafHead.rblock;
        index = 0;
    }

    return RecId{-1, -1};
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]) {
    if(relId == RELCAT_RELID || relId ==  ATTRCAT_RELID) {
        return E_NOTPERMITTED;
    }

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if(ret != SUCCESS) {
        return ret;
    }

    if(attrCatEntry.rootBlock != -1) {
        return SUCCESS;
    }

    IndLeaf rootBlockBuf;

    int rootBlock = rootBlockBuf.getBlockNum();
    if(rootBlock == E_DISKFULL) {
        return rootBlock;
    }

    attrCatEntry.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int block = relCatEntry.firstBlk;
    while(block != -1) {
        RecBuffer buf(block);

        unsigned char slotMap[relCatEntry.numSlotsPerBlk];
        buf.getSlotMap(slotMap);
        
        for(int i=0; i<relCatEntry.numSlotsPerBlk; i++) {
            if(slotMap[i] == SLOT_OCCUPIED) {
                Attribute record[relCatEntry.numAttrs];
                buf.getRecord(record, i);
                RecId recId = {block, i};
                int retVal = bPlusInsert(relId, attrName, record[attrCatEntry.offset], recId);
                //AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
                if(retVal == E_DISKFULL) {
                    return retVal;
                }
            }
        }
        struct HeadInfo head;
        buf.getHeader(&head);
        block = head.rblock;
    }
    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum) {
    if(rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }

    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if(type == IND_LEAF) {
        IndLeaf leaf(rootBlockNum);
        leaf.releaseBlock();

        return SUCCESS;
    }
    else if(type == IND_INTERNAL) {
        IndInternal internal(rootBlockNum);
        struct HeadInfo head;
        internal.getHeader(&head);

        InternalEntry internalEntry;
        for(int i=0; i<head.numEntries; i++) {
            internal.getEntry(&internalEntry, i);
            bPlusDestroy(internalEntry.lChild);
        }
        bPlusDestroy(internalEntry.rChild);
        internal.releaseBlock();

        return SUCCESS;
    }
    else {
        return E_INVALIDBLOCK;
    }
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId) {
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if(ret != SUCCESS) {
        return SUCCESS;
    }
    
    int blockNum = attrCatEntry.rootBlock;
    if(blockNum == -1) {
        return E_NOINDEX;
    }

    int leafBlkNum = findLeafToInsert(blockNum, attrVal, attrCatEntry.attrType);

    struct Index entry;
    entry.attrVal = attrVal;
    entry.block = recId.block;
    entry.slot = recId.slot;
    ret = insertIntoLeaf(relId, attrName, leafBlkNum, entry);
    if(ret == E_DISKFULL) {
        bPlusDestroy(blockNum);
        attrCatEntry.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
        return ret;
    }
    
    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType) {
    int blockNum = rootBlock;
    while(StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF) {
        IndInternal internal(blockNum);
        struct HeadInfo head;
        internal.getHeader(&head);
        InternalEntry internalEntry;
        int i = 0;
        for(i; i<head.numEntries; i++) {
            internal.getEntry(&internalEntry, i);
            int cmpVal = compareAttrs(internalEntry.attrVal, attrVal, attrType);
            if(cmpVal >= 0) {
                break;
            }
        }

        if(i == head.numEntries) {
            blockNum = internalEntry.rChild;
        }
        else {
            blockNum = internalEntry.lChild;
        }
    }

    return blockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry) {
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    IndLeaf indLeaf(blockNum);

    struct HeadInfo blockHeader;
    indLeaf.getHeader(&blockHeader);

    Index indices[blockHeader.numEntries + 1];

    Index index;
    int i = 0;
    int x = 0;
    int flag = 0;
    for(i; i<blockHeader.numEntries; i++) {
        indLeaf.getEntry(&index, i);
        if(flag == 0) {
            if(compareAttrs(indexEntry.attrVal, index.attrVal, attrCatEntry.attrType) >= 0) {
                indices[x++] = index;
            }
            else{
                indices[x++] = indexEntry;
                indices[x++] = index;
                flag = 1;
            }
        }
        else {
            indices[x++] = index;
        }
    }
    if(i == blockHeader.numEntries && flag == 0) {
        indices[x++] = indexEntry;
        flag = 1;
    }

    if(blockHeader.numEntries != MAX_KEYS_LEAF) {
        blockHeader.numEntries++;
        indLeaf.setHeader(&blockHeader);
        for(int i=0; i<blockHeader.numEntries; i++) {
            indLeaf.setEntry(&indices[i], i);
        }

        return SUCCESS;
    }

    int newRightBlk = splitLeaf(blockNum, indices);
    if(newRightBlk == E_DISKFULL) {
        return E_DISKFULL;
    }

    if(blockHeader.pblock != -1) {
        InternalEntry entry;
        entry.attrVal = indices[31].attrVal;
        entry.lChild = blockNum;
        entry.rChild = newRightBlk;
        int ret = insertIntoInternal(relId, attrName, blockHeader.pblock, entry);
        if(ret != SUCCESS) {
            return ret;
        }
    }
    else {
        int ret = createNewRoot(relId, attrName, indices[31].attrVal, blockNum, newRightBlk);
        if(ret != SUCCESS) {
            return ret;
        }
    }

    return SUCCESS;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]) {
    IndLeaf rightBlk;
    IndLeaf leftBlk(leafBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = leafBlockNum;

    if(rightBlkNum == E_DISKFULL) {
        return E_DISKFULL;
    }

    struct HeadInfo leftBlkHeader, rightBlkHeader;
    rightBlk.getHeader(&rightBlkHeader);
    leftBlk.getHeader(&leftBlkHeader);

    rightBlkHeader.numEntries = 32;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlkHeader.lblock = leftBlkNum;
    rightBlkHeader.rblock = leftBlkHeader.rblock;
    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = 32;
    leftBlkHeader.rblock = rightBlkNum;
    leftBlk.setHeader(&leftBlkHeader);

    for(int i=0; i<32; i++) {
        leftBlk.setEntry(&indices[i], i);
        rightBlk.setEntry(&indices[i+32], i);
    }

    return rightBlkNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry) {
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    IndInternal indBlk(intBlockNum);

    struct HeadInfo blockHeader;
    indBlk.getHeader(&blockHeader);

    InternalEntry internalEntries[blockHeader.numEntries + 1];
    
    InternalEntry internalEntry;
    int i = 0;
    int x = 0;
    int flag = 0;
    for(i; i<blockHeader.numEntries; i++) {
        indBlk.getEntry(&internalEntry, i);
        if(flag == 0) {
            if(compareAttrs(intEntry.attrVal, internalEntry.attrVal, attrCatEntry.attrType) > 0) {
                internalEntries[x++] = internalEntry;
            }
            else {
                internalEntry.lChild = intEntry.rChild;
                internalEntries[x++] = intEntry;
                internalEntries[x++] = internalEntry;
                flag = 1;
            }
        }
        else {
            internalEntries[x++] = internalEntry;
        }
    }
     if(blockHeader.numEntries == i && flag == 0) {
        internalEntries[x++] = intEntry;
        flag = 0;
    }

    if(blockHeader.numEntries != MAX_KEYS_INTERNAL) {
        blockHeader.numEntries++;
        indBlk.setHeader(&blockHeader);

        for(int i=0; i<blockHeader.numEntries; i++) {
            indBlk.setEntry(&internalEntries[i], i);
        }

        return SUCCESS;
    }
    
    int newRightBlk = splitInternal(intBlockNum, internalEntries);
    if(newRightBlk == E_DISKFULL) {
        bPlusDestroy(intEntry.rChild);
        return E_DISKFULL;
    }

    if(blockHeader.pblock != -1) {
        struct InternalEntry x;
        x.lChild = intBlockNum;
        x.rChild = newRightBlk;
        x.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        int ret = insertIntoInternal(relId, attrName, blockHeader.pblock, x);
        if(ret != SUCCESS) {
            return ret;
        }
    }
    else{
        int ret = createNewRoot(relId, attrName, internalEntries[50].attrVal, intBlockNum, newRightBlk);
        if(ret != SUCCESS) {
            return ret;
        }
    }

    return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[]) {
    IndInternal rightBlk;
    IndInternal leftBlk(intBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = intBlockNum;

    if(rightBlkNum == E_DISKFULL) {
        return E_DISKFULL;
    }

    struct HeadInfo leftBlkHeader, rightBlkHeader;
    rightBlk.getHeader(&rightBlkHeader);
    leftBlk.getHeader(&leftBlkHeader);

    rightBlkHeader.numEntries = 50;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = 50;
    leftBlk.setHeader(&leftBlkHeader);

    for(int i=0; i<50; i++) {
        leftBlk.setEntry(&internalEntries[i], i);
        rightBlk.setEntry(&internalEntries[i+51], i);
    }

    //int type = StaticBuffer::getStaticBlockType(internalEntries[0].lChild);

    InternalEntry y;
    for(int i=0; i<rightBlkHeader.numEntries; i++) {
        rightBlk.getEntry(&y, i);
        BlockBuffer left(y.lChild);
        struct HeadInfo lHead;
        left.getHeader(&lHead);
        lHead.pblock = rightBlkNum;
        left.setHeader(&lHead);
    }
    BlockBuffer right(y.rChild);
    struct HeadInfo rHead;
    right.getHeader(&rHead);
    rHead.pblock = rightBlkNum;
    right.setHeader(&rHead);
    /**/

    return rightBlkNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild) {
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    IndInternal newRootBlk;
    int newRootBlkNum = newRootBlk.getBlockNum();

    if(newRootBlkNum == E_DISKFULL) {
        bPlusDestroy(rChild);
        return E_DISKFULL;
    }

    struct HeadInfo head;
    newRootBlk.getHeader(&head);
    head.numEntries = 1;
    newRootBlk.setHeader(&head);

    struct InternalEntry intEntry;
    intEntry.lChild = lChild;
    intEntry.attrVal = attrVal;
    intEntry.rChild = rChild;

    newRootBlk.setEntry(&intEntry, 0);

    BlockBuffer left(lChild);
    struct HeadInfo lHead;
    left.getHeader(&lHead);
    lHead.pblock = newRootBlkNum;
    left.setHeader(&lHead);
    
    BlockBuffer right(rChild);
    struct HeadInfo rHead;
    right.getHeader(&rHead);
    rHead.pblock = newRootBlkNum;
    right.setHeader(&rHead);

    attrCatEntry.rootBlock = newRootBlkNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    return SUCCESS;
}