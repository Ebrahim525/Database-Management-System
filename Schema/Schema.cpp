#include "Schema.h"
#include <cstdio>
#include <cmath>
#include <cstring>
#include <iostream>

int Schema::openRel(char relname[ATTR_SIZE]) {
    int ret = OpenRelTable::openRel(relname);

    if(ret >= 0) {
        return SUCCESS;
    }

    return ret;
}

int Schema::closeRel(char relname[ATTR_SIZE]) {
    if(strcmp(relname, RELCAT_RELNAME) == 0 || strcmp(relname, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relname);

    if(relId == E_RELNOTOPEN) {
        return relId;
    }

    return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]) {
    if(strcmp(oldRelName, RELCAT_RELNAME) == 0 || strcmp(oldRelName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    if(strcmp(newRelName, RELCAT_RELNAME) == 0 || strcmp(newRelName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    if(OpenRelTable::getRelId(oldRelName) !=E_RELNOTOPEN) {
        return E_RELOPEN;
    }

    int retVal = BlockAccess::renameRelation(oldRelName, newRelName);

    return retVal;
}

int Schema::renameAttr(char relName[ATTR_SIZE], char oldAttrName[ATTR_SIZE], char newAttrName[ATTR_SIZE]) {
    if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    if(OpenRelTable::getRelId(relName) !=E_RELNOTOPEN) {
        return E_RELOPEN;
    }

    int retVal = BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);

    return retVal;
}

int Schema::createRel(char relName[], int nAttrs, char attrs[][ATTR_SIZE], int attrtype[]) {
    Attribute relNameAsAttribute;
    strcpy(relNameAsAttribute.sVal, relName);

    RecId targetRelid;

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    char qq[ATTR_SIZE] = "RelName";
    targetRelid = BlockAccess::linearSearch(RELCAT_RELID, qq, relNameAsAttribute, EQ);
    if(targetRelid.block != -1 && targetRelid.slot != -1) {
        return E_RELEXIST;
    }

    for(int i=0; i<nAttrs-1; i++) {
        for(int j=i+1; j<nAttrs; j++){
            if(strcmp(attrs[i], attrs[j]) == 0) {
                return E_DUPLICATEATTR;
            }
        }
    }

    Attribute relCatRecord[RELCAT_NO_ATTRS];
    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);
    relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
    relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
    relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor(2016/(16*nAttrs + 1));

    int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);
    if(retVal != SUCCESS) {
        return retVal;
    }

    for(int i=0; i<nAttrs; i++) {
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
        strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[i]);
        attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrtype[i];
        attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;

        retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
        if(retVal != SUCCESS) {
            Schema::deleteRel(relName);
            return E_DISKFULL;
        }
    }

    return SUCCESS;
}

int Schema::deleteRel(char *relName) {
    if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);
    if(relId != E_RELNOTOPEN) {
        return E_RELOPEN;
    }

    int ret = BlockAccess::deleteRelation(relName);

    return ret;
}

int Schema::createIndex(char relName[ATTR_SIZE], char attrName[ATTR_SIZE]) {
    if(strcmp(relName, "RELATIONCAT") == 0 || strcmp(relName, "ATTRIBUTECAT") == 0) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);
    if(relId == E_RELNOTOPEN) {
        return relId;
    }

    return BPlusTree::bPlusCreate(relId, attrName);
}

int Schema::dropIndex(char relName[ATTR_SIZE], char attrName[ATTR_SIZE]) {
    if(strcmp(relName, "RELATIONCAT") == 0 || strcmp(relName, "ATTRIBUTECAT") == 0) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);
    if(relId == E_RELNOTOPEN) {
        return relId;
    }

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if(ret != SUCCESS) {
        return ret;
    }

    int rootBlock = attrCatEntry.rootBlock;
    if(rootBlock == -1) {
        return E_NOINDEX;
    }

    BPlusTree::bPlusDestroy(rootBlock);
    rootBlock = -1;
    attrCatEntry.rootBlock = -1;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
    
    return SUCCESS;
}

int Schema::list_all() {
    Attribute record[ATTR_SIZE];
    RecBuffer relCat(RELCAT_BLOCK);
    
    unsigned char slotMap[SLOTMAP_SIZE_RELCAT_ATTRCAT];
    relCat.getSlotMap(slotMap);

    for(int i=0; i<SLOTMAP_SIZE_RELCAT_ATTRCAT; i++) {
        if(slotMap[i] == SLOT_OCCUPIED) {
            relCat.getRecord(record, i);
            printf("%s\n", record[0].sVal);
        }
    }

    return SUCCESS;
}

int Schema::fdisk() {
    Disk::createDisk();
    Disk::formatDisk();
    Disk::addMetaData();

    StaticBuffer::init();
    OpenRelTable::init();

    return SUCCESS;
}