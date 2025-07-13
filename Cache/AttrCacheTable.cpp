#include "AttrCacheTable.h"

#include <cstring>


AttrCacheEntry *AttrCacheTable::attrCache[MAX_OPEN];


int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf) {
    if(relId < 0 || relId >= MAX_OPEN) {
            return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
            return E_RELNOTOPEN;
    }

    for (AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
        if (entry->attrCatEntry.offset == attrOffset) {
            strcpy(attrCatBuf->relName, entry->attrCatEntry.relName);
            strcpy(attrCatBuf->attrName, entry->attrCatEntry.attrName);
            attrCatBuf->offset = entry->attrCatEntry.offset;
            attrCatBuf->attrType  = entry->attrCatEntry.attrType;
			attrCatBuf->primaryFlag  = entry->attrCatEntry.primaryFlag;
			attrCatBuf->rootBlock  = entry->attrCatEntry.rootBlock;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf) {
    if(relId < 0 || relId >= MAX_OPEN) {
            return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
            return E_RELNOTOPEN;
    }

    for (AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
        if (strcmp(entry->attrCatEntry.attrName, attrName) == 0) {
            strcpy(attrCatBuf->relName, entry->attrCatEntry.relName);
            strcpy(attrCatBuf->attrName, entry->attrCatEntry.attrName);
            attrCatBuf->offset = entry->attrCatEntry.offset;
            attrCatBuf->attrType  = entry->attrCatEntry.attrType;
			attrCatBuf->primaryFlag  = entry->attrCatEntry.primaryFlag;
			attrCatBuf->rootBlock  = entry->attrCatEntry.rootBlock;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS], AttrCatEntry* attrCatEntry) {
    strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
    strcpy(attrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal);
    attrCatEntry->attrType = (int)record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
    attrCatEntry->primaryFlag = (bool)record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;
    attrCatEntry->rootBlock = (int)record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
    attrCatEntry->offset = (int)record[ATTRCAT_OFFSET_INDEX].nVal;
}

void AttrCacheTable::attrCatEntryToRecord(AttrCatEntry *attrCatEntry, union Attribute record[ATTRCAT_NO_ATTRS]) {
    strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, attrCatEntry->relName);
    strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal, attrCatEntry->attrName);
    record[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrCatEntry->attrType;
    record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = attrCatEntry->primaryFlag;
    record[ATTRCAT_ROOT_BLOCK_INDEX].nVal = attrCatEntry->rootBlock;
    record[ATTRCAT_OFFSET_INDEX].nVal = attrCatEntry->offset;
}

int AttrCacheTable::setSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex) {
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }
    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    AttrCacheEntry *x = attrCache[relId];
    while(x != nullptr) {
        if(strcmp(x->attrCatEntry.attrName, attrName)==0) {
            x->searchIndex = *searchIndex;
            return SUCCESS;
        }
        x = x->next;
    }
    
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setSearchIndex(int relId, int attrOffset, IndexId *searchIndex) {
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }
    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    AttrCacheEntry *x = attrCache[relId];
    while(x != nullptr) {
        if(x->attrCatEntry.offset == attrOffset) {
            x->searchIndex = *searchIndex;
            return SUCCESS;
        }
        x = x->next;
    }
    
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::resetSearchIndex(int relId, char attrName[ATTR_SIZE]) {
    IndexId indexId = {-1, -1};
    int ret = AttrCacheTable::setSearchIndex(relId, attrName, &indexId);
    return ret;
}

int AttrCacheTable::resetSearchIndex(int relId, int attrOffset) {
    IndexId indexId = {-1, -1};
    int ret = AttrCacheTable::setSearchIndex(relId, attrOffset, &indexId);
    return ret;
}

int AttrCacheTable::getSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex) {
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    for(AttrCacheEntry *x = attrCache[relId]; x != nullptr; x=x->next) {
        if(strcmp(x->attrCatEntry.attrName, attrName) == 0) {
            *searchIndex = x->searchIndex;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getSearchIndex(int relId, int attrOffset, IndexId *searchIndex) {
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    for(AttrCacheEntry *x = attrCache[relId]; x != nullptr; x=x->next) {
        if(x->attrCatEntry.offset == attrOffset) {
            *searchIndex = x->searchIndex;
            return SUCCESS;
        }
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf) {
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }
    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    AttrCacheEntry *x = attrCache[relId];
    while(x != nullptr) {
        if(strcmp(attrName, x->attrCatEntry.attrName) == 0) {
            x->attrCatEntry = *attrCatBuf;
            x->dirty = true;
            return SUCCESS;
        }
        x = x->next;
    }

    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setAttrCatEntry(int relId, int attrOffset, AttrCatEntry *attrCatBuf) {
    if(relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }
    if(attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    AttrCacheEntry *x = attrCache[relId];
    while(x != nullptr) {
        if(attrOffset == x->attrCatEntry.offset) {
            x->attrCatEntry = *attrCatBuf;
            x->dirty = true;
            return SUCCESS;
        }
        x = x->next;
    }

    return E_ATTRNOTEXIST;
}