#include "Algebra.h"
#include <iostream>
#include <cstring>

bool isNumber(char *str) {
    int len;
    float ignore;

    int ret = sscanf(str, "%f %n", &ignore, &len);
    return ret == 1 && len == strlen(str);
}


int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if(srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
    if(ret == E_ATTRNOTEXIST) {
        return ret;
    }

    int type = attrCatEntry.attrType;
    Attribute attrVal;
    if (type == NUMBER) {
        if (isNumber(strVal)) {
            attrVal.nVal = atof(strVal);
        }
        else {
            return E_ATTRTYPEMISMATCH;
        }
    }
    else if (type == STRING) {
        strcpy(attrVal.sVal, strVal);
    }


    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatBuf);
    int src_nAttrs = relCatBuf.numAttrs;
    char attr_names[src_nAttrs][ATTR_SIZE];
    int attr_types[src_nAttrs];

    for(int i=0; i<src_nAttrs; i++) {
        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatBuf);
        strcpy(attr_names[i], attrCatBuf.attrName);
        attr_types[i] = attrCatBuf.attrType;
    }

    ret = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
    if(ret != SUCCESS) {
        return ret;
    }

    int tarRelId = OpenRelTable::openRel(targetRel);
    if(tarRelId < 0 || tarRelId >= MAX_OPEN) {
        Schema::deleteRel(targetRel);
        return tarRelId;
    }


    Attribute record[src_nAttrs];

    RelCacheTable::resetSearchIndex(srcRelId);
    AttrCacheTable::resetSearchIndex(srcRelId, attr);

    while(BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS) {
        ret = BlockAccess::insert(tarRelId, record);

        if(ret != SUCCESS) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    Schema::closeRel(targetRel);

    return SUCCESS;
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]) {
    if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);
    if(relId == E_RELNOTOPEN) {
        return relId;
    }

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    
    if(relCatEntry.numAttrs != nAttrs){
        return E_NATTRMISMATCH;
    }

    Attribute recordValues[nAttrs];
    AttrCatEntry attrCatEntry;
    for(int i=0; i<nAttrs; i++) {
        AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);

        int type = attrCatEntry.attrType;
        if(type == NUMBER) {
            if (isNumber(record[i])) {
                recordValues[i].nVal = atof(record[i]);
            }
            else {
                return E_ATTRTYPEMISMATCH;
            }
        }
        else if(type == STRING) {
            strcpy(recordValues[i].sVal, record[i]);
        }
    }

    int retVal = BlockAccess::insert(relId, recordValues);

    return retVal;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if(srcRelId < 0 || srcRelId >= MAX_OPEN) {
        return E_RELNOTOPEN;
    }

    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatBuf);
    int numOfAttrs = relCatBuf.numAttrs;
    char attrNames[numOfAttrs][ATTR_SIZE];
    int attrTypes[numOfAttrs];

    for(int i=0; i<numOfAttrs; i++) {
        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatBuf);
        strcpy(attrNames[i], attrCatBuf.attrName);
        attrTypes[i] = attrCatBuf.attrType;
    }

    int ret = Schema::createRel(targetRel, numOfAttrs, attrNames, attrTypes);
    if(ret != SUCCESS) {
        return ret;
    }
    int tarRelId = OpenRelTable::openRel(targetRel);
    if(tarRelId < 0 || tarRelId >= MAX_OPEN) {
        Schema::deleteRel(targetRel);
        return tarRelId;
    }

    RelCacheTable::resetSearchIndex(srcRelId);
    Attribute record[numOfAttrs];

    while(BlockAccess::project(srcRelId, record) == SUCCESS) {
        ret = BlockAccess::insert(tarRelId, record);
        if(ret != SUCCESS) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    Schema::closeRel(targetRel);

    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) {
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if(srcRelId < 0 || srcRelId >= MAX_OPEN) {
        return E_RELNOTOPEN;
    }

    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatBuf);
    int numOfAttrs = relCatBuf.numAttrs;

    int attr_offset[tar_nAttrs];
    int attr_types[tar_nAttrs];

    for(int i=0; i<tar_nAttrs; i++) {
        AttrCatEntry attrCatBuf;
        int ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &attrCatBuf);
        if(ret != SUCCESS) {
            return E_ATTRNOTEXIST;
        }
        attr_offset[i] = attrCatBuf.offset;
        attr_types[i] = attrCatBuf.attrType;
    }

    int ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);
    if(ret != SUCCESS) {
        return ret;
    }
    int tarRelId = OpenRelTable::openRel(targetRel);
    if(tarRelId < 0 || tarRelId >= MAX_OPEN) {
        Schema::deleteRel(targetRel);
        return tarRelId;
    }

    RelCacheTable::resetSearchIndex(srcRelId);

    Attribute record[numOfAttrs];

    while(BlockAccess::project(srcRelId, record) == SUCCESS) {
        Attribute proj_record[tar_nAttrs];

        for(int i=0; i<tar_nAttrs; i++) {
            proj_record[i] = record[attr_offset[i]];
        }
        ret = BlockAccess::insert(tarRelId, proj_record);

        if(ret != SUCCESS) {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    Schema::closeRel(targetRel);
    
    return SUCCESS;
}

int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE]) {
    int relId1 = OpenRelTable::getRelId(srcRelation1);
    int relId2 = OpenRelTable::getRelId(srcRelation2);
    if(relId1 < 0 || relId1 >= MAX_OPEN || relId2 < 0 || relId2 >= MAX_OPEN) {
        return E_RELNOTOPEN;
    }

    AttrCatEntry attrCatEntry1, attrCatEntry2;
    int ret1 = AttrCacheTable::getAttrCatEntry(relId1, attribute1, &attrCatEntry1);
    int ret2 = AttrCacheTable::getAttrCatEntry(relId2, attribute2, &attrCatEntry2);
    if(ret1 == E_ATTRNOTEXIST || ret2 == E_ATTRNOTEXIST) {
        return E_ATTRNOTEXIST;
    }
    if(attrCatEntry1.attrType != attrCatEntry2.attrType) {
        return E_ATTRTYPEMISMATCH;
    }

    RelCatEntry relCatEntry1, relCatEntry2;
    RelCacheTable::getRelCatEntry(relId1, &relCatEntry1);
    RelCacheTable::getRelCatEntry(relId2, &relCatEntry2);
    int numOfAttributes1 = relCatEntry1.numAttrs;
    int numOfAttributes2 = relCatEntry2.numAttrs;
    AttrCatEntry attrCatEntry11, attrCatEntry22;
    int count = 0;
    for(int i=0; i<numOfAttributes1; i++) {
        AttrCacheTable::getAttrCatEntry(relId1, i, &attrCatEntry11);
        for(int j=0; j<numOfAttributes2; j++) {
            AttrCacheTable::getAttrCatEntry(relId2, j, &attrCatEntry22);
            if(strcmp(attrCatEntry11.attrName, attrCatEntry22.attrName) == 0) {
                count++;
            }
        }
    }
    if(count > 1) {
        return E_DUPLICATEATTR;
    }

    if(attrCatEntry2.rootBlock == -1) {
        int ret = BPlusTree::bPlusCreate(relId2, attribute2);
        if(ret != SUCCESS) {
            return ret;
        }
    }

    int numOfAttributesInTarget = numOfAttributes1 + numOfAttributes2 - 1;
    char targetRelAttrNames[numOfAttributesInTarget][ATTR_SIZE];
    int targetRelAttrTypes[numOfAttributesInTarget];

    int k = 0;
    int i = 0;
    while(i < numOfAttributesInTarget + 1) {
        if(i < numOfAttributes1) {
            AttrCacheTable::getAttrCatEntry(relId1, i, &attrCatEntry11);
            strcpy(targetRelAttrNames[k], attrCatEntry11.attrName);
            targetRelAttrTypes[k] = attrCatEntry11.attrType;
        }
        else {
            int z = i-numOfAttributes1;
            AttrCacheTable::getAttrCatEntry(relId2, z, &attrCatEntry22);
            if(strcmp(attrCatEntry22.attrName, attribute2) == 0) {
                i++;
                continue;
            }
            strcpy(targetRelAttrNames[k], attrCatEntry22.attrName);
            targetRelAttrTypes[k] = attrCatEntry22.attrType;
        }
        i++;
        k++;
    }

    int ret = Schema::createRel(targetRelation, numOfAttributesInTarget, targetRelAttrNames, targetRelAttrTypes);
    if(ret != SUCCESS) {
        return ret;
    }
    int targetRelId = OpenRelTable::openRel(targetRelation);
    if(targetRelId < 0 || targetRelId >= MAX_OPEN) {
        return targetRelId;
    }

    Attribute record1[numOfAttributes1];
    Attribute record2[numOfAttributes2];
    Attribute record3[numOfAttributesInTarget];

    RelCacheTable::resetSearchIndex(relId1);
    
    while(BlockAccess::project(relId1, record1) == SUCCESS) {
        RelCacheTable::resetSearchIndex(relId2);
        AttrCacheTable::resetSearchIndex(relId2, attribute2);

        while(BlockAccess::search(relId2, record2, attribute2, record1[attrCatEntry1.offset], EQ) == SUCCESS) {
            int i = 0;
            int k = 0;
            while(i < numOfAttributesInTarget + 1) {
                if(i < numOfAttributes1) {
                    record3[k] = record1[k];
                }
                else {
                    if(i == numOfAttributes1 + attrCatEntry2.offset) {
                        i++;
                        continue;
                    }
                    record3[k] = record2[i-numOfAttributes1];
                }
                i++;
                k++;
            }

            ret = BlockAccess::insert(targetRelId, record3);
                if(ret != SUCCESS) {
                OpenRelTable::closeRel(targetRelId);
                Schema::deleteRel(targetRelation);
                return E_DISKFULL;
            }
        }
    }

    return SUCCESS;
}

int Algebra::print_table(char relName[ATTR_SIZE]) {
    int relId = OpenRelTable::getRelId(relName);
    if(relId < 0 || relId >=MAX_OPEN) {
        return E_RELNOTOPEN;
    }

    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(relId, &relCatBuf);
    int numOfAttrs = relCatBuf.numAttrs;
    Attribute record[numOfAttrs];

    AttrCatEntry attrCatBuf;
    int attrType[numOfAttrs];

    for(int i=0; i<numOfAttrs; i++) {
        AttrCacheTable::getAttrCatEntry(relId, i, &attrCatBuf);
        attrType[i] = attrCatBuf.attrType;
        printf("%s | ", attrCatBuf.attrName);
    }
    printf("\n");

    RelCacheTable::resetSearchIndex(relId);
    
    while(BlockAccess::project(relId, record) == SUCCESS) {
        for(int i=0; i<numOfAttrs; i++) {
            if(attrType[i] == NUMBER) {
                printf("%d | ", (int)record[i].nVal);
            }
            else {
                printf("%s | ", record[i].sVal);
            }
        }
        printf("\n");
    }

    return SUCCESS;
}