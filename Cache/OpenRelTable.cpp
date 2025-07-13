#include "OpenRelTable.h"
#include <stdlib.h>
#include <cstring>
#include <stdio.h>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

AttrCacheEntry *createlist(int attrSize){
  
  AttrCacheEntry *head = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
  AttrCacheEntry *x = head;

  while (attrSize!=1) {
    x->next = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    x=x->next;
    attrSize--;
  }
  x->next=nullptr;

  return head;
}


OpenRelTable::OpenRelTable() {
  OpenRelTable::init();
}

OpenRelTable::~OpenRelTable() {

  for(int i=2; i<MAX_OPEN; i++) {
    if(!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i);
    }
  }

  if(RelCacheTable::relCache[ATTRCAT_RELID]->dirty == true) {
    RelCatEntry attrCatBuf;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatBuf);
    Attribute record[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&attrCatBuf, record);

    RecBuffer relCatBlock(RelCacheTable::relCache[ATTRCAT_RELID]->recId.block);
    relCatBlock.setRecord(record, RelCacheTable::relCache[ATTRCAT_RELID]->recId.slot);
  }
  free(RelCacheTable::relCache[ATTRCAT_RELID]);

  if(RelCacheTable::relCache[RELCAT_RELID]->dirty == true) {
    RelCatEntry relcatBuf;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relcatBuf);
    Attribute record[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&relcatBuf, record);

    RecBuffer relCatBlock(RelCacheTable::relCache[RELCAT_RELID]->recId.block);
    relCatBlock.setRecord(record, RelCacheTable::relCache[RELCAT_RELID]->recId.slot);
  }
  free(RelCacheTable::relCache[RELCAT_RELID]);

  for (int i=0; i<2; i++) {
    AttrCacheEntry *head = AttrCacheTable::attrCache[i];
    AttrCacheEntry *x;

    int count = 0;
    while(head != nullptr) {
      x = head;
      head= head->next;
      count++;
      //  if(x->dirty == true) {
      //   AttrCatEntry attrCatBuf;
      //   AttrCacheTable::getAttrCatEntry(i, count, &attrCatBuf);
      //   Attribute record[ATTRCAT_NO_ATTRS];
      //   AttrCacheTable::attrCatEntryToRecord(&attrCatBuf, record);

      //   RecBuffer attrCatBlock(AttrCacheTable::attrCache[i]->recId.block);
      //   attrCatBlock.setRecord(record, AttrCacheTable::attrCache[i]->recId.slot);
      //  }
       free(x);
    }
  }
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
  for(int i=0; i<MAX_OPEN; i++) {
    if(strcmp(tableMetaInfo[i].relName, relName) == 0 && tableMetaInfo[i].free == false) {
      return i;
    }
  }
  return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() {
  for(int i=2; i<MAX_OPEN; i++) {
    if(tableMetaInfo[i].free == true) {
      return i;
    }
  }
  return E_CACHEFULL;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]) {

  int relId = OpenRelTable::getRelId(relName);
  if(relId != E_RELNOTOPEN) {
    return relId;
  }

  relId = OpenRelTable::getFreeOpenRelTableEntry();
  if(relId == E_CACHEFULL) {
    return E_CACHEFULL;
  }

  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  Attribute attrVal;
  strcpy(attrVal.sVal, relName);
  char relcatattrrelname[16];
  strcpy(relcatattrrelname, RELCAT_ATTR_RELNAME);
  RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, relcatattrrelname, attrVal, EQ);

  if(relcatRecId.block == -1 && relcatRecId.slot == -1) {
    return E_RELNOTEXIST;
  }

  RecBuffer relBuf(relcatRecId.block);
  Attribute record[RELCAT_NO_ATTRS];
  relBuf.getRecord(record, relcatRecId.slot);

  RelCacheEntry *relCacheEntry = (RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  RelCacheTable::recordToRelCatEntry(record, &(relCacheEntry->relCatEntry));
  relCacheEntry->recId.block = relcatRecId.block;
  relCacheEntry->recId.slot = relcatRecId.slot;
  RelCacheTable::relCache[relId] = relCacheEntry;

  AttrCacheEntry *listHead;
  listHead = createlist(relCacheEntry->relCatEntry.numAttrs);
  AttrCacheEntry *attrCacheEntry = listHead;
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  Attribute recordAttr[ATTRCAT_NO_ATTRS];
  

  for(int i=0; i<relCacheEntry->relCatEntry.numAttrs; i++) {
    RecId attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, relcatattrrelname, attrVal, EQ);

    RecBuffer attrBuf(attrcatRecId.block);
    attrBuf.getRecord(recordAttr, attrcatRecId.slot);
    AttrCacheTable::recordToAttrCatEntry(recordAttr, &(attrCacheEntry->attrCatEntry));

    attrCacheEntry->recId.block = attrcatRecId.block;
		attrCacheEntry->recId.slot = attrcatRecId.slot;

		attrCacheEntry = attrCacheEntry->next;
  }

  AttrCacheTable::attrCache[relId] = listHead;

  tableMetaInfo[relId].free = false;
  strcpy(tableMetaInfo[relId].relName, relName);


  return relId;

}

int OpenRelTable::closeRel(int relId) {
  if(relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
    return E_NOTPERMITTED;
  }

  if(relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if(tableMetaInfo[relId].free) {
    return E_RELNOTOPEN;
  }

  Attribute record[RELCAT_NO_ATTRS];
  if(RelCacheTable::relCache[relId]->dirty == 1) {
    RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[relId]->relCatEntry), record);
    RecBuffer relCatBlock(RelCacheTable::relCache[relId]->recId.block);
    relCatBlock.setRecord(record, RelCacheTable::relCache[relId]->recId.slot);
  }
  free(RelCacheTable::relCache[relId]);
  RelCacheTable::relCache[relId] = nullptr;


  AttrCacheEntry *head = AttrCacheTable::attrCache[relId];
  AttrCacheEntry *x = head;
  Attribute record2[ATTRCAT_NO_ATTRS];
  while(head!=nullptr) {
    x = head;
    if(x->dirty == true) {
      AttrCacheTable::attrCatEntryToRecord(&(x->attrCatEntry), record2);
      RecBuffer attrCatBlock(x->recId.block);
      attrCatBlock.setRecord(record2, x->recId.slot);
    }

    head = head->next;
    free(x);
    x = nullptr;
  }
  AttrCacheTable::attrCache[relId] = nullptr;

  tableMetaInfo[relId].free = true;

  return SUCCESS;
}


void OpenRelTable::init() {

  for (int i=0; i<MAX_OPEN; i++) {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
        tableMetaInfo[i].free = true;
  }

  RecBuffer relCatBlock(RELCAT_BLOCK);
  Attribute relCatRecord[RELCAT_NO_ATTRS];

  for (int i=0; i<2; i++) {
    //relID=i

    relCatBlock.getRecord(relCatRecord, i);

    struct RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = RELCAT_BLOCK;
    relCacheEntry.recId.slot = i;
    relCacheEntry.searchIndex.block = -1;
    relCacheEntry.searchIndex.slot = -1;

    RelCacheTable::relCache[i] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[i]) = relCacheEntry;
  }

  RecBuffer attrCatBlock(ATTRCAT_BLOCK);
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  int slotNo = 0;

  for (int i=0; i<2; i++) {
    //relId=i

    int numOfAttr = RelCacheTable::relCache[i]->relCatEntry.numAttrs;
    AttrCacheEntry *head = createlist(numOfAttr);
    AttrCacheEntry *x = head;
    
    while (numOfAttr!=0) {
      attrCatBlock.getRecord(attrCatRecord, slotNo);
      AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(x->attrCatEntry));
      x->recId.block = ATTRCAT_BLOCK;
      x->recId.slot = slotNo;

      x=x->next;
      numOfAttr--;
      slotNo++;
    }

    //AttrCacheTable::attrCache[i] = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::attrCache[i] = head;
  }


  tableMetaInfo[RELCAT_RELID].free = false;
  tableMetaInfo[ATTRCAT_RELID].free = false;
  strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);
  strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);
}