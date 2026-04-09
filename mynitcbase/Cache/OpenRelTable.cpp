#include "OpenRelTable.h"
#include <cstring>
#include <stdlib.h>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];
OpenRelTable::OpenRelTable()
{

  for (int i = 0; i < MAX_OPEN; i++)
  {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    tableMetaInfo[i].free = true;
    tableMetaInfo[i].relName[0] = '\0';
  }

  RecBuffer relCatBlock(RELCAT_BLOCK);
  Attribute relCatRecord[RELCAT_NO_ATTRS];

  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  struct RelCacheEntry relCatCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCatCacheEntry.relCatEntry);
  relCatCacheEntry.recId.block = RELCAT_BLOCK;
  relCatCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;
  relCatCacheEntry.dirty = false;
  relCatCacheEntry.searchIndex = {-1, -1};

  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCatCacheEntry;

  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);

  struct RelCacheEntry attrCatCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &attrCatCacheEntry.relCatEntry);

  attrCatCacheEntry.recId.block = RELCAT_BLOCK;
  attrCatCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;
  attrCatCacheEntry.dirty = false;
  attrCatCacheEntry.searchIndex = {-1, -1};

  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = attrCatCacheEntry;

  tableMetaInfo[RELCAT_RELID].free = false;
  tableMetaInfo[ATTRCAT_RELID].free = false;
  strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);
  strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);

  RecBuffer attrCatBlock(ATTRCAT_BLOCK);
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

  AttrCacheEntry *head = nullptr;
  AttrCacheEntry *prev = nullptr;

  for (int no_relcatattrs = 0; no_relcatattrs < RELCAT_NO_ATTRS; no_relcatattrs++) // ? slots 0-5 are for relation catalog
  {
    attrCatBlock.getRecord(attrCatRecord, no_relcatattrs);
    AttrCacheEntry *entry = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));

    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &entry->attrCatEntry);

    entry->recId.block = ATTRCAT_BLOCK;
    entry->recId.slot = no_relcatattrs;
    entry->next = nullptr;
    entry->dirty = false;
    entry->searchIndex = {-1, -1};
    if (head == nullptr)
      head = entry;
    else
      prev->next = entry;
    prev = entry;
  }
  AttrCacheTable::attrCache[RELCAT_RELID] = head;

  head = nullptr;
  prev = nullptr;
  for (int no_attrcatattrs = 0; no_attrcatattrs < ATTRCAT_NO_ATTRS; no_attrcatattrs++)
  {
    attrCatBlock.getRecord(attrCatRecord, no_attrcatattrs + RELCAT_NO_ATTRS);
    AttrCacheEntry *entry = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &entry->attrCatEntry);
    entry->recId.block = ATTRCAT_BLOCK;
    entry->recId.slot = no_attrcatattrs + RELCAT_NO_ATTRS;
    entry->next = nullptr;
    if (head == nullptr)
      head = entry;
    else
      prev->next = entry;
    prev = entry;
  }
  AttrCacheTable::attrCache[ATTRCAT_RELID] = head;
}

OpenRelTable::~OpenRelTable()
{
  for (int i = 2; i < MAX_OPEN; ++i)
  {
    if (!tableMetaInfo[i].free)
      OpenRelTable::closeRel(i);
  }
  if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty == true)
  {
    union Attribute record[ATTRCAT_NO_ATTRS];
    RelCatEntry entry;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &entry);
    RelCacheTable::relCatEntryToRecord(&entry, record);
    RecId recId = RelCacheTable::relCache[ATTRCAT_RELID]->recId;
    RecBuffer relCatBlock(recId.block);
    relCatBlock.setRecord(record, recId.slot);
  }
  free(RelCacheTable::relCache[ATTRCAT_RELID]);
  if (RelCacheTable::relCache[RELCAT_RELID]->dirty == true)
  {
    union Attribute record[RELCAT_NO_ATTRS];
    RelCatEntry entry;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &entry);
    RelCacheTable::relCatEntryToRecord(&entry, record);
    RecId recId = RelCacheTable::relCache[RELCAT_RELID]->recId;
    RecBuffer relCatBlock(recId.block);
    relCatBlock.setRecord(record, recId.slot);
  }
  free(RelCacheTable::relCache[RELCAT_RELID]);

  for (int i = 0; i < 2; i++)
  {
    if (AttrCacheTable::attrCache[i] != nullptr)
    {
      AttrCacheEntry *current = AttrCacheTable::attrCache[i];
      while (current != nullptr)
      {
        AttrCacheEntry *next = current->next;
        free(current);
        current = next;
      }
      AttrCacheTable::attrCache[i] = nullptr;
    }
  }
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE])
{
  for (int cacheslot = 0; cacheslot < MAX_OPEN; cacheslot++)
  {
    if (!tableMetaInfo[cacheslot].free && strcmp(tableMetaInfo[cacheslot].relName, relName) == 0)
      return cacheslot;
  }
  return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry()
{

  for (int cacheSlot = 2; cacheSlot < MAX_OPEN; cacheSlot++)
  {
    if (tableMetaInfo[cacheSlot].free)
      return cacheSlot;
  }
  return E_CACHEFULL;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE])
{
  int existing = getRelId(relName);
  if (existing != E_RELNOTOPEN)
    return existing;

  int relId = getFreeOpenRelTableEntry();
  if (relId == E_CACHEFULL)
    return E_CACHEFULL;

  RecBuffer relCatBuffer(RELCAT_BLOCK);
  HeadInfo head;
  relCatBuffer.getHeader(&head);
  Attribute relcatAttribute;
  strcpy(relcatAttribute.sVal, relName);
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, relcatAttribute, EQ);
  if (relcatRecId.block == -1 && relcatRecId.slot == -1)
    return E_RELNOTEXIST;
  Attribute record[RELCAT_NO_ATTRS];
  relCatBuffer.getRecord(record, relcatRecId.slot);
  RelCacheEntry *entry = (RelCacheEntry *)malloc(sizeof(RelCacheEntry));
  RelCacheTable::recordToRelCatEntry(record, &entry->relCatEntry);
  entry->recId.block = relcatRecId.block;
  entry->recId.slot = relcatRecId.slot;
  entry->dirty = false;
  entry->searchIndex = {-1, -1};
  RelCacheTable::relCache[relId] = entry;

  AttrCacheEntry *listHead = nullptr, *prev = nullptr;
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  while (true)
  {
    RecId attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, (char *)ATTRCAT_ATTR_RELNAME, relcatAttribute, EQ);
    if (attrcatRecId.block == -1 && attrcatRecId.slot == -1)
      break;
    RecBuffer attrbuffer(attrcatRecId.block);
    Attribute record[ATTRCAT_NO_ATTRS];
    attrbuffer.getRecord(record, attrcatRecId.slot);
    AttrCacheEntry *entry = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(record, &entry->attrCatEntry);
    entry->recId.block = attrcatRecId.block;
    entry->recId.slot = attrcatRecId.slot;
    entry->dirty = false;
    entry->searchIndex = {-1, -1};
    entry->next = nullptr;
    if (!listHead)
      listHead = entry;
    else
      prev->next = entry;
    prev = entry;
  }

  tableMetaInfo[relId].free = false;
  strcpy(tableMetaInfo[relId].relName, relName);
  AttrCacheTable::attrCache[relId] = listHead;

  return relId;
}

int OpenRelTable::closeRel(int relId)
{
  if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
    return E_NOTPERMITTED;

  if (0 > relId || relId >= MAX_OPEN)
    return E_OUTOFBOUND;

  if (tableMetaInfo[relId].free)
    return E_RELNOTOPEN;

  if (RelCacheTable::relCache[relId]->dirty == true)
  {
    /* Get the Relation Catalog entry from RelCacheTable::relCache
    Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
    Attribute relCatBuffer[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&(RelCacheTable::relCache[relId]->relCatEntry), relCatBuffer);

    // declaring an object of RecBuffer class to write back to the buffer
    RecId recId = RelCacheTable::relCache[relId]->recId;
    RecBuffer relCatBlock(recId.block);

    // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    relCatBlock.setRecord(relCatBuffer, RelCacheTable::relCache[relId]->recId.slot);
  }

  // free the memory allocated in the relation and attribute caches which was
  // allocated in the OpenRelTable::openRel() function
  free(RelCacheTable::relCache[relId]);

  // // RelCacheEntry *relCacheBuffer = RelCacheTable::relCache[relId];

  //* because we are not modifying the attribute cache at this stage,
  //* write-back is not required. We will do it in subsequent
  //* stages when it becomes needed)

  AttrCacheEntry *head = AttrCacheTable::attrCache[relId];
  AttrCacheEntry *next = head->next;

  while (true)
  {
    if (head->dirty)
    {
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      AttrCacheTable::attrCatEntryToRecord(&(head->attrCatEntry), attrCatRecord);

      RecBuffer attrCatBlockBuffer(head->recId.block);
      attrCatBlockBuffer.setRecord(attrCatRecord, head->recId.slot);
    }

    free(head);
    head = next;

    if (head == NULL)
      break;
    next = next->next;
  }

  // update `tableMetaInfo` to set `relId` as a free slot
  // update `relCache` and `attrCache` to set the entry at `relId` to nullptr
  tableMetaInfo[relId].free = true;
  RelCacheTable::relCache[relId] = nullptr;
  AttrCacheTable::attrCache[relId] = nullptr;

  return SUCCESS;
}
