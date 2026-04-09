#include "BlockAccess.h"
#include <cstring>

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op)
{
  RecId prevRecId;
  RelCacheTable::getSearchIndex(relId, &prevRecId);

  int block, slot;

  if (prevRecId.block == -1 && prevRecId.slot == -1)
  {
    RelCatEntry relCat;
    if (RelCacheTable::getRelCatEntry(relId, &relCat) != SUCCESS)
      return RecId{-1, -1};
    block = relCat.firstBlk;
    slot = 0;
  }
  else
  {
    block = prevRecId.block;
    slot = prevRecId.slot + 1;
  }
  AttrCatEntry attrCat;
  if (AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCat) != SUCCESS)
    return RecId{-1, -1};

  while (block != -1)
  {
    RecBuffer buffer(block);
    HeadInfo head;
    buffer.getHeader(&head);

    unsigned char slotMap[head.numSlots];
    buffer.getSlotMap(slotMap);

    while (slot < head.numSlots)
    {
      if (slotMap[slot] == SLOT_UNOCCUPIED)
      {
        slot++;
        continue;
      }

      Attribute RECORD[head.numAttrs];
      buffer.getRecord(RECORD, slot);

      int cmpVal = compareAttrs(RECORD[attrCat.offset], attrVal, attrCat.attrType);

      if (
          (op == NE && cmpVal != 0) || // ? not equal to
          (op == LT && cmpVal < 0) ||  // ? less than
          (op == LE && cmpVal <= 0) || // ? less than or equal to
          (op == EQ && cmpVal == 0) || // ? equal to
          (op == GT && cmpVal > 0) ||  // ? greater than
          (op == GE && cmpVal >= 0)    // ? greater than or equal to
      )
      {
        RecId point = {block, slot};
        RelCacheTable::setSearchIndex(relId, &point);
        return point;
      }

      slot++; // ? move to next slot
    }
    block = head.rblock;
    slot = 0;
  }
  return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{
  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  Attribute newRelationName;
  strcpy(newRelationName.sVal, newName);

  RecId recId1;
  recId1 = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, newRelationName, EQ);

  if (recId1.block != -1 && recId1.slot != -1)
    return E_RELEXIST;

  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  Attribute oldRelationName; // ? set oldRelationName with oldName
  strcpy(oldRelationName.sVal, oldName);

  RecId recId2;
  recId2 = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, oldRelationName, EQ);

  if (recId2.block == -1 && recId2.slot == -1)
    return E_RELNOTEXIST;

  RecBuffer relBuffer(RELCAT_BLOCK);
  Attribute relRecord[RELCAT_NO_ATTRS];
  relBuffer.getRecord(relRecord, recId2.slot);

  strcpy(relRecord[RELCAT_REL_NAME_INDEX].sVal, newName);
  relBuffer.setRecord(relRecord, recId2.slot);
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

  while (true)
  {
    RecId recId3;
    recId3 = linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);

    if (recId3.block == -1 && recId3.slot == -1)
      break;
    RecBuffer attrBuffer(recId3.block);
    Attribute attrRecord[ATTRCAT_NO_ATTRS];
    attrBuffer.getRecord(attrRecord, recId3.slot);
    strcpy(attrRecord[ATTRCAT_REL_NAME_INDEX].sVal, newName);
    attrBuffer.setRecord(attrRecord, recId3.slot);
  }
  return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{
  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  Attribute relNameAttr; // ? set relNameAttr to relName
  strcpy(relNameAttr.sVal, relName);

  RecId recId1;
  recId1 = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameAttr, EQ);

  if (recId1.block == -1 && recId1.slot == -1)
    return E_RELNOTEXIST;

  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  RecId attrToRenameRecId{-1, -1};
  Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

  while (true)
  {
    RecId recId2;
    recId2 = linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
    if (recId2.block == -1 && recId2.slot == -1)
      break;
    RecBuffer attrBuffer(recId2.block);
    attrBuffer.getRecord(attrCatEntryRecord, recId2.slot);
    if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0)
      return E_ATTREXIST;
    if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0)
      attrToRenameRecId = recId2;
  }

  if (attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1)
    return E_ATTRNOTEXIST;

  RecBuffer renameBuffer(attrToRenameRecId.block);
  renameBuffer.getRecord(attrCatEntryRecord, attrToRenameRecId.slot);
  strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
  renameBuffer.setRecord(attrCatEntryRecord, attrToRenameRecId.slot);

  return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record)
{
  RelCatEntry relEntry;
  int ret = RelCacheTable::getRelCatEntry(relId, &relEntry);
  if (ret != SUCCESS)
    return ret;

  RecId recId = {-1, -1};
  int prevBlockNum = -1;

  int blockNum = relEntry.firstBlk;
  int numOfSlots = relEntry.numSlotsPerBlk;
  int numOfAttributes = relEntry.numAttrs;

  while (blockNum != -1)
  {
    RecBuffer buffer(blockNum);
    struct HeadInfo head1;
    ret = buffer.getHeader(&head1);
    if (ret != SUCCESS)
      return ret;
    unsigned char slotMap[numOfSlots];
    ret = buffer.getSlotMap(slotMap);
    if (ret != SUCCESS)
      return ret;
    for (int i = 0; i < numOfSlots; i++)
    {
      if (slotMap[i] == SLOT_UNOCCUPIED)
      {
        recId = {blockNum, i};
        break;
      }
    }
    if (recId.block != -1)
      break;
    prevBlockNum = blockNum;
    blockNum = head1.rblock;
  }
  if (recId.block == -1)
  {
    if (relId == RELCAT_RELID)
      return E_MAXRELATIONS;
    RecBuffer newBlock;
    int newBlockNum = newBlock.getBlockNum();
    if (newBlockNum == E_DISKFULL)
      return E_DISKFULL;
    recId = {newBlockNum, 0};
    struct HeadInfo head2;
    head2.blockType = REC;
    head2.pblock = -1;
    head2.rblock = -1;
    head2.lblock = prevBlockNum;
    head2.numAttrs = numOfAttributes;
    head2.numSlots = numOfSlots;
    head2.numEntries = 0;
    newBlock.setHeader(&head2);

    unsigned char newSlotMap[numOfSlots];
    for (int i = 0; i < numOfSlots; i++)
      newSlotMap[i] = SLOT_UNOCCUPIED;
    newBlock.setSlotMap(newSlotMap);

    if (prevBlockNum != -1)
    {
      RecBuffer prev(prevBlockNum);
      struct HeadInfo head3;
      ret = prev.getHeader(&head3);
      if (ret != SUCCESS)
        return ret;
      head3.rblock = recId.block;
      prev.setHeader(&head3);
    }
    else
      relEntry.firstBlk = newBlockNum;
    relEntry.lastBlk = newBlockNum;
    ret = RelCacheTable::setRelCatEntry(relId, &relEntry);
    if (ret != SUCCESS)
      return ret;
  }
  RecBuffer target(recId.block);
  ret = target.setRecord(record, recId.slot);
  if (ret != SUCCESS)
    return ret;
  unsigned char targetSlotMap[numOfSlots];
  ret = target.getSlotMap(targetSlotMap);
  if (ret != SUCCESS)
    return ret;
  targetSlotMap[recId.slot] = SLOT_OCCUPIED;
  ret = target.setSlotMap(targetSlotMap);
  if (ret != SUCCESS)
    return ret;

  struct HeadInfo head4;
  ret = target.getHeader(&head4);
  if (ret != SUCCESS)
    return ret;
  head4.numEntries++;
  ret = target.setHeader(&head4);
  if (ret != SUCCESS)
    return ret;

  relEntry.numRecs++;
  ret = RelCacheTable::setRelCatEntry(relId, &relEntry);
  if (ret != SUCCESS)
    return ret;

  int flag = SUCCESS;
  for (int i = 0; i < relEntry.numAttrs; i++)
  {
    AttrCatEntry attrcatentry;
    ret = AttrCacheTable::getAttrCatEntry(relId, i, &attrcatentry);
    if (ret != SUCCESS)
      return ret;
    int rootBlock = attrcatentry.rootBlock;
    if (rootBlock != -1)
    {
      ret = BPlusTree::bPlusInsert(relId, attrcatentry.attrName, record[i], recId);
      if (ret == E_DISKFULL)
        flag = E_INDEX_BLOCKS_RELEASED;
    }
  }
  return flag;
}

int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
  RecId recId;
  AttrCatEntry attrcatentry;
  int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcatentry);
  if (ret != SUCCESS)
    return ret;
  int rootBlock = attrcatentry.rootBlock;
  if (rootBlock == -1)
    recId = linearSearch(relId, attrName, attrVal, op);
  else
    recId = BPlusTree::bPlusSearch(relId, attrName, attrVal, op);
  if (recId.block == -1 && recId.slot == -1)
    return E_NOTFOUND;
  RecBuffer buffer(recId.block);
  ret = buffer.getRecord(record, recId.slot);
  return ret;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE])
{
  if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;
  int ret = RelCacheTable::resetSearchIndex(RELCAT_RELID);
  Attribute relNameAttr;
  strcpy(relNameAttr.sVal, relName);
  RecId recId;
  recId = linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, relNameAttr, EQ);
  if (recId.block == -1 && recId.slot == -1)
    return E_RELNOTEXIST;

  Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
  RecBuffer relCatBuffer(recId.block);

  struct HeadInfo relCatHeader;
  relCatBuffer.getHeader(&relCatHeader);
  relCatBuffer.getRecord(relCatEntryRecord, recId.slot);
  int firstBlk = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
  int numAttrs = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
  int next = firstBlk;

  while (next != -1)
  {
    RecBuffer bufferObject(next); // BLockbuff
    struct HeadInfo blockHeader;
    bufferObject.getHeader(&blockHeader);
    next = blockHeader.rblock;
    bufferObject.releaseBlock();
  }

  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  int numberOfAttributesDeleted = 0;
  while (true)
  {
    RecId attrCatRecId;
    attrCatRecId = linearSearch(ATTRCAT_RELID, (char *)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
    if (attrCatRecId.block == -1 && attrCatRecId.slot == -1)
      break;
    numberOfAttributesDeleted++;
    RecBuffer attrCatBuf(attrCatRecId.block);
    struct HeadInfo attrCatBufHeader;
    attrCatBuf.getHeader(&attrCatBufHeader);
    union Attribute record[attrCatBufHeader.numAttrs];
    attrCatBuf.getRecord(record, attrCatRecId.slot);

    int rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;

    unsigned char slotMap[attrCatBufHeader.numSlots];
    attrCatBuf.getSlotMap(slotMap);
    slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
    attrCatBuf.setSlotMap(slotMap);

    (attrCatBufHeader.numEntries)--;
    attrCatBuf.setHeader(&attrCatBufHeader);

    if (attrCatBufHeader.numEntries == 0)
    {
      if (attrCatBufHeader.lblock != -1)
      {
        RecBuffer leftBlockBuffer(attrCatBufHeader.lblock);
        struct HeadInfo leftBlockHeader;
        leftBlockBuffer.getHeader(&leftBlockHeader);
        leftBlockHeader.rblock = attrCatBufHeader.rblock;
        leftBlockBuffer.setHeader(&leftBlockHeader);
      }
      else
      {
        RelCatEntry attrcatentry;
        RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrcatentry);
        attrcatentry.firstBlk = attrCatBufHeader.rblock;
        RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrcatentry);
      }

      if (attrCatBufHeader.rblock != -1)
      {
        RecBuffer rightBlockBuffer(attrCatBufHeader.rblock);
        struct HeadInfo rightBlockHeader;
        rightBlockBuffer.getHeader(&rightBlockHeader);
        rightBlockHeader.lblock = attrCatBufHeader.lblock;
        rightBlockBuffer.setHeader(&rightBlockHeader);
      }
      else
      {
        RelCatEntry attrcatentry;
        RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrcatentry);
        attrcatentry.lastBlk = attrCatBufHeader.lblock;
        RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrcatentry);
      }
      attrCatBuf.releaseBlock();
    }
    if (rootBlock != -1)
    {
      int ret = BPlusTree::bPlusDestroy(rootBlock);
      if (ret != SUCCESS)
        return SUCCESS;
    }
  }
  relCatBuffer.getHeader(&relCatHeader);
  relCatHeader.numEntries = relCatHeader.numEntries - 1;
  relCatBuffer.setHeader(&relCatHeader);

  unsigned char relCatSlotMap[relCatHeader.numSlots];

  relCatBuffer.getSlotMap(relCatSlotMap);
  relCatSlotMap[recId.slot] = SLOT_UNOCCUPIED;
  relCatBuffer.setSlotMap(relCatSlotMap);

  RelCatEntry relCatEntryBuffer;
  RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntryBuffer);
  (relCatEntryBuffer.numRecs)--;
  RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntryBuffer);

  RelCatEntry AttrCatEntryBuffer;
  RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &AttrCatEntryBuffer);
  AttrCatEntryBuffer.numRecs -= numberOfAttributesDeleted;
  RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &AttrCatEntryBuffer);

  return SUCCESS;
}

int BlockAccess::project(int relId, Attribute *record)
{
  int ret;
  RecId prevRecId;
  RelCacheTable::getSearchIndex(relId, &prevRecId);
  int block, slot;
  if (prevRecId.block == -1 && prevRecId.slot == -1)
  {
    RelCatEntry relCatBuf;
    ret = RelCacheTable::getRelCatEntry(relId, &relCatBuf);
    if (ret != SUCCESS)
      return ret;
    block = relCatBuf.firstBlk;
    slot = 0;
  }
  else
  {
    block = prevRecId.block;
    slot = prevRecId.slot + 1;
  }
  while (block != -1)
  {
    RecBuffer buffer(block);
    struct HeadInfo head;
    ret = buffer.getHeader(&head);
    if (ret != SUCCESS)
      return ret;
    unsigned char slotMap[head.numSlots];
    ret = buffer.getSlotMap(slotMap);
    if (slot >= head.numSlots)
    {
      block = head.rblock;
      slot = 0;
      continue;
    }
    else if (slotMap[slot] == SLOT_UNOCCUPIED)
      slot++;
    else
      break;
  }
  if (block == -1)
    return E_NOTFOUND;
  RecId nextRecId{block, slot};
  RelCacheTable::setSearchIndex(relId, &nextRecId);
  RecBuffer recordBuffer(nextRecId.block);
  ret = recordBuffer.getRecord(record, nextRecId.slot);
  return SUCCESS;
}
