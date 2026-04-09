#include "BPlusTree.h"
#include <cstring>
#include <iostream>

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
  IndexId searchIndex;
  AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);
  AttrCatEntry attrcatentry;
  int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcatentry);
  int block, index;
  if (searchIndex.block == -1 && searchIndex.index == -1)
  {
    block = attrcatentry.rootBlock;
    index = 0;
    if (block == -1)
      return RecId{-1, -1};
  }
  else
  {
    block = searchIndex.block;
    index = searchIndex.index + 1;
    IndLeaf leaf(block);
    struct HeadInfo leafHead;
    leaf.getHeader(&leafHead);
    if (index >= leafHead.numEntries)
    {
      block = leafHead.rblock;
      index = 0;
      if (block == -1)
        return RecId{-1, -1};
    }
  }

  // traverse internal nodes
  while (StaticBuffer::getStaticBlockType(block) == IND_INTERNAL)
  {
    IndInternal internalBlk(block);
    struct HeadInfo intHead;
    internalBlk.getHeader(&intHead);
    InternalEntry intEntry;
    if (op == NE || op == LT || op == LE)
    {
      internalBlk.getEntry(&intEntry, 0);
      block = intEntry.lChild;
    }
    else
    {
      bool found = false;
      int i = 0;
      while (i < intHead.numEntries)
      {
        internalBlk.getEntry(&intEntry, i);
        int cmp = compareAttrs(intEntry.attrVal, attrVal, attrcatentry.attrType);
        if (((op == EQ || op == GE) && cmp >= 0) || (op == GT && cmp > 0))
        {
          found = true;
          break;
        }
        i++;
      }
      if (found)
        block = intEntry.lChild;
      else
      {
        internalBlk.getEntry(&intEntry, intHead.numEntries - 1);
        block = intEntry.rChild;
      }
    }
    // FIX: do NOT reset index to 0 here — index is only meaningful at the leaf level
    // and was already set correctly before entering this loop
  }

  while (block != -1)
  {
    IndLeaf leafblk(block);
    HeadInfo leafHead;
    leafblk.getHeader(&leafHead);
    Index leafEntry;
    while (index < leafHead.numEntries)
    {
      leafblk.getEntry(&leafEntry, index);
      int cmp = compareAttrs(leafEntry.attrVal, attrVal, attrcatentry.attrType);
      if ((op == EQ && cmp == 0) ||
          (op == LE && cmp <= 0) ||
          (op == LT && cmp < 0) ||
          (op == NE && cmp != 0) ||
          (op == GE && cmp >= 0) ||
          (op == GT && cmp > 0))
      {
        IndexId searchIndex{block, index};
        AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);
        return RecId{leafEntry.block, leafEntry.slot};
      }
      else if ((op == EQ || op == LE || op == LT) && cmp > 0)
        return RecId{-1, -1};
      index++;
    }
    if (op != NE)
      break;
    block = leafHead.rblock;
    index = 0;
  }
  return RecId{-1, -1};
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE])
{
  if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
    return E_NOTPERMITTED;
  AttrCatEntry attrcatentry;
  int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcatentry);
  if (ret != SUCCESS)
    return ret;
  if (attrcatentry.rootBlock != -1)
    return SUCCESS;

  IndLeaf rootBlockBuf;
  int rootBlock = rootBlockBuf.getBlockNum();
  if (rootBlock == E_DISKFULL)
    return E_DISKFULL;
  attrcatentry.rootBlock = rootBlock;
  AttrCacheTable::setAttrCatEntry(relId, attrName, &attrcatentry);

  RelCatEntry relcatentry;
  ret = RelCacheTable::getRelCatEntry(relId, &relcatentry);
  if (ret != SUCCESS)
    return ret;
  int block = relcatentry.firstBlk;
  while (block != -1)
  {
    RecBuffer buffer(block);
    unsigned char slotMap[relcatentry.numSlotsPerBlk];
    ret = buffer.getSlotMap(slotMap);
    if (ret != SUCCESS)
      return ret;
    for (int slot = 0; slot < relcatentry.numSlotsPerBlk; slot++)
    {
      if (slotMap[slot] == SLOT_OCCUPIED)
      {
        Attribute record[relcatentry.numAttrs];
        ret = buffer.getRecord(record, slot);
        if (ret != SUCCESS)
          return ret;
        RecId recId{block, slot};
        ret = bPlusInsert(relId, attrName, record[attrcatentry.offset], recId);
        if (ret == E_DISKFULL)
          return E_DISKFULL;
        if (ret != SUCCESS)
          return ret;
      }
    }
    struct HeadInfo head;
    ret = buffer.getHeader(&head);
    if (ret != SUCCESS)
      return ret;
    block = head.rblock;
  }
  return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum)
{
  if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS)
    return E_OUTOFBOUND;
  int type = StaticBuffer::getStaticBlockType(rootBlockNum);
  if (type == IND_LEAF)
  {
    IndLeaf leafBlock(rootBlockNum);
    leafBlock.releaseBlock();
    return SUCCESS;
  }
  else if (type == IND_INTERNAL)
  {
    IndInternal internalBlock(rootBlockNum);
    struct HeadInfo head;
    int ret = internalBlock.getHeader(&head);
    if (ret != SUCCESS)
      return SUCCESS;
    struct InternalEntry entry;
    internalBlock.getEntry(&entry, 0);
    bPlusDestroy(entry.lChild);
    for (int i = 0; i < head.numEntries; i++)
    {
      InternalEntry entry;
      internalBlock.getEntry(&entry, i);
      bPlusDestroy(entry.rChild);
    }
    internalBlock.releaseBlock();
    return SUCCESS;
  }
  else
    return E_INVALIDBLOCK;
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId)
{
  AttrCatEntry attrcatentry;
  int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcatentry);
  if (ret != SUCCESS)
    return ret;
  int blockNum = attrcatentry.rootBlock;
  if (blockNum == -1)
    return E_NOINDEX;
  int leafBlkNum = findLeafToInsert(blockNum, attrVal, attrcatentry.attrType);
  struct Index leafData;
  leafData.attrVal = attrVal;
  leafData.block = recId.block;
  leafData.slot = recId.slot;
  ret = insertIntoLeaf(relId, attrName, leafBlkNum, leafData);
  if (ret == E_DISKFULL)
  {
    bPlusDestroy(blockNum);
    attrcatentry.rootBlock = -1;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrcatentry);
    return E_DISKFULL;
  }
  return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType)
{
  int blockNum = rootBlock;
  while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF)
  {
    IndInternal internalBlock(blockNum);
    struct HeadInfo head;
    int ret = internalBlock.getHeader(&head);
    if (ret != SUCCESS)
      return ret;
    int found = 0;
    struct InternalEntry entry;
    for (int i = 0; i < head.numEntries; i++)
    {
      internalBlock.getEntry(&entry, i);
      if (compareAttrs(entry.attrVal, attrVal, attrType) >= 0)
      {
        found = 1;
        break;
      }
    }
    if (!found)
    {
      internalBlock.getEntry(&entry, head.numEntries - 1);
      blockNum = entry.rChild;
    }
    else
      blockNum = entry.lChild;
  }
  return blockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry)
{
  AttrCatEntry attrcatentry;
  int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcatentry);
  if (ret != SUCCESS)
    return ret;
  IndLeaf leafBlock(blockNum);
  struct HeadInfo blockHeader;
  ret = leafBlock.getHeader(&blockHeader);
  if (ret != SUCCESS)
    return ret;
  Index indices[blockHeader.numEntries + 1];

  bool inserted = false;
  int idx = 0;
  for (int i = 0; i < blockHeader.numEntries; i++)
  {
    Index entry;
    ret = leafBlock.getEntry(&entry, i);
    if (ret != SUCCESS)
      return ret;
    if (!inserted && compareAttrs(indexEntry.attrVal, entry.attrVal, attrcatentry.attrType) <= 0)
    {
      indices[idx++] = indexEntry;
      inserted = true;
    }
    indices[idx++] = entry;
  }
  if (!inserted)
    indices[idx] = indexEntry;

  if (blockHeader.numEntries < MAX_KEYS_LEAF)
  {
    blockHeader.numEntries++;
    ret = leafBlock.setHeader(&blockHeader);
    if (ret != SUCCESS)
      return ret;
    for (int i = 0; i < blockHeader.numEntries; i++)
    {
      ret = leafBlock.setEntry(&indices[i], i);
      if (ret != SUCCESS)
        return ret;
    }
    return SUCCESS;
  }

  int newRightBlk = splitLeaf(blockNum, indices);
  if (newRightBlk == E_DISKFULL)
    return E_DISKFULL;

  // FIX: use outer `ret` and return it — don't declare a new local `ret` inside the if
  if (blockHeader.pblock != -1)
  {
    InternalEntry middleEntry;
    middleEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
    middleEntry.lChild = blockNum;
    middleEntry.rChild = newRightBlk;
    ret = insertIntoInternal(relId, attrName, blockHeader.pblock, middleEntry);
  }
  else
    ret = createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlk);
  return ret;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[])
{
  IndLeaf rightBlk;
  IndLeaf leftBlk(leafBlockNum);
  int rightBlkNum = rightBlk.getBlockNum();
  int leftBlkNum = leftBlk.getBlockNum();
  // FIX: use MIDDLE_INDEX_LEAF+1 as the split point (reference uses this constant)
  int half = (MAX_KEYS_LEAF + 1) / 2; // = 32 = MIDDLE_INDEX_LEAF + 1
  if (rightBlkNum == E_DISKFULL)
    return E_DISKFULL;
  struct HeadInfo leftBlkHeader, rightBlkHeader;
  int ret = leftBlk.getHeader(&leftBlkHeader);
  if (ret != SUCCESS)
    return ret;
  ret = rightBlk.getHeader(&rightBlkHeader);
  if (ret != SUCCESS)
    return ret;
  rightBlkHeader.numEntries = half;
  rightBlkHeader.pblock = leftBlkHeader.pblock;
  rightBlkHeader.lblock = leftBlkNum;
  rightBlkHeader.rblock = leftBlkHeader.rblock;
  ret = rightBlk.setHeader(&rightBlkHeader);
  if (ret != SUCCESS)
    return ret;
  leftBlkHeader.numEntries = half;
  leftBlkHeader.rblock = rightBlkNum;
  ret = leftBlk.setHeader(&leftBlkHeader);
  if (ret != SUCCESS)
    return ret;

  // FIX: loop only `half` times (0..31), right block starts at index `half` (=MIDDLE_INDEX_LEAF+1)
  for (int i = 0; i < half; i++)
  {
    ret = leftBlk.setEntry(&indices[i], i);
    if (ret != SUCCESS)
      return ret;
    ret = rightBlk.setEntry(&indices[i + half], i);
    if (ret != SUCCESS)
      return ret;
  }
  return rightBlkNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry)
{
  AttrCatEntry attrcatentry;
  int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcatentry);
  if (ret != SUCCESS)
    return ret;
  IndInternal intBlk(intBlockNum);
  struct HeadInfo intBlkHeader;
  ret = intBlk.getHeader(&intBlkHeader);
  if (ret != SUCCESS)
    return ret;
  InternalEntry internalEntries[intBlkHeader.numEntries + 1];
  bool inserted = false;
  int idx = 0;
  for (int i = 0; i < intBlkHeader.numEntries; i++)
  {
    InternalEntry entry;
    ret = intBlk.getEntry(&entry, i);
    if (ret != SUCCESS)
      return ret;
    if (!inserted && compareAttrs(intEntry.attrVal, entry.attrVal, attrcatentry.attrType) <= 0)
    {
      internalEntries[idx++] = intEntry;
      inserted = true;
      entry.lChild = intEntry.rChild;
    }
    internalEntries[idx++] = entry;
  }
  if (!inserted)
    internalEntries[idx] = intEntry;

  if (intBlkHeader.numEntries < MAX_KEYS_INTERNAL)
  {
    intBlkHeader.numEntries++;
    ret = intBlk.setHeader(&intBlkHeader);
    if (ret != SUCCESS)
      return ret;
    for (int i = 0; i < intBlkHeader.numEntries; i++)
    {
      ret = intBlk.setEntry(&internalEntries[i], i);
      if (ret != SUCCESS)
        return ret;
    }
    return SUCCESS;
  }

  int newRightBlk = splitInternal(intBlockNum, internalEntries);
  if (newRightBlk == E_DISKFULL)
  {
    bPlusDestroy(intEntry.rChild);
    return E_DISKFULL;
  }
  if (intBlkHeader.pblock != -1)
  {
    InternalEntry middleEntry;
    middleEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
    middleEntry.lChild = intBlockNum;
    middleEntry.rChild = newRightBlk;
    ret = insertIntoInternal(relId, attrName, intBlkHeader.pblock, middleEntry);
  }
  else
    ret = createNewRoot(relId, attrName, internalEntries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlk);
  return ret;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[])
{
  IndInternal rightBlk;
  IndInternal leftBlk(intBlockNum);
  int rightBlkNum = rightBlk.getBlockNum();
  int leftBlkNum = intBlockNum;
  if (rightBlkNum == E_DISKFULL)
    return E_DISKFULL;
  struct HeadInfo leftBlkHeader, rightBlkHeader;
  int ret = leftBlk.getHeader(&leftBlkHeader);
  if (ret != SUCCESS)
    return ret;
  ret = rightBlk.getHeader(&rightBlkHeader);
  if (ret != SUCCESS)
    return ret;

  // MIDDLE_INDEX_INTERNAL = 50, each side gets 50 entries, index 50 goes to parent
  leftBlkHeader.numEntries = MIDDLE_INDEX_INTERNAL;  // 50
  rightBlkHeader.numEntries = MIDDLE_INDEX_INTERNAL; // 50
  rightBlkHeader.pblock = leftBlkHeader.pblock;
  ret = rightBlk.setHeader(&rightBlkHeader);
  if (ret != SUCCESS)
    return ret;
  ret = leftBlk.setHeader(&leftBlkHeader);
  if (ret != SUCCESS)
    return ret;

  // FIX: left gets indices[0..49], right gets indices[51..100]
  // (index 50 = MIDDLE_INDEX_INTERNAL goes up to parent)
  for (int i = 0; i < MIDDLE_INDEX_INTERNAL; i++)
  {
    ret = leftBlk.setEntry(&internalEntries[i], i);
    if (ret != SUCCESS)
      return ret;
    ret = rightBlk.setEntry(&internalEntries[i + MIDDLE_INDEX_INTERNAL + 1], i);
    if (ret != SUCCESS)
      return ret;
  }

  // FIX: update pblock of all children of the right block
  // right block's children are the lChild of each entry + rChild of last entry
  // i.e. internalEntries[MIDDLE_INDEX_INTERNAL+1].lChild through internalEntries[100].rChild
  BlockBuffer firstRightChild(internalEntries[MIDDLE_INDEX_INTERNAL + 1].lChild);
  struct HeadInfo firstRightChildHeader;
  firstRightChild.getHeader(&firstRightChildHeader);
  firstRightChildHeader.pblock = rightBlkNum;
  firstRightChild.setHeader(&firstRightChildHeader);

  for (int i = 0; i < MIDDLE_INDEX_INTERNAL; i++)
  {
    BlockBuffer childBlk(internalEntries[MIDDLE_INDEX_INTERNAL + 1 + i].rChild);
    struct HeadInfo childBlkHeader;
    ret = childBlk.getHeader(&childBlkHeader);
    if (ret != SUCCESS)
      return ret;
    childBlkHeader.pblock = rightBlkNum;
    ret = childBlk.setHeader(&childBlkHeader);
    if (ret != SUCCESS)
      return ret;
  }

  return rightBlkNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild)
{
  AttrCatEntry attrcatentry;
  int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcatentry);
  if (ret != SUCCESS)
    return ret;
  IndInternal newRootBlk;
  int newRootBlkNum = newRootBlk.getBlockNum();
  if (newRootBlkNum == E_DISKFULL)
  {
    bPlusDestroy(rChild);
    return E_DISKFULL;
  }
  struct HeadInfo newRootBlkHeader;
  ret = newRootBlk.getHeader(&newRootBlkHeader);
  if (ret != SUCCESS)
    return ret;
  newRootBlkHeader.numEntries = 1;
  ret = newRootBlk.setHeader(&newRootBlkHeader);
  if (ret != SUCCESS)
    return ret;
  InternalEntry entry;
  entry.lChild = lChild;
  entry.rChild = rChild;
  entry.attrVal = attrVal;
  ret = newRootBlk.setEntry(&entry, 0);

  BlockBuffer leftChildBlk(lChild);
  struct HeadInfo leftChildHeader;
  ret = leftChildBlk.getHeader(&leftChildHeader);
  if (ret != SUCCESS)
    return ret;
  leftChildHeader.pblock = newRootBlkNum;
  ret = leftChildBlk.setHeader(&leftChildHeader);
  if (ret != SUCCESS)
    return ret;

  BlockBuffer rightChildBlk(rChild);
  struct HeadInfo rightChildHeader;
  ret = rightChildBlk.getHeader(&rightChildHeader);
  if (ret != SUCCESS)
    return ret;
  rightChildHeader.pblock = newRootBlkNum;
  ret = rightChildBlk.setHeader(&rightChildHeader);
  if (ret != SUCCESS)
    return ret;

  attrcatentry.rootBlock = newRootBlkNum;
  ret = AttrCacheTable::setAttrCatEntry(relId, attrName, &attrcatentry);
  if (ret != SUCCESS)
    return ret;
  return SUCCESS;
}
