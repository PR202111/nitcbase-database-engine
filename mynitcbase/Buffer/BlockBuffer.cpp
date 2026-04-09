#include "BlockBuffer.h"
#include <cstdlib>
#include <cstring>

BlockBuffer::BlockBuffer(char blocktype)
{
  int blockType = blocktype == 'R' ? REC : blocktype == 'I' ? IND_INTERNAL
                                       : blocktype == 'L'   ? IND_LEAF
                                                            : UNUSED_BLK;
  int blockNum = getFreeBlock(blockType);
  if (blockNum < 0 || blockNum >= DISK_BLOCKS)
  {
    this->blockNum = blockNum;
    return;
  }
}

RecBuffer::RecBuffer() : BlockBuffer('R') {}

BlockBuffer::BlockBuffer(int blockNum)
{
  this->blockNum = blockNum;
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType) {}

IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum) {}

IndInternal::IndInternal() : IndBuffer('I') {}

IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum) {}

IndLeaf::IndLeaf() : IndBuffer('L') {}

IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum) {}

int BlockBuffer::setHeader(struct HeadInfo *head)
{
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
    return ret;
  struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;
  bufferHeader->blockType = head->blockType;
  bufferHeader->lblock = head->lblock;
  bufferHeader->rblock = head->rblock;
  bufferHeader->numAttrs = head->numAttrs;
  bufferHeader->numEntries = head->numEntries;
  bufferHeader->numSlots = head->numSlots;
  bufferHeader->pblock = head->pblock;
  int dirtyBitSetter = StaticBuffer::setDirtyBit(this->blockNum);
  if (dirtyBitSetter != SUCCESS)
    return dirtyBitSetter;
  return SUCCESS;
}

int BlockBuffer::getHeader(struct HeadInfo *head)
{
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
    return ret;
  memcpy(&head->blockType, bufferPtr, 4);
  memcpy(&head->pblock, bufferPtr + 4, 4);
  memcpy(&head->lblock, bufferPtr + 8, 4);
  memcpy(&head->rblock, bufferPtr + 12, 4);
  memcpy(&head->numEntries, bufferPtr + 16, 4);
  memcpy(&head->numAttrs, bufferPtr + 20, 4);
  memcpy(&head->numSlots, bufferPtr + 24, 4);
  return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType)
{
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
    return ret;
  *((int32_t *)bufferPtr) = blockType;
  StaticBuffer::blockAllocMap[this->blockNum] = blockType;
  int dirtyBitChecker = StaticBuffer::setDirtyBit(this->blockNum);
  if (dirtyBitChecker != SUCCESS)
    return dirtyBitChecker;
  return SUCCESS;
}

int BlockBuffer::getFreeBlock(int blockType)
{
  int freeBlock = -1;
  for (int i = 0; i < DISK_BLOCKS; i++)
  {
    if (StaticBuffer::blockAllocMap[i] == UNUSED_BLK)
    {
      freeBlock = i;
      break;
    }
  }
  if (freeBlock == -1)
    return E_DISKFULL;
  this->blockNum = freeBlock;
  int bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);
  if (bufferNum < 0)
    return bufferNum;
  struct HeadInfo head;
  head.blockType = blockType;
  head.lblock = -1;
  head.rblock = -1;
  head.pblock = -1;
  head.numAttrs = -1;
  head.numSlots = -1;
  head.numEntries = 0;
  int ret = this->setHeader(&head);
  if (ret != SUCCESS)
    return ret;
  ret = this->setBlockType(blockType);
  if (ret != SUCCESS)
    return ret;
  return this->blockNum;
}

int RecBuffer::getRecord(union Attribute *rec, int slotNum)
{
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
    return ret;
  struct HeadInfo head;
  this->getHeader(&head);
  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;
  if (slotNum < 0 || slotNum >= slotCount)
    return E_OUTOFBOUND;
  int recordSize = attrCount * ATTR_SIZE;
  int offset = HEADER_SIZE + slotCount + (slotNum * recordSize);
  unsigned char *slotPointer = bufferPtr + offset;
  memcpy(rec, slotPointer, recordSize);
  return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum)
{
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
    return ret;
  struct HeadInfo head;
  this->getHeader(&head);
  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;
  if (slotNum < 0 || slotNum >= slotCount)
    return E_OUTOFBOUND;
  int recordSize = attrCount * ATTR_SIZE;
  int offset = HEADER_SIZE + slotCount + (slotNum * recordSize);
  unsigned char *slotPointer = bufferPtr + offset;
  memcpy(slotPointer, rec, recordSize);
  StaticBuffer::setDirtyBit(this->blockNum);
  return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr)
{
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);
  if (bufferNum != E_BLOCKNOTINBUFFER && bufferNum != E_OUTOFBOUND)
  {
    StaticBuffer::metainfo[bufferNum].timeStamp = 0;
    for (int bufferidx = 0; bufferidx < BUFFER_CAPACITY; bufferidx++)
    {
      if (bufferidx != bufferNum && !StaticBuffer::metainfo[bufferidx].free)
        StaticBuffer::metainfo[bufferidx].timeStamp++;
    }
  }
  else
  {
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);
    if (bufferNum == E_OUTOFBOUND)
      return E_OUTOFBOUND;
    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }
  *buffPtr = StaticBuffer::blocks[bufferNum];
  return SUCCESS;
}

int RecBuffer::getSlotMap(unsigned char *slotMap)
{
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
    return ret;

  RecBuffer recordBlock(this->blockNum);
  struct HeadInfo head;
  recordBlock.getHeader(&head);

  int slotCount = head.numSlots;
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;
  for (int slots = 0; slots < slotCount; slots++)
    *(slotMap + slots) = *(slotMapInBuffer + slots);
  return SUCCESS;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType)
{
  double diff;
  if (attrType == STRING)
    diff = strcmp(attr1.sVal, attr2.sVal); // ? compare strings and keep difference
  else
    diff = attr1.nVal - attr2.nVal; // ? compare numbers and keep difference
  if (diff > 0)
    return 1;
  if (diff < 0)
    return -1;
  return 0;
}

int RecBuffer::setSlotMap(unsigned char *slotMap)
{
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
    return ret;
  struct HeadInfo head;
  ret = this->getHeader(&head);
  if (ret != SUCCESS)
    return ret;
  int numSlots = head.numSlots;
  unsigned char *slotMapFront = bufferPtr + HEADER_SIZE;
  memcpy(slotMapFront, slotMap, numSlots);
  ret = StaticBuffer::setDirtyBit(this->blockNum);
  if (ret != SUCCESS)
    return ret;
  return SUCCESS;
}

int BlockBuffer::getBlockNum()
{
  return this->blockNum;
}

void BlockBuffer::releaseBlock()
{
  if (blockNum == INVALID_BLOCKNUM || StaticBuffer::blockAllocMap[blockNum] == UNUSED_BLK)
    return;
  int bufferNum = StaticBuffer::getBufferNum(blockNum);
  if (bufferNum >= 0 && bufferNum < BUFFER_CAPACITY)
    StaticBuffer::metainfo[bufferNum].free = true;
  StaticBuffer::blockAllocMap[blockNum] = UNUSED_BLK;
  this->blockNum = INVALID_BLOCKNUM;
  return;
}

int IndInternal::getEntry(void *ptr, int indexNum)
{
  if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL)
    return E_OUTOFBOUND;
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
    return ret;
  struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;
  unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * (LCHILD_SIZE + ATTR_SIZE));

  memcpy(&(internalEntry->lChild), entryPtr, LCHILD_SIZE);
  memcpy(&(internalEntry->attrVal), entryPtr + LCHILD_SIZE, ATTR_SIZE);
  memcpy(&(internalEntry->rChild), entryPtr + LCHILD_SIZE + ATTR_SIZE, RCHILD_SIZE);
  return SUCCESS;
}

int IndLeaf::getEntry(void *ptr, int indexNum)
{
  if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
    return E_OUTOFBOUND;
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
    return ret;
  struct Index *leafEntry = (struct Index *)ptr;
  unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
  memcpy(leafEntry, entryPtr, LEAF_ENTRY_SIZE);
  return SUCCESS;
}

int IndInternal::setEntry(void *ptr, int indexNum)
{
  if (indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL)
    return E_OUTOFBOUND;
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
    return ret;
  struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;
  unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * (LCHILD_SIZE + ATTR_SIZE));
  memcpy(entryPtr, &(internalEntry->lChild), LCHILD_SIZE);
  memcpy(entryPtr + LCHILD_SIZE, &(internalEntry->attrVal), ATTR_SIZE);
  memcpy(entryPtr + LCHILD_SIZE + ATTR_SIZE, &(internalEntry->rChild), RCHILD_SIZE);
  ret = StaticBuffer::setDirtyBit(this->blockNum);
  if (ret != SUCCESS)
    return ret;
  return SUCCESS;
}

int IndLeaf::setEntry(void *ptr, int indexNum)
{
  if (indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
    return E_OUTOFBOUND;
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS)
    return ret;
  struct Index *leafEntry = (struct Index *)ptr;
  unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
  memcpy(entryPtr, leafEntry, LEAF_ENTRY_SIZE);
  ret = StaticBuffer::setDirtyBit(this->blockNum);
  if (ret != SUCCESS)
    return ret;
  return SUCCESS;
}
