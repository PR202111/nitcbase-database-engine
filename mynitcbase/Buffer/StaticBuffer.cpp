#include "StaticBuffer.h"
#include "stdlib.h"
#include <cstring>

#include "../trace_macros.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];


StaticBuffer::StaticBuffer()
{
  TRACE_FUNC("StaticBuffer");
  for (int bufferidx = 0; bufferidx < BUFFER_CAPACITY; bufferidx++)
  {
    metainfo[bufferidx].free = true;
    metainfo[bufferidx].dirty = false;
    metainfo[bufferidx].blockNum = -1;
    metainfo[bufferidx].timeStamp = -1;
  }
  for (int i = 0; i < BLOCK_ALLOCATION_MAP_SIZE; i++)
  {
    unsigned char *bufferPtr = blocks[i];

    Disk::readBlock(bufferPtr, i);
    metainfo[i].free = false;
    metainfo[i].dirty = false;
    metainfo[i].blockNum = i;
    metainfo[i].timeStamp = 0;
  }
  memcpy(blockAllocMap, blocks[0], BLOCK_ALLOCATION_MAP_SIZE * BLOCK_SIZE);
}

StaticBuffer::~StaticBuffer()
{
  TRACE_FUNC("StaticBuffer");
  for (int bufferidx = 0; bufferidx < BUFFER_CAPACITY; bufferidx++)
  {
    if (metainfo[bufferidx].free == false && metainfo[bufferidx].dirty == true)
      Disk::writeBlock(StaticBuffer::blocks[bufferidx], metainfo[bufferidx].blockNum);
  }
  memcpy(blocks[0], blockAllocMap, BLOCK_ALLOCATION_MAP_SIZE * BLOCK_SIZE);
  for (int i = 0; i < BLOCK_ALLOCATION_MAP_SIZE; i++)
    Disk::writeBlock(blocks[i], i);
}

int StaticBuffer::getFreeBuffer(int blockNum)
{
  TRACE_FUNC("StaticBuffer");
  if (blockNum < 0 || blockNum >= DISK_BLOCKS)
    return E_OUTOFBOUND;
  for (int bufferidx = 0; bufferidx < BUFFER_CAPACITY; bufferidx++)
  {
    if (!metainfo[bufferidx].free)
      metainfo[bufferidx].timeStamp++;
  }
  int bufferNum = -1;
  for (int bufferidx = 0; bufferidx < BUFFER_CAPACITY; bufferidx++)
  {
    if (metainfo[bufferidx].free)
    {
      bufferNum = bufferidx;
      break;
    }
  }
  if (bufferNum == -1)
  {
    int maxTime = -1;
    for (int bufferidx = 0; bufferidx < BUFFER_CAPACITY; bufferidx++)
    {
      if (metainfo[bufferidx].timeStamp > maxTime)
      {
        maxTime = metainfo[bufferidx].timeStamp;
        bufferNum = bufferidx;
      }
    }
    if (metainfo[bufferNum].dirty)
      Disk::writeBlock(blocks[bufferNum], metainfo[bufferNum].blockNum);
  }
  metainfo[bufferNum].free = false;
  metainfo[bufferNum].dirty = false;
  metainfo[bufferNum].blockNum = blockNum;
  metainfo[bufferNum].timeStamp = 0;

  return bufferNum;
}

int StaticBuffer::getBufferNum(int blockNum)
{
  TRACE_FUNC("StaticBuffer");
  if (blockNum < 0 || blockNum >= DISK_BLOCKS)
    return E_OUTOFBOUND;
  for (int bufferidx = 0; bufferidx < BUFFER_CAPACITY; bufferidx++)
  {
    if (metainfo[bufferidx].free == false && metainfo[bufferidx].blockNum == blockNum)
      return bufferidx;
  }
  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum)
{
  TRACE_FUNC("StaticBuffer");
  int bufferNum = getBufferNum(blockNum);
  if (bufferNum == E_BLOCKNOTINBUFFER)
    return E_BLOCKNOTINBUFFER;
  else if (bufferNum == E_OUTOFBOUND)
    return E_OUTOFBOUND;
  else
    metainfo[bufferNum].dirty = true;
  return SUCCESS;
}

int StaticBuffer::getStaticBlockType(int blockNum)
{
  TRACE_FUNC("StaticBuffer");
  if (blockNum < 0 || blockNum >= DISK_BLOCKS)
    return E_OUTOFBOUND;
  return (int)blockAllocMap[blockNum];
}
