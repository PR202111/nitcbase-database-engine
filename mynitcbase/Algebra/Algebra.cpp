#include "Algebra.h"

#include <cstdio>
#include <cstring>
#include <cstdlib>

#include "../BlockAccess/BlockAccess.h"
#include "../Cache/RelCacheTable.h"
#include "../Cache/AttrCacheTable.h"
#include "../Cache/OpenRelTable.h"
#include "../Buffer/StaticBuffer.h"

#include "../trace_macros.h"

#define COL_WIDTH 15

bool isNumber(char *str)
{
  TRACE_FUNC("Algebra");
  int len;
  float ignore;
  int ret = sscanf(str, "%f %n", &ignore, &len);
  return ret == 1 && len == strlen(str);
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE])
{
  TRACE_FUNC("Algebra");
  if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;
  int relId = OpenRelTable::getRelId(relName);
  if (relId == E_RELNOTOPEN)
    return E_RELNOTOPEN;
  RelCatEntry Relentry;
  int ret = RelCacheTable::getRelCatEntry(relId, &Relentry);
  if (ret != SUCCESS)
    return ret;
  if (nAttrs != Relentry.numAttrs)
    return E_NATTRMISMATCH;
  union Attribute recordValues[nAttrs];
  for (int i = 0; i < nAttrs; i++)
  {
    AttrCatEntry Attrentry;
    ret = AttrCacheTable::getAttrCatEntry(relId, i, &Attrentry);
    if (ret != SUCCESS)
      return ret;
    int type = Attrentry.attrType;
    if (type == NUMBER)
    {
      if (isNumber(record[i]))
        recordValues[i].nVal = atof(record[i]);
      else
        return E_ATTRTYPEMISMATCH;
    }
    else if (type == STRING)
      strcpy(recordValues[i].sVal, record[i]);
  }
  return BlockAccess::insert(relId, recordValues);
}

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE])
{
  TRACE_FUNC("Algebra");
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if (srcRelId == E_RELNOTOPEN)
    return E_RELNOTOPEN;

  AttrCatEntry condAttr;
  if (AttrCacheTable::getAttrCatEntry(srcRelId, attr, &condAttr) != SUCCESS)
    return E_ATTRNOTEXIST;

  Attribute attrVal;
  int type = condAttr.attrType;
  if (type == NUMBER)
  {
    if (!isNumber(strVal))
      return E_ATTRTYPEMISMATCH;
    attrVal.nVal = atof(strVal);
  }
  else if (type == STRING)
    strcpy(attrVal.sVal, strVal);

  RelCacheTable::resetSearchIndex(srcRelId);
  RelCatEntry relCat;
  if (RelCacheTable::getRelCatEntry(srcRelId, &relCat) != SUCCESS)
    return E_RELNOTOPEN;
  int src_nAttrs = relCat.numAttrs;
  char attr_names[src_nAttrs][ATTR_SIZE];
  int attr_types[src_nAttrs];

  for (int i = 0; i < src_nAttrs; i++)
  {
    AttrCatEntry attrEntry;
    if (AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrEntry) != SUCCESS)
      return E_ATTRNOTEXIST;
    strcpy(attr_names[i], attrEntry.attrName);
    attr_types[i] = attrEntry.attrType;
  }

  int ret = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
  if (ret != SUCCESS)
    return ret;
  int targetRelId = OpenRelTable::openRel(targetRel);
  if (targetRelId < 0)
  {
    Schema::deleteRel(targetRel);
    return targetRelId;
  }

  // FIX: removed the extra stray printf("|"); that was here before the separator line
  printf("\n|");
  for (int i = 0; i < src_nAttrs; i++)
  {
    for (int j = 0; j < COL_WIDTH + 2; j++)
      printf("-");
    printf("|");
  }
  printf("\n");

  printf("|");
  for (int i = 0; i < src_nAttrs; i++)
    printf(" %-*s |", COL_WIDTH, attr_names[i]);
  printf("\n");

  printf("|");
  for (int i = 0; i < src_nAttrs; i++)
  {
    for (int j = 0; j < COL_WIDTH + 2; j++)
      printf("-");
    printf("|");
  }
  printf("\n");

  RelCacheTable::resetSearchIndex(srcRelId);
  AttrCacheTable::resetSearchIndex(srcRelId, attr);

  Attribute record[src_nAttrs];

  while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS)
  {
    
    ret = BlockAccess::insert(targetRelId, record);

    

    if (ret != SUCCESS)
    {
      Schema::closeRel(targetRel);
      Schema::deleteRel(targetRel);
      return ret;
    }
  }
  
  

  Schema::closeRel(targetRel);
  return SUCCESS;
}

// project all attributes
int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE])
{
  TRACE_FUNC("Algebra");
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if (srcRelId == E_RELNOTOPEN)
    return E_RELNOTOPEN;
  RelCatEntry relCatBuf;
  int ret = RelCacheTable::getRelCatEntry(srcRelId, &relCatBuf);
  if (ret != SUCCESS)
    return ret;
  int src_nAttrs = relCatBuf.numAttrs;
  char attrNames[src_nAttrs][ATTR_SIZE];
  int attrTypes[src_nAttrs];

  for (int i = 0; i < src_nAttrs; i++)
  {
    AttrCatEntry attrCatBuf;
    ret = AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatBuf);
    if (ret != SUCCESS)
      return ret;
    strcpy(attrNames[i], attrCatBuf.attrName);
    attrTypes[i] = attrCatBuf.attrType;
  }
  ret = Schema::createRel(targetRel, src_nAttrs, attrNames, attrTypes);
  if (ret != SUCCESS)
    return ret;
  int targetRelId = OpenRelTable::openRel(targetRel);
  if (targetRelId < 0)
  {
    Schema::deleteRel(targetRel);
    return targetRelId;
  }

  RelCacheTable::resetSearchIndex(srcRelId);
  Attribute record[src_nAttrs];

  while (BlockAccess::project(srcRelId, record) == SUCCESS)
  {
    ret = BlockAccess::insert(targetRelId, record);
    if (ret != SUCCESS)
    {
      Schema::closeRel(targetRel);
      Schema::deleteRel(targetRel);
      return ret;
    }
  }
  Schema::closeRel(targetRel);
  return SUCCESS;
}

// project specified attributes
int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE])
{
  TRACE_FUNC("Algebra");
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if (srcRelId == E_RELNOTOPEN)
    return E_RELNOTOPEN;
  RelCatEntry relCatBuf;
  int ret = RelCacheTable::getRelCatEntry(srcRelId, &relCatBuf);
  if (ret != SUCCESS)
    return ret;
  int src_nAttrs = relCatBuf.numAttrs;
  int attr_offset[tar_nAttrs];
  int attr_types[tar_nAttrs];
  for (int i = 0; i < tar_nAttrs; i++)
  {
    AttrCatEntry attrCatBuf;
    ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &attrCatBuf);
    if (ret != SUCCESS)
      return ret;
    attr_offset[i] = attrCatBuf.offset;
    attr_types[i] = attrCatBuf.attrType;
  }
  ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);
  if (ret != SUCCESS)
    return ret;
  int targetRelId = OpenRelTable::openRel(targetRel);
  if (targetRelId < 0)
  {
    Schema::deleteRel(targetRel);
    return targetRelId;
  }

  RelCacheTable::resetSearchIndex(srcRelId);
  Attribute record[src_nAttrs];

  while (BlockAccess::project(srcRelId, record) == SUCCESS)
  {
    Attribute project_record[tar_nAttrs];
    for (int i = 0; i < tar_nAttrs; i++)
      project_record[i] = record[attr_offset[i]];
    ret = BlockAccess::insert(targetRelId, project_record);
    if (ret != SUCCESS)
    {
      Schema::closeRel(targetRel);
      Schema::deleteRel(targetRel);
      return ret;
    }
  }
  Schema::closeRel(targetRel);
  return SUCCESS;
}

int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE])
{
  TRACE_FUNC("Algebra");
  int relId1 = OpenRelTable::getRelId(srcRelation1);
  if (relId1 == E_RELNOTOPEN)
    return E_RELNOTOPEN;
  int relId2 = OpenRelTable::getRelId(srcRelation2);
  if (relId2 == E_RELNOTOPEN)
    return E_RELNOTOPEN;

  AttrCatEntry attrcatentry1, attrcatentry2;
  int ret = AttrCacheTable::getAttrCatEntry(relId1, attribute1, &attrcatentry1);
  if (ret != SUCCESS)
    return ret;
  ret = AttrCacheTable::getAttrCatEntry(relId2, attribute2, &attrcatentry2);
  if (ret != SUCCESS)
    return ret;

  if (attrcatentry1.attrType != attrcatentry2.attrType)
    return E_ATTRTYPEMISMATCH;

  RelCatEntry relcatentry1, relcatentry2;
  RelCacheTable::getRelCatEntry(relId1, &relcatentry1);
  RelCacheTable::getRelCatEntry(relId2, &relcatentry2);

  // FIX: also skip attribute1 in rel1 during duplicate check (reference code does this)
  for (int i = 0; i < relcatentry1.numAttrs; i++)
  {
    AttrCatEntry x1;
    AttrCacheTable::getAttrCatEntry(relId1, i, &x1);
    if (strcmp(x1.attrName, attribute1) == 0)
      continue;
    for (int j = 0; j < relcatentry2.numAttrs; j++)
    {
      AttrCatEntry x2;
      AttrCacheTable::getAttrCatEntry(relId2, j, &x2);
      if (strcmp(x2.attrName, attribute2) == 0)
        continue;
      if (strcmp(x1.attrName, x2.attrName) == 0)
        return E_DUPLICATEATTR;
    }
  }
  int rootBlock = attrcatentry2.rootBlock;
  if (rootBlock == -1)
  {
    ret = BPlusTree::bPlusCreate(relId2, attribute2);
    if (ret != SUCCESS)
      return ret;
    rootBlock = attrcatentry2.rootBlock;
  }

  int numOfAttributes1 = relcatentry1.numAttrs;
  int numOfAttributes2 = relcatentry2.numAttrs;
  int numOfAttributesInTarget = numOfAttributes1 + numOfAttributes2 - 1;

  char targetRelAttrNames[numOfAttributesInTarget][ATTR_SIZE];
  int targetRelAttrTypes[numOfAttributesInTarget];

  for (int i = 0; i < numOfAttributes1; i++)
  {
    AttrCatEntry x;
    AttrCacheTable::getAttrCatEntry(relId1, i, &x);
    strcpy(targetRelAttrNames[i], x.attrName);
    targetRelAttrTypes[i] = x.attrType;
  }
  for (int i = 0, flag = 0; i < numOfAttributes2; i++)
  {
    AttrCatEntry x;
    AttrCacheTable::getAttrCatEntry(relId2, i, &x);
    if (strcmp(x.attrName, attribute2) == 0)
    {
      flag = 1;
      continue;
    }
    strcpy(targetRelAttrNames[numOfAttributes1 + i - flag], x.attrName);
    targetRelAttrTypes[numOfAttributes1 + i - flag] = x.attrType;
  }

  ret = Schema::createRel(targetRelation, numOfAttributesInTarget, targetRelAttrNames, targetRelAttrTypes);
  if (ret != SUCCESS)
    return ret;

  int targetRelId = OpenRelTable::openRel(targetRelation);
  if (targetRelId < 0)
  {
    Schema::deleteRel(targetRelation);
    return targetRelId;
  }

  Attribute record1[numOfAttributes1];
  Attribute record2[numOfAttributes2];
  Attribute targetRecord[numOfAttributesInTarget];

  RelCacheTable::resetSearchIndex(relId1);
  while (BlockAccess::project(relId1, record1) == SUCCESS)
  {
    RelCacheTable::resetSearchIndex(relId2);
    AttrCacheTable::resetSearchIndex(relId2, attribute2);

    while (BlockAccess::search(relId2, record2, attribute2, record1[attrcatentry1.offset], EQ) == SUCCESS)
    {
      for (int i = 0; i < numOfAttributes1; i++)
        targetRecord[i] = record1[i];
      for (int i = 0, flag = 0; i < numOfAttributes2; i++)
      {
        if (attrcatentry2.offset == i)
        {
          flag = 1;
          continue;
        }
        targetRecord[numOfAttributes1 + i - flag] = record2[i];
      }
      ret = BlockAccess::insert(targetRelId, targetRecord);
      if (ret == E_DISKFULL)
      {
        OpenRelTable::closeRel(targetRelId);
        Schema::deleteRel(targetRelation);
        return E_DISKFULL;
      }
    }
  }

  OpenRelTable::closeRel(targetRelId);
  return SUCCESS;
}
