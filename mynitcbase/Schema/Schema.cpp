#include "Schema.h"
#include <cmath>
#include <cstring>
#include <vector>
#include <string>

int Schema::openRel(char relName[ATTR_SIZE])
{
  int ret = OpenRelTable::openRel(relName);
  if (ret >= 0)
    return SUCCESS;
  return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE])
{
  if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;
  int relId = OpenRelTable::getRelId(relName);
  RelCatEntry relCatEntry;
  if (relId == E_RELNOTOPEN)
    return E_RELNOTOPEN;
  return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE])
{
  if (strcmp(oldRelName, RELCAT_RELNAME) == 0 || strcmp(oldRelName, ATTRCAT_RELNAME) == 0 || strcmp(newRelName, RELCAT_RELNAME) == 0 || strcmp(newRelName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;
  if (OpenRelTable::getRelId(oldRelName) != E_RELNOTOPEN)
    return E_RELOPEN;
  int retVal = BlockAccess::renameRelation(oldRelName, newRelName);
  return retVal;
}

int Schema::renameAttr(char *relName, char *oldAttrName, char *newAttrName)
{
  if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;
  if (OpenRelTable::getRelId(relName) != E_RELNOTOPEN)
    return E_RELOPEN;
  int retVal = BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);
  return retVal;
}

int Schema::createRel(char relName[], int nAttrs, char attrs[][ATTR_SIZE], int attrtype[])
{
  union Attribute relNameAsAttribute;
  strcpy(relNameAsAttribute.sVal, relName);
  RecId targetRelId;
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  targetRelId = BlockAccess::linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, relNameAsAttribute, EQ);
  if (targetRelId.block != -1 && targetRelId.slot != -1)
    return E_RELEXIST;
  for (int i = 0; i < nAttrs; i++)
  {
    for (int j = i + 1; j < nAttrs; j++)
    {
      if (strcmp(attrs[i], attrs[j]) == 0)
        return E_DUPLICATEATTR;
    }
  }

  union Attribute relCatRecord[RELCAT_NO_ATTRS];
  strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);
  relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
  relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
  relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
  relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
  relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor(2016 / (16 * nAttrs + 1));

  int ret = BlockAccess::insert(RELCAT_RELID, relCatRecord);
  if (ret != SUCCESS)
    return ret;

  for (int i = 0; i < nAttrs; i++)
  {
    union Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
    strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[i]);
    attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrtype[i];
    attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
    attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
    attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;
    ret = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
    if (ret != SUCCESS)
    {
      Schema::deleteRel(relName);
      return ret;
    }
  }
  return SUCCESS;
}

int Schema::deleteRel(char *relName)
{
  if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;
  int relId = OpenRelTable::getRelId(relName);
  if (relId != E_RELNOTOPEN)
    return E_RELOPEN;
  return BlockAccess::deleteRelation(relName);
}

int Schema::createIndex(char relName[ATTR_SIZE], char attrName[ATTR_SIZE])
{
  if ((strcmp(relName, RELCAT_RELNAME) == 0) || (strcmp(relName, ATTRCAT_RELNAME) == 0))
    return E_NOTPERMITTED;
  int relId = OpenRelTable::getRelId(relName);
  if (relId == E_RELNOTOPEN)
    return E_RELNOTOPEN;
  return BPlusTree::bPlusCreate(relId, attrName);
}

int Schema::dropIndex(char relName[ATTR_SIZE], char attrName[ATTR_SIZE])
{
  if ((strcmp(relName, RELCAT_RELNAME) == 0) || (strcmp(relName, ATTRCAT_RELNAME) == 0))
    return E_NOTPERMITTED;
  int relId = OpenRelTable::getRelId(relName);
  if (relId == E_RELNOTOPEN)
    return E_RELNOTOPEN;
  AttrCatEntry attrcatentry;
  int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcatentry);
  if (ret != SUCCESS)
    return ret;
  int rootBlock = attrcatentry.rootBlock;
  if (rootBlock == -1)
    return E_NOINDEX;
  ret = BPlusTree::bPlusDestroy(rootBlock);
  if (ret != SUCCESS)
    return ret;
  attrcatentry.rootBlock = -1;
  ret = AttrCacheTable::setAttrCatEntry(relId, attrName, &attrcatentry);
  if (ret != SUCCESS)
    return ret;
  return SUCCESS;
}
