#include "Algebra.h"

#include <cstdio>
#include <cstring>
#include <cstdlib>

#include "../BlockAccess/BlockAccess.h"
#include "../Cache/RelCacheTable.h"
#include "../Cache/AttrCacheTable.h"
#include "../Cache/OpenRelTable.h"

bool isNumber(char *str)
{
    int len;
    float ignore;
    int ret = sscanf(str, "%f %n", &ignore, &len);
    return ret == 1 && len == strlen(str);
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE])
{
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

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE])
{
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