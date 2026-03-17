#include "Algebra.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cstring>
#include <iostream>


inline bool isNumber(char *str)
{
    int len;
    float ignore;
    
    int ret = sscanf(str, "%f %n", &ignore, &len);
    return ret == 1 && len == strlen(str);
}

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE])
{
    int srcRelId = OpenRelTable::getRelId(srcRel); // we'll implement this later
    if (srcRelId == E_RELNOTOPEN)
        return E_RELNOTOPEN;


    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);

    if (ret == E_ATTRNOTEXIST)
        return E_ATTRNOTEXIST;

    
    int type = attrCatEntry.attrType;
    Attribute attrVal;
    if (type == NUMBER)
    {
        if (isNumber(strVal))
        { 
            attrVal.nVal = atof(strVal);
        }
        else
        {
            return E_ATTRTYPEMISMATCH;
        }
    }
    else if (type == STRING)
    {
        strcpy(attrVal.sVal, strVal);
    }

    
    RelCacheTable::resetSearchIndex(srcRelId);


    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    
    printf("|");
    for (int i = 0; i < relCatEntry.numAttrs; ++i)
    {
        // get attrCatEntry at offset i using AttrCacheTable::getAttrCatEntry()
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);

        printf(" %s |", attrCatEntry.attrName);
    }
    printf("\n");

    while (true)
    {
        RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

        if (searchRes.block != -1 && searchRes.slot != -1)
        {
            
            RecBuffer blockBuffer (searchRes.block);
            
            HeadInfo blockHeader;
            blockBuffer.getHeader(&blockHeader);

            Attribute recordBuffer [blockHeader.numAttrs];
            blockBuffer.getRecord(recordBuffer, searchRes.slot);   


            printf("|");
            for (int i = 0; i < relCatEntry.numAttrs; ++i)
            {
                AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
                if (attrCatEntry.attrType == NUMBER)
                    printf(" %d |", (int)recordBuffer[i].nVal);
                else 
                    printf(" %s |", recordBuffer[i].sVal);

            }
            printf("\n");
        }
        else 
            break;
    }

    return SUCCESS;
}




int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){
   
    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
    return E_NOTPERMITTED;


    int relId = OpenRelTable::getRelId(relName);


    if (relId < 0 || relId >= MAX_OPEN) return E_RELNOTOPEN;

    // get the relation catalog entry from relation cache
    // (use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatBuffer;
    RelCacheTable::getRelCatEntry(relId, &relCatBuffer);


    if (relCatBuffer.numAttrs != nAttrs) return E_NATTRMISMATCH;

   
    Attribute recordValues[nAttrs];

    
    for (int attrIndex = 0; attrIndex < nAttrs; attrIndex++)
    {
        
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, attrIndex, &attrCatEntry);

        int type = attrCatEntry.attrType;
        if (type == NUMBER)
        {
            
            if (isNumber(record[attrIndex]))
            {
                /* convert the char array to numeral and store it
                   at recordValues[i].nVal using atof() */
                recordValues[attrIndex].nVal = atof (record[attrIndex]);
            }
            else
                return E_ATTRTYPEMISMATCH;
        }
        else if (type == STRING)
        {
            
            strcpy((char *) &(recordValues[attrIndex].sVal), record[attrIndex]);
        }
    }

    // insert the record by calling BlockAccess::insert() function
    // let retVal denote the return value of insert call
    int ret = BlockAccess::insert(relId, recordValues);

    return ret;
}