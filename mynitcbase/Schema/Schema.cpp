#include "Schema.h"

#include <cmath>
#include <cstring>

inline bool operator == (RecId lhs, RecId rhs) {
	return (lhs.block == rhs.block && lhs.slot == rhs.slot);
}

inline bool operator != (RecId lhs, RecId rhs) {
	return !(lhs == rhs);
}

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

	// this function returns the rel-id of a relation if it is open or
	// E_RELNOTOPEN if it is not. we will implement this later.
	int relId = OpenRelTable::getRelId(relName);

	if (relId == E_RELNOTOPEN)
		return E_RELNOTOPEN;

	return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]) {
    //! if the oldRelName or newRelName is either Relation Catalog or Attribute Catalog,
	if (strcmp(oldRelName, RELCAT_RELNAME) == 0 || strcmp(oldRelName, ATTRCAT_RELNAME) == 0)
		return E_NOTPERMITTED;

	if (strcmp(newRelName, RELCAT_RELNAME) == 0 || strcmp(newRelName, ATTRCAT_RELNAME) == 0)
		return E_NOTPERMITTED;

    //! if the relation is open
	int relId = OpenRelTable::getRelId(oldRelName);
	if (relId != E_RELNOTOPEN)
       return E_RELOPEN;

    // retVal = BlockAccess::renameRelation(oldRelName, newRelName);
    // return retVal
	return BlockAccess::renameRelation(oldRelName, newRelName);
}

int Schema::renameAttr(char *relName, char *oldAttrName, char *newAttrName) {
    //! if the relName is either Relation Catalog or Attribute Catalog,
	if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
		return E_NOTPERMITTED;

    //! if the relation is open
	int relId = OpenRelTable::getRelId(relName);
	if (relId != E_RELNOTOPEN)
           return E_RELOPEN;
        

    // Call BlockAccess::renameAttribute with appropriate arguments.
    // return the value returned by the above renameAttribute() call
	return BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);
}

int Schema::createRel(char relName[],int nAttrs, char attrs[][ATTR_SIZE],int attrtype[]){
    // declare relNameAsAttribute of type Attribute and set its sVal field to relName
	Attribute relNameAsAttribute;
	strcpy((char *)relNameAsAttribute.sVal,(const char*)relName);

	RecId targetRelId;

    // Reset the searchIndex using RelCacheTable::resetSearhIndex()
	RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // search for a relation with name relName 
	targetRelId = BlockAccess::linearSearch(RELCAT_RELID, "RelName", relNameAsAttribute, EQ);

    // aldready exists
	if (targetRelId != RecId{-1, -1})
        return E_RELEXIST;

    // check if any attributes are repeated
	for (int i = 0; i < nAttrs-1; i++) 
		for (int j = i+1; j < nAttrs; j++) 
			if (strcmp(attrs[i], attrs[j]) == 0) return E_DUPLICATEATTR;

   
    Attribute relCatRecord[RELCAT_NO_ATTRS];

    // fill the relCatRecord fields as given below
	strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);// name of the relation
    relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs; // number of attributes
    relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0; // number of records 
    relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1; // first block
    relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1; // last block
    relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor((2016*1.00) / (16*nAttrs + 1)); // number of slots per block

    // Call BlockAccess::insert() to insert the above record in the relation catalog.
	int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);
	if (retVal != SUCCESS) return retVal;

    // set the attr catalog entries for the relation 
	for (int attrIndex = 0; attrIndex < nAttrs; attrIndex++)
    {
		Attribute attrCatRecord [ATTRCAT_NO_ATTRS];
		
        // fill the attrCatRecord fields as given below
		strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);// name of the relation
        strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[attrIndex]);// name of the attribute
        attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrtype[attrIndex];// type of the attribute
        attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;// primary flag 
        attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;// root block
		attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = attrIndex;// offset of the attribute in the record

        // retVal = BlockAccess::insert to insert the above record in the attribute catalog.
		retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);

	   	if (retVal != SUCCESS) {
			Schema::deleteRel(relName);
			return E_DISKFULL;
	   	}
    }

    return SUCCESS;
}

int Schema::deleteRel(char *relName) {
    // if the relation to delete is either Relation Catalog or Attribute Catalog
	if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
        return E_NOTPERMITTED;

    // get the rel-id 
	int relId = OpenRelTable::getRelId(relName);

    // if relation is opened in open relation table
	if (relId >= 0 && relId < MAX_OPEN) return E_RELOPEN;

    // Call BlockAccess::deleteRelation()
	int retVal = BlockAccess::deleteRelation(relName);

	return retVal;
}
