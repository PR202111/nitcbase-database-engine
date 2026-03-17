#include "BlockAccess.h"

#include <cstring>
#include "BlockAccess.h"

#include <cstring>

inline bool operator == (RecId lhs, RecId rhs) {
	return (lhs.block == rhs.block && lhs.slot == rhs.slot);
}

inline bool operator != (RecId lhs, RecId rhs) {
	return (lhs.block != rhs.block || lhs.slot != rhs.slot);
}

// performs a linear search on the relation with Id relId
RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op)
{
	// get the previous search index of the relation relId from the relation cache
	RecId prevRecId;
	RelCacheTable::getSearchIndex(relId, &prevRecId);

	// let block and slot denote the record id of the record being currently checked
	int block = -1, slot = -1;

	// if the current search index record is both block and slot = -1
	if (prevRecId.block == -1 && prevRecId.slot == -1)
	{
		RelCatEntry relCatBuffer;
		RelCacheTable::getRelCatEntry(relId, &relCatBuffer);

		// set block and slot to the first block and slot 0
		block = relCatBuffer.firstBlk, slot = 0;
	}
	else
	{
		// set the block to be same but increment slot by 1
		block = prevRecId.block, slot = prevRecId.slot + 1;
	}


	RelCatEntry relCatBuffer;
	RelCacheTable::getRelCatEntry(relId, &relCatBuffer);

	while (block != -1)
	{
		RecBuffer blockBuffer(block);

		HeadInfo blockHeader;
		blockBuffer.getHeader(&blockHeader);

		unsigned char slotMap[blockHeader.numSlots];
		blockBuffer.getSlotMap(slotMap);

		// check slot in range or not
		if (slot >= relCatBuffer.numSlotsPerBlk)
		{
			block = blockHeader.rblock, slot = 0;
			continue; 
		}

		if (slotMap[slot] == SLOT_UNOCCUPIED)
		{
			slot++;
			continue;
		}

		Attribute record[blockHeader.numAttrs];
		blockBuffer.getRecord(record, slot);

		AttrCatEntry attrCatBuffer;
		AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuffer);

		int attrOffset = attrCatBuffer.offset;

		int cmpVal = compareAttrs(record[attrOffset], attrVal, attrCatBuffer.attrType); 

		if (
			(op == NE && cmpVal != 0) || 
			(op == LT && cmpVal < 0) ||	 
			(op == LE && cmpVal <= 0) || 
			(op == EQ && cmpVal == 0) || 
			(op == GT && cmpVal > 0) ||	 
			(op == GE && cmpVal >= 0)	
		)
		{
			
			RecId newRecId = {block, slot};
			RelCacheTable::setSearchIndex(relId, &newRecId);

			return RecId{block, slot};
		}

		slot++;
	}

	return RecId{-1, -1};
}

// 1. reset the searchIndex of the relation catalog
// 2. search the relation catalog for an entry with relName = newName
// 3. if found return E_RELEXIST
// 4. if not found, search the relation catalog for an entry with relName = oldName
// 5. update the relName field to newName
// 6. similarly, search the attribute catalog for all entries with relName = oldName
// 7. update the relName field to newName
int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
	RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName;    
	strcpy(newRelationName.sVal, newName);

	// linear search on the relation catalog for an entry with "RelName" = newRelationName
	RecId searchIndex = BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, newRelationName, EQ);

    // If relation with name newName already exists 
	if (searchIndex != RecId{-1, -1})
       return E_RELEXIST;

    // reset the searchIndex of the relation catalog 
	RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName;
	strcpy(oldRelationName.sVal, oldName);

    // search the relation catalog for an entry with "RelName" = oldRelationName
	searchIndex = BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, oldRelationName, EQ);

    //! If relation with name oldName does not exist 
	if (searchIndex == RecId{-1, -1})
       return E_RELNOTEXIST;

	RecBuffer relCatBlock (RELCAT_BLOCK);
	
	Attribute relCatRecord [RELCAT_NO_ATTRS];
	relCatBlock.getRecord(relCatRecord, searchIndex.slot);

	// change the relName field of the record to newName
	strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, newName);

    // set back the record value 
	relCatBlock.setRecord(relCatRecord, searchIndex.slot);

    // reset the searchIndex 
	RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    
	for (int attrIndex = 0; attrIndex < relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal; attrIndex++) {
		//    linearSearch on the attribute catalog for relName = oldRelationName
		searchIndex = BlockAccess::linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);

		RecBuffer attrCatBlock (searchIndex.block);

		Attribute attrCatRecord [ATTRCAT_NO_ATTRS];
		attrCatBlock.getRecord(attrCatRecord, searchIndex.slot);

		// update the relName field of the attribute catalog record to newName
		strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, newName);

		// set back the record value
		attrCatBlock.setRecord(attrCatRecord, searchIndex.slot);
	}

    return SUCCESS;
}

// 1. reset the searchIndex of the relation catalog
// 2. search the relation catalog for an entry with relName = relName
// 3. if not found return E_RELNOTEXIST
// 4. reset the searchIndex of the attribute catalog
// 5. search the attribute catalog for an entry with relName = relName and attrName = newName
// 6. if found return E_ATTREXIST
// 7. search the attribute catalog for an entry with relName = relName and attrName = oldName
// 8. if not found return E_ATTRNOTEXIST
// 9. update the attrName field to newName
int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {
    // reset the searchIndex 
	RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
	strcpy(relNameAttr.sVal, relName);

	// Search for the relation with name relName
	RecId searchIndex = BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameAttr, EQ);
    
	//! If relation with name relName does not exist 
	if (searchIndex == RecId{-1, -1})
       return E_RELNOTEXIST;
	
    // reset the searchIndex of the attribute catalog 
	RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    RecId attrToRenameRecId{-1, -1};

    
    while (true) {
        // linear search on the attribute catalog for RelName = relNameAttr
		searchIndex = BlockAccess::linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);

		if (searchIndex == RecId{-1, -1}) break;

		RecBuffer attrCatBlock (searchIndex.block);

		Attribute attrCatRecord [ATTRCAT_NO_ATTRS];
		attrCatBlock.getRecord(attrCatRecord, searchIndex.slot);

        // if attrCatEntryRecord.attrName = oldName
		if (strcmp(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0){
			attrToRenameRecId = searchIndex;
			break;
		}

        //! if attrCatEntryRecord.attrName = newName
		if (strcmp(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0)
            return E_ATTREXIST;
    }

	// if attribute with the old name does not exist
    if (attrToRenameRecId == RecId{-1, -1})
        return E_ATTRNOTEXIST;

    //   update the AttrName of the record with newName

	RecBuffer attrCatBlock (attrToRenameRecId.block);
	Attribute attrCatRecord [ATTRCAT_NO_ATTRS];
	attrCatBlock.getRecord(attrCatRecord, attrToRenameRecId.slot);
	
	 //   set back the record with RecBuffer.setRecord
	strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName );
	attrCatBlock.setRecord(attrCatRecord, attrToRenameRecId.slot);

    return SUCCESS;
}



int BlockAccess::insert(int relId, Attribute *record) {
    
	RelCatEntry relCatEntry;
	RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int blockNum = relCatEntry.firstBlk;

    RecId rec_id = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk;
    int numOfAttributes = relCatEntry.numAttrs;
 
    int prevBlockNum = -1;


    while (blockNum != -1) {
		RecBuffer blockBuffer (blockNum);

        // get header of block(blockNum)
		HeadInfo blockHeader;
		blockBuffer.getHeader(&blockHeader);

        // get slot map of block(blockNum)
		int numSlots = blockHeader.numSlots;
		unsigned char slotMap [numSlots];
		blockBuffer.getSlotMap(slotMap);

        // search for free slot in the block 'blockNum' and store it's rec-id in rec_id
		int slotIndex = 0;
		for (; slotIndex < numSlots; slotIndex++) {
			if (slotMap[slotIndex] == SLOT_UNOCCUPIED) {
				rec_id = RecId{blockNum, slotIndex};
				break;
			}
		}

		if (rec_id != RecId{-1, -1}) break;

	   prevBlockNum = blockNum;
	   blockNum = blockHeader.rblock;
    }

    //  if no free slot is found 
	if (rec_id == RecId{-1, -1})
    {
        // if relation is RELCAT, do not allocate any more blocks
		if (relId == RELCAT_RELID) return E_MAXRELATIONS;

		RecBuffer blockBuffer;

        // get the block number 
        blockNum = blockBuffer.getBlockNum();
        if (blockNum == E_DISKFULL) return E_DISKFULL;

        // Assign rec_id.block = new block number and rec_id.slot = 0
		rec_id = RecId {blockNum, 0};

		// TODO: set the header of the new record block         
		HeadInfo blockHeader;
		blockHeader.blockType = REC;
		blockHeader.lblock = prevBlockNum, blockHeader.rblock = blockHeader.pblock = -1;
		blockHeader.numAttrs = numOfAttributes, blockHeader.numSlots = numOfSlots, blockHeader.numEntries = 0;

		// set the header of the new block 
		blockBuffer.setHeader(&blockHeader);

		// initialize the slot map 
	   	unsigned char slotMap [numOfSlots];
		for (int slotIndex = 0; slotIndex < numOfSlots; slotIndex++)
			slotMap[slotIndex] = SLOT_UNOCCUPIED;

		// set the slot map of the new block
		blockBuffer.setSlotMap(slotMap);

        // if prevBlockNum != -1
		if (prevBlockNum != -1)
        {
			RecBuffer prevBlockBuffer (prevBlockNum);

			HeadInfo prevBlockHeader;
			prevBlockBuffer.getHeader(&prevBlockHeader);

			// update the right block field of the header of the previous block to the new block
			prevBlockHeader.rblock = blockNum;
            
			// set the header of the previous block
			prevBlockBuffer.setHeader(&prevBlockHeader);
        }
        else
        {
            // set the first block field of the relation catalog entry to the new block 
			relCatEntry.firstBlk = blockNum;
			// set the relation catalog entry 
			RelCacheTable::setRelCatEntry(relId, &relCatEntry);
        }

        // update last block field in the relation catalog 
		relCatEntry.lastBlk = blockNum;
		RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }

    RecBuffer blockBuffer (rec_id.block);

	// insert the record into rec_id'th slot
	blockBuffer.setRecord(record, rec_id.slot);

	unsigned char slotmap [numOfSlots];
	blockBuffer.getSlotMap(slotmap);

	// set the slotmap
	slotmap[rec_id.slot] = SLOT_OCCUPIED;
	blockBuffer.setSlotMap(slotmap);

	HeadInfo blockHeader;
	blockBuffer.getHeader(&blockHeader);

	// increment the number of entries in the block header by 1
	blockHeader.numEntries++;
	blockBuffer.setHeader(&blockHeader);

    // Increment the number of records field in the relation cache entry 
	relCatEntry.numRecs++;
	RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    return SUCCESS;
}

// search and return the record id of the first record in the relation which is true.
int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    RecId recId;

    // linear search on the relation with relId for a record with attrName op attrVal
	recId = BlockAccess::linearSearch(relId, attrName, attrVal, op);

	if (recId == RecId{-1, -1})
       return E_NOTFOUND;

   	RecBuffer blockBuffer (recId.block);
   	blockBuffer.getRecord(record, recId.slot);

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    // if the relation to delete is either Relation Catalog or Attribute Catalog
	if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
        return E_NOTPERMITTED;

    // reset the searchIndex 
	RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // assign relNameAttr.sVal = relName
    Attribute relNameAttr; 
	strcpy((char*)relNameAttr.sVal,(const char*)relName);

    //  linearSearch on the relation catalog for RelName = relNameAttr
	RecId relCatRecId = BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameAttr ,EQ);
	if (relCatRecId == RecId{-1, -1}) return E_RELNOTEXIST;

	RecBuffer relCatBlockBuffer (relCatRecId.block);
    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
	relCatBlockBuffer.getRecord(relCatEntryRecord, relCatRecId.slot);
	int firstBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
	int numAttributes = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
	int currentBlockNum = firstBlock;
    
	// deleting the record blocks of the relation 
	// for each record block of the relation release it 
	while (currentBlockNum != -1) {
		RecBuffer currentBlockBuffer (currentBlockNum);

    	// get block header using BlockBuffer.getHeader
		HeadInfo currentBlockHeader;
		currentBlockBuffer.getHeader(&currentBlockHeader);

		// get the next block from the header (rblock)
		currentBlockNum = currentBlockHeader.rblock;

		// release the block using BlockBuffer.releaseBlock
		currentBlockBuffer.releaseBlock();
	}

    

	// deleting the tuples in attribute catalog 
    // reset the searchIndex of the attribute catalog
	RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    int numberOfAttributesDeleted = 0;

    while(true) {
        RecId attrCatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, RELCAT_ATTR_RELNAME, relNameAttr, EQ);
		if (attrCatRecId == RecId{-1, -1}) break;
        numberOfAttributesDeleted++;

		RecBuffer attrCatBlockBuffer (attrCatRecId.block);
		HeadInfo attrCatHeader;
		attrCatBlockBuffer.getHeader(&attrCatHeader);
		Attribute attrCatRecord [ATTRCAT_NO_ATTRS];
		attrCatBlockBuffer.getRecord(attrCatRecord, attrCatRecId.slot);

        int rootBlock = attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal; 
        // (This will be used later to delete any indexes if it exists)
		
		// update the slot map 
		unsigned char slotmap [attrCatHeader.numSlots];
		attrCatBlockBuffer.getSlotMap(slotmap);
		slotmap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
		attrCatBlockBuffer.setSlotMap(slotmap);

        // Decrement the numEntries in the header of the block
		attrCatHeader.numEntries--;
		attrCatBlockBuffer.setHeader(&attrCatHeader);

        
        if (attrCatHeader.numEntries == 0) {
			// ie current block is empty so set the left one to the right one 
			RecBuffer prevBlock (attrCatHeader.lblock);
			
			HeadInfo leftHeader;
			prevBlock.getHeader(&leftHeader);

			leftHeader.rblock = attrCatHeader.rblock;
			prevBlock.setHeader(&leftHeader);

			//and set the right one to the left one and release the current block
            if (attrCatHeader.rblock != INVALID_BLOCKNUM) 
			{
				RecBuffer nextBlock(attrCatHeader.rblock);
				
				HeadInfo rightHeader;
				nextBlock.getHeader(&rightHeader);

				rightHeader.lblock = attrCatHeader.lblock;
				nextBlock.setHeader(&rightHeader);

            } 
			else 
			{// update it as the last block
				RelCatEntry relCatEntryBuffer;
				RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntryBuffer);
				relCatEntryBuffer.lastBlk = attrCatHeader.lblock;
				RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntryBuffer);
            }

			attrCatBlockBuffer.releaseBlock();
        }

    }

	// updating the catalogs after deletion of the relation
	// update numEnteries in the header
	HeadInfo relCatHeader;
	relCatBlockBuffer.getHeader(&relCatHeader);
	relCatHeader.numEntries--;
	relCatBlockBuffer.setHeader(&relCatHeader);

    // update the slotmap as free
	unsigned char slotmap [relCatHeader.numSlots];
	relCatBlockBuffer.getSlotMap(slotmap);
	slotmap[relCatRecId.slot] = SLOT_UNOCCUPIED;
	relCatBlockBuffer.setSlotMap(slotmap);

    // decrement the number of records field in the relation catalog entry by 1
	RelCatEntry relCatEntryBuffer;
	RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntryBuffer);
	relCatEntryBuffer.numRecs--;
	RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntryBuffer);

    // decrement the number of records field in the relation catalog entry by numberOfAttributesDeleted	
	RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntryBuffer);
	relCatEntryBuffer.numRecs -= numberOfAttributesDeleted;
	RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntryBuffer);


    return SUCCESS;
}

/*
NOTE: the caller is expected to allocate space for the argument `record` based
      on the size of the relation. This function will only copy the result of
      the projection onto the array pointed to by the argument.
*/
int BlockAccess::project(int relId, Attribute *record) {

    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    int block, slot;

    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        RelCatEntry relEntry;
        RelCacheTable::getRelCatEntry(relId, &relEntry);

        block = relEntry.firstBlk;
        slot = 0;
    }
    else
    {
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    while (block != -1)
    {
        RecBuffer recbuffer(block);

        HeadInfo header;
        recbuffer.getHeader(&header);

        unsigned char slotmap[header.numSlots];
        recbuffer.getSlotMap(slotmap);

        if (slot >= header.numSlots)
        {
            block = header.rblock;
            slot = 0;
        }
        else if (slotmap[slot] == SLOT_UNOCCUPIED)
        {
            slot++;
        }
        else
        {
            break;
        }
    }

    if (block == -1)
    {
        return E_NOTFOUND;
    }

    RecId nextRecId{block, slot};
    RelCacheTable::setSearchIndex(relId, &nextRecId);

    RecBuffer recBuffer(block);
    recBuffer.getRecord(record, slot);

    return SUCCESS;
}