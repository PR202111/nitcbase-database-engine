#include "BPlusTree.h"

#include <cstring>

using namespace std;

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
    IndexId searchIndex;

    // Get previous search index
    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    // Reset search index (IMPORTANT)
    IndexId resetIdx = {-1, -1};
    AttrCacheTable::setSearchIndex(relId, attrName, &resetIdx);

    AttrCatEntry attrcatentry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcatentry);
    if (ret != SUCCESS)
        return RecId{-1, -1};

    int block, index;

    // FIRST CALL vs CONTINUATION
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
        HeadInfo leafHead;
        leaf.getHeader(&leafHead);

        if (index >= leafHead.numEntries)
        {
            block = leafHead.rblock;
            index = 0;

            if (block == -1)
                return RecId{-1, -1};
        }
    }

    /************ INTERNAL NODE TRAVERSAL ************/
    while (StaticBuffer::getStaticBlockType(block) == IND_INTERNAL)
    {
        IndInternal internalBlk(block);
        HeadInfo intHead;
        internalBlk.getHeader(&intHead);

        InternalEntry intEntry;

        if (op == NE || op == LT || op == LE)
        {
            // Always go to leftmost child
            internalBlk.getEntry(&intEntry, 0);
            block = intEntry.lChild;
        }
        else
        {
            bool found = false;

            for (int i = 0; i < intHead.numEntries; i++)
            {
                internalBlk.getEntry(&intEntry, i);

                int cmp = compareAttrs(intEntry.attrVal, attrVal, attrcatentry.attrType);

                if ((op == EQ || op == GE) && cmp >= 0)
                {
                    block = intEntry.lChild;
                    found = true;
                    break;
                }

                if (op == GT && cmp > 0)
                {
                    block = intEntry.lChild;
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                // Move to rightmost child (FIXED)
                block = intEntry.rChild;
            }
        }
    }

    /************ LEAF NODE SCAN ************/
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
                (op == GE && cmp >= 0) ||
                (op == GT && cmp > 0) ||
                (op == NE && cmp != 0))
            {
                // Update search index
                IndexId newIdx = {block, index};
                AttrCacheTable::setSearchIndex(relId, attrName, &newIdx);

                return RecId{leafEntry.block, leafEntry.slot};
            }
            else if ((op == EQ || op == LE || op == LT) && cmp > 0)
            {
                // No more possible matches
                return RecId{-1, -1};
            }

            index++;
        }

        if (op != NE)
            break;

        // Move to next leaf block
        block = leafHead.rblock;
        index = 0;
    }

    return RecId{-1, -1};
}