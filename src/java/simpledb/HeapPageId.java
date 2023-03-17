package simpledb;

import sun.security.jca.GetInstance;

import java.sql.Struct;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    private int tid;
    private int pno;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        tid=tableId;
        pno=pgNo;
        // some code goes here
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        return tid;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        // some code goes here
        return pno;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
        //BtreePageId÷– π”√ (tid << 16) + (pno << 2)
        return tid+pno*31;

        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        if(!(o instanceof PageId))
            return false;
        HeapPageId p = (HeapPageId) o;
        return tid == p.tid && pno == p.pno;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
