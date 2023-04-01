package simpledb;

import com.sun.deploy.panel.ITreeNode;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f=f;
        this.td=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        long offset = pid.getPageNumber() * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile rFile = new RandomAccessFile(f, "r");
            rFile.seek(offset);
            for (int i = 0; i < BufferPool.getPageSize(); i++) {
                data[i] = (byte) rFile.read();
            }
            HeapPage page = new HeapPage((HeapPageId) pid, data);
            rFile.close();
            return page;
        } catch (Exception e) {
            return null;
        }
    }


    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }

    //为什么BtreeFile里面的iterator用的是extends，而有的地方是implements
    //因为DbFileIterator是一个Interface
    class HeapFileIterator implements DbFileIterator {
        private HeapPage pg;

        private Iterator<Tuple> it = null;
        private int pgNo;//与pg重复了
        private TransactionId tid;
        private HeapFile f;

        public HeapFileIterator(HeapFile f, TransactionId tid) {
            this.f = f;
            this.tid = tid;
        }

        /**
         * Open this iterator by getting an iterator on the first page
         */
        public void open() throws DbException, TransactionAbortedException {
            pgNo=0;
            HeapPageId pid=new HeapPageId(getId(),0);
            pg=(HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
            it = pg.iterator();
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException, DbException {
            if(it==null)
                return false;
            if(it.hasNext())
                return true;
            //为了使调用hasNext不影响当前状态，所有变量都copy一份
            int npgNo=pgNo;
            while(npgNo<numPages()-1)//下一个page存在的情况下，直到找到不为空的slot
            {
                try{//为什么要try？？
                    HeapPageId pid=new HeapPageId(getId(),++npgNo);
                    HeapPage npg=(HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
                    Iterator<Tuple> nit = npg.iterator();
                    if(nit.hasNext())
                        return true;
                }
                catch (Exception e){
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
            if(!this.hasNext())
                throw new NoSuchElementException();
            if(it.hasNext())
                return it.next();
            while(pgNo<numPages()-1)//下一个page存在的情况下，直到找到不为空的slot
            {
                HeapPageId pid=new HeapPageId(getId(),++pgNo);
                pg=(HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
                it = pg.iterator();
                if(it.hasNext())
                    return it.next();
            }
               return null;
        }

        /**
         * rewind this iterator back to the beginning of the tuples
         */
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * close the iterator
         */
        public void close() {
            pg=null;
            pgNo=0;
            it = null;
            f=null;
            tid=null;

        }
    }



}

