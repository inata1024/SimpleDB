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
    int numPage;
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
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
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
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
            byte[] data = page.getPageData();
            raf.write(data);
        }
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
        //ͨ������pid����BufferPool����page
        //�ɻ��޸�BufferPool��������Page���᲻��ֻ�޸���һ����������
        //ȷʵ����Ϊ����һ��transaction�ڣ��������������ļ�
        ArrayList<Page> ans=new ArrayList<>();
        int pos=0;
        BufferPool bp=Database.getBufferPool();
        HeapPage currPg=(HeapPage) bp.getPage(tid,new HeapPageId(getId(),pos++),Permissions.READ_WRITE);

        while(pos<numPages()&&currPg.getNumEmptySlots()==0)
            currPg=(HeapPage) bp.getPage(tid,new HeapPageId(getId(),pos++),Permissions.READ_WRITE);
        //������ɹ�
        //ϸ�ڣ�������ж�����������pos<numPages(),��Ϊ���һ��Page����ɹ���ʧ�ܶ���pos==numPages()
        if(currPg.getNumEmptySlots()!=0)
        {
            currPg.insertTuple(t);
            currPg.markDirty(true,tid);
            ans.add(currPg);
        }
        //�������ļ���д��һҳ
        else
        {
           int newPos=numPages();
           HeapPageId newPID=new HeapPageId(getId(),newPos);
           HeapPage newPg=new HeapPage(newPID,HeapPage.createEmptyPageData());
           newPg.insertTuple(t);
           newPg.markDirty(true,tid);
           ans.add(newPg);
           writePage(newPg);//�������֮����write
        }
        return ans;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> ans=new ArrayList<>();
        BufferPool bp=Database.getBufferPool();
        RecordId rid=t.getRecordId();
        HeapPageId pgID=(HeapPageId) rid.getPageId();
        if(pgID.getTableId()!=getId())
            throw new DbException("is not a member of the file");
        int pos=rid.getTupleNumber();
        HeapPage currPage=(HeapPage) bp.getPage(tid,pgID,Permissions.READ_WRITE);
        currPage.deleteTuple(t);
        currPage.markDirty(true,tid);
        ans.add(currPage);
        return ans;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }

    //ΪʲôBtreeFile�����iterator�õ���extends�����еĵط���implements
    //��ΪDbFileIterator��һ��Interface
    class HeapFileIterator implements DbFileIterator {
        private HeapPage pg;

        private Iterator<Tuple> it = null;
        private int pgNo;//��pg�ظ���
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
            //Ϊ��ʹ����hasNext��Ӱ�쵱ǰ״̬�����б�����copyһ��
            int npgNo=pgNo;
            while(npgNo<numPages()-1)//��һ��page���ڵ�����£�ֱ���ҵ���Ϊ�յ�slot
            {
                try{//ΪʲôҪtry����
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
            while(pgNo<numPages()-1)//��һ��page���ڵ�����£�ֱ���ҵ���Ϊ�յ�slot
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

