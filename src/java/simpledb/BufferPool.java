package simpledb;

import java.io.*;

import java.security.cert.TrustAnchor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountedCompleter;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private ConcurrentHashMap<PageId,Page> id2pg;
    private ConcurrentHashMap<PageId, Date> id2date;//��¼bp��page�ķ���ʱ��

    /**
     * ͨ��PageIdȷ��Page�ϵ�����1:shared�� 0:exclusive�� û���key:û��
     * ��������page��BufferPool��Ӧͬʱ����id2lock
     */
    private ConcurrentHashMap<PageId,Integer> pid2lock;
    /**
     * ͨ��TransactionIdȷ��һ��Transaction��������
     */

    private ConcurrentHashMap<TransactionId,HashSet<PageId>> tid2lock;
    //���ڼ��deadlock�ĵȴ�������
    private ConcurrentHashMap<TransactionId,PageId> want;

    private int maxPagenum;
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    private int Policy=3;//1��LRU 2��MRU 3��������� Ĭ��ΪLRU
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    //�����ȡ������ʱ��Ҫ�ȴ�
    private final long SLEEP_INTERVAL;
    private final long DeadLockDetectCount;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        id2pg=new ConcurrentHashMap<PageId,Page>();
        pid2lock=new ConcurrentHashMap<PageId,Integer>();
        tid2lock=new ConcurrentHashMap<>();
        id2date=new ConcurrentHashMap<>();
        want=new ConcurrentHashMap<>();
        maxPagenum=numPages;
        SLEEP_INTERVAL = 1;
        DeadLockDetectCount=25;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }
    /**
     * �ж�tid�Ƿ������pid����
     */
    public boolean ownLock(TransactionId tid,PageId pid)
    {
        //�����ǰtid��hashset��û��ʼ��
        if(!tid2lock.containsKey(tid))
        {
            HashSet<PageId> hs=new HashSet<>();
            tid2lock.put(tid,hs);
            return false;
        }
        HashSet<PageId> hs=tid2lock.get(tid);
        boolean a=hs.contains(pid);
        return a;
    }

    /**
     * ������������������ͬ��
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException, InterruptedException {
        // some code goes here
        /**
         * ����:
         * һ��transactionҪ��page��������shared��
         * һ��transactionҪдpage��������exclusive��
         * ���һ��transaction��һ��page��shared����Ψһӵ���ߣ����Խ���������exclusive
         */

        int needLock=-1;//�ж��Ƿ���Ҫ�����ı�־��Ĭ��-1Ϊ����Ҫ��0Ϊex����1Ϊshared��
        boolean lockAllocated=false;//�Ƿ����뵽��
        //�� ���� ��ӵ���� ��Ҫ��һ���ж�
        if(perm==Permissions.READ_ONLY&&!ownLock(tid,pid))
        {
            //���page������
            if(pid2lock.containsKey(pid))
            {
                //���page����shared��
                if(pid2lock.get(pid)>0)
                    needLock=1;//��shared��
                //���page����exclusive��
                else
                {
                    int count=0;
                    //����ֱ��page��û��,��lockAllocated������������
                    while(!lockAllocated)
                    {
                        Thread.sleep(SLEEP_INTERVAL);
                        //��ǰ�̳߳��Ի�ȡ��,���������double checking��������
                        //�Ϳ��ĸ��߳����ÿ�
                        synchronized (this){
//                            //��ѭ����������ĳֵ���ж���deadlock
//                            if(count>DeadLockDetectCount)
//                            {
//                                throw new TransactionAbortedException();
//                            }
                            //����page���ѽ��
                            if(!pid2lock.containsKey(pid))
                            {
                                lockAllocated=true;//��ǰ�̻߳����
                                pid2lock.put(pid,-1);//����-1ռλ����ʾpage��û��
                            }
                            else
                            {
                                want.put(tid,pid);
                                if(count>DeadLockDetectCount)
                                //����һ���ڵȴ�ĳ��tid����,�����tidҲ�ڵȱ��ˣ����ж�������.�˴����tid��Ψһ��
                                //���ܵ���Э��ֻ���ֱ�ӵȴ���
                                {
                                    for(TransactionId temp :tid2lock.keySet())
                                        if(temp!=tid&&tid2lock.get(temp).contains(pid)&&want.containsKey(temp)) {
                                            System.out.println("kill from 1");
                                            throw new TransactionAbortedException();
                                        }
                                }
                            }
                            count++;
                        }
                    }
                    needLock=1;//��shared��
                }
            }
            //���page��û��
            else
            {
                pid2lock.put(pid,-1);//����-1ռλ����ʾpage��û��
                needLock=1;//��shared��
            }
        }
        int debug=0;
        if(perm==Permissions.READ_ONLY&&!pid2lock.containsKey(pid))
            debug=1;

        if(perm==Permissions.READ_WRITE)
        {
            //tidӵ�ж�page�����������shared����Ҫ��һ���жϣ������ex��ֱ��getpage
            if(ownLock(tid,pid)&&pid2lock.get(pid)>0)
            {
                int count=0;
                //�����Ψһӵ�У�����ֱ��Ψһӵ��
                //�����ж���̵߳���Ψһӵ�У���������
                while(!lockAllocated)
                {
                    Thread.sleep(SLEEP_INTERVAL);
                    synchronized (this){
//                        //��ѭ����������ĳֵ���ж���deadlock
//                        if(count>DeadLockDetectCount)
//                        {
//                            throw new TransactionAbortedException();
//                        }
                        if(!pid2lock.containsKey(pid))
                            debug=1;
                        if(pid2lock.get(pid)==1)
                        {
                            pid2lock.put(pid,0);//��һ����̫��Ҫ����Ϊ������߳������ᵼ������
                            lockAllocated=true;
                        }
                        else {
                            want.put(tid,pid);//�����ȴ�����
                            if(count>DeadLockDetectCount)//���ѭ�����ټ��
                            {
                                for(TransactionId id:want.keySet())
                                {
                                    //��idӵ�ж�pid����������ϣ���õ���pid������˵����������
                                    //idӦ�����ڵ�ǰtid����������
                                    if(id!=tid&&tid2lock.get(id).contains(pid)&&want.get(id).equals(pid))
                                    {
                                        System.out.println("kill from 2");
                                        throw new TransactionAbortedException();
                                    }

                                }
                            }
                        }
                        count++;
                    }
                }
                needLock=0;//��ex��
            }
            //tidû�ж�page����
            if(!ownLock(tid,pid))
            {
                int count=0;
                //���page������������ֱ��û����û���Ͳ�������
                while(!lockAllocated)
                {
                    Thread.sleep(SLEEP_INTERVAL);
                    synchronized (this){
//                        //��ѭ����������ĳֵ���ж���deadlock
//                        if(count>DeadLockDetectCount)
//                        {
//                            throw new TransactionAbortedException();
//                        }
                        if(!pid2lock.containsKey(pid))
                        {
                            lockAllocated=true;
                            pid2lock.put(pid,0);//�޸Ĺ��������ȷ��ֻ��һ���̻߳����
                        }
                        else
                        {
                            want.put(tid,pid);
                            if(count>DeadLockDetectCount)
                            //����һ���ڵȴ�ĳ��tid����,�����tidҲ�ڵȱ��ˣ����ж�������.�˴����tid��Ψһ��
                            //���ܵ���Э��ֻ���ֱ�ӵȴ���
                            {
                                for(TransactionId temp :tid2lock.keySet())
                                    if(temp!=tid&&tid2lock.get(temp).contains(pid)&&want.containsKey(temp)) {
                                        System.out.println("kill from 3");
                                        throw new TransactionAbortedException();
                                    }
                            }
                        }
                        count++;
                    }
                }
                needLock=0;//��ex��
            }
        }
        synchronized (this){
            //��shared��
            if(needLock==1)
            {
                //������shared��,����1��ʾ����һ��shared��
                if(pid2lock.get(pid)>0)//����Ϊʲô�����NullPointer?
                    pid2lock.put(pid,pid2lock.get(pid)+1);
                else
                    pid2lock.put(pid, 1);
            }
            //��ex��
            if(needLock==0)
            {
                pid2lock.put(pid,0);
            }
            HashSet<PageId> temp=tid2lock.get(tid);
            temp.add(pid);
            tid2lock.put(tid,temp);
        }

        //getPage
        if(id2pg.containsKey(pid))
        {
            id2date.put(pid,new Date());
            return id2pg.get(pid);
        }
        else
        {
            if(id2pg.size()==maxPagenum)
            {
                evictPage();
            }
            DbFile table = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page newPage = table.readPage(pid);
            id2pg.put(pid, newPage);
            id2date.put(pid,new Date());//��¼����ǰҳ��ʱ��
            return newPage;
        }
    }

    public boolean check(){
        for(TransactionId tid:tid2lock.keySet()){
            HashSet<PageId> hs = tid2lock.get(tid);
            for(PageId id:hs) {
                if(!pid2lock.containsKey(id))
                    return true;
            }
        }
        return false;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        //�����ex����ֱ��remove�������shared�����÷����

        //��ȡtransactionӵ�е���
        HashSet<PageId> t_locks=tid2lock.get(tid);
        Integer lockType=pid2lock.get(pid);

        int debug=0;
        if(!pid2lock.containsKey(pid))//???
        {
            //tid��ӵ�жԴ�page����
            t_locks.remove(pid);
            //����tid��������
            tid2lock.put(tid,t_locks);
            return ;//��֪��Ϊʲô������������
            //debug=1;//������
        }

        //�����shared��
        if(lockType>0)
        {
            //���Ψһӵ�У�ֱ��remove
            if(lockType==1)
                pid2lock.remove(pid);
            //��Ψһӵ�У����Լ���һ�ݼ�ȥ
            else
                pid2lock.put(pid,lockType-1);
        }
        //�����ex����remove
        else
            pid2lock.remove(pid);
        //tid��ӵ�жԴ�page����
        t_locks.remove(pid);
        //����tid��������
        tid2lock.put(tid,t_locks);

        if(check())
            debug=1;//������
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return ownLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        //tid��ӵ���κ������������Ϊû���κ�ҳ�棬ֱ��return
        if(!tid2lock.containsKey(tid))
        {
            return;
        }

        HashSet<PageId> t_locks= (HashSet<PageId>) tid2lock.get(tid).clone();
        if(commit)
            flushPages(tid);
        else
        {
            //�����и��ĸ�ԭ
            //��transactionӵ�е�����page��
            for (PageId pid : t_locks)
            {
                //�ҳ��Ĺ���
                //transactionӵ�еģ���һ����bp��
                if(id2pg.containsKey(pid)&&id2pg.get(pid).isDirty()!=null)
                {
                    //��diskȡ����
                    DbFile table = Database.getCatalog().getDatabaseFile(pid.getTableId());
                    Page newPage = table.readPage(pid);
                    //����bp�е�page
                    id2pg.put(pid,newPage);
                }

            }
        }
        //����
        //����������ConcurrentModificationException�������Ϸ��ֿ�������Ϊһ�߱���t_locks��releasePage���ٸ�t_locks
        //t_locks��ͨ��get�����õ��ģ�ֻ����һ�����ã���get���Ķ������clone���쳣��ʧ
        for (PageId tLock : t_locks) releasePage(tid, tLock);

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException, InterruptedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pgs=new ArrayList<>();
        DbFile table = Database.getCatalog().getDatabaseFile(tableId);
        //�ɻ�ΪʲôҪ��dirty page�ŵ�cache�У�
        //����BuffferPoolWriteTest��handleManyDirtyPagesҲ����֣�Ϊʲô�弸��
        //��Ҫ���Ƕ��page��Ӱ������
        pgs=table.insertTuple(tid,t);
        //��dirty page����cache,�����������ʱ�� Ǳ��������forѭ���ڸ��µĶ��page date������ͬ
        //�������ʱ��Ӧ��Ҳ����Ҫ̫��ȷ
        for(int i=0;i<pgs.size();i++)
        {
            PageId pid=pgs.get(i).getId();
            id2pg.put(pid, pgs.get(i));
            id2date.put(pid,new Date());
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
  qq       */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException, InterruptedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pgs=new ArrayList<>();

        int tableID=t.getRecordId().getPageId().getTableId();
        DbFile table = Database.getCatalog().getDatabaseFile(tableID);
        pgs=table.deleteTuple(tid,t);
        for(int i=0;i<pgs.size();i++)
        {
            PageId pid=pgs.get(i).getId();
            id2pg.put(pid, pgs.get(i));
            id2date.put(pid,new Date());
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Set<PageId> pids = new HashSet<PageId>(id2pg.keySet());
        for(PageId pid : pids) {
            flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        id2date.remove(pid);
        id2pg.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if(!id2pg.containsKey(pid))//���bp����û�����page
            return;
        Page pg=id2pg.get(pid);//ȡ��page
        if(pg.isDirty()==null)//�����dirty
            return;
        pg.markDirty(false,null);//ȥ��dirty���
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pg);//д���ļ�
        id2pg.put(pid,pg);//����BufferPool
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> pids = tid2lock.get(tid);
        for(PageId pid : pids) {
            //����ex��ʱflush
            if(pid2lock.get(pid)==0)
                flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException,IllegalArgumentException {
        // some code goes here
        // not necessary for lab1
        // LRU policy / MRU policy
        // ����id2pg�ҳ�latest/most recently
        PageId evictPgId=null;//������PageId
        Set<PageId> pids = new HashSet<PageId>(id2date.keySet());
        Date evictDate=new Date();
        //����policy��ȡevictPgId
        switch (Policy)
        {
            case 1:{
                for(PageId pid : pids) {
                    Date currDate=id2date.get(pid);
                    //������clean page
                    //���ڼ�ʱ����׼ȷ�Բ��㣬������ܳ���
                    if(currDate.compareTo(evictDate)<0&&id2pg.get(pid).isDirty()==null)
                    {
                        evictDate=currDate;
                        evictPgId=pid;
                    }
                }
                break;
            }
            case 2:{
                evictDate=new Date(0);//��ʼ��Ϊ�����ʱ��
                for(PageId pid : pids) {
                    Date currDate=id2date.get(pid);
                    if(currDate.compareTo(evictDate)>0&&id2pg.get(pid).isDirty()==null)
                    {
                        evictDate=currDate;
                        evictPgId=pid;
                    }
                }
                break;
            }
            case 3:{
                //��id2pg�����ȡһ��
                //Random rand = new Random();
                //int pos=rand.nextInt(pids.size()),i=0;
                for(PageId pid:pids){
                    if(id2pg.get(pid).isDirty()==null)
                        evictPgId=pid;
                }
                break;
            }
            default:throw new IllegalArgumentException();
        }
        if(evictPgId==null)
            throw new DbException("all pages are dirty");

        Page target=id2pg.get(evictPgId);
        if(target.isDirty()!=null)//�����dirty page����Ҫflush
        {
            try{
                flushPage(evictPgId);
            } catch (IOException e){
                e.printStackTrace();
            }
        }
        id2date.remove(evictPgId);//evictPgIdһ����Ϊnull
        id2pg.remove(evictPgId);

    }
    /**
     * Set eviction policy
     * @param p policy number 1��LRU 2��MRU 3���������
     */
    public void setPolicy(int p) throws IllegalArgumentException{
        //some code goes here
        // not necessary for lab1
        if(p>3||p<0)
            throw new IllegalArgumentException();
        Policy=p;
    }
}

