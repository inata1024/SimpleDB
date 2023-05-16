package simpledb;

import java.io.*;

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
    private ConcurrentHashMap<PageId, Date> id2date;//记录bp中page的访问时间

    /**
     * 通过PageId确定Page上的锁，1:shared锁 0:exclusive锁 没这个key:没锁
     * 凡是有新page到BufferPool，应同时更新id2lock
     */
    private ConcurrentHashMap<PageId,Integer> pid2lock;
    /**
     * 通过TransactionId确定一个Transaction包含的锁
     */
    private ConcurrentHashMap<TransactionId,HashSet<PageId>> tid2lock;

    private int maxPagenum;
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    private int Policy=1;//1：LRU 2：MRU 3：随机策略 默认为LRU
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    //事务获取不到锁时需要等待
    private final long SLEEP_INTERVAL;
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
        maxPagenum=numPages;
        SLEEP_INTERVAL = 500;
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
     * 判断tid是否包含对pid的锁
     */
    public boolean ownLock(TransactionId tid,PageId pid)
    {
        //如果当前tid的hashset还没初始化
        if(!tid2lock.containsKey(tid))
        {
            HashSet<PageId> hs=new HashSet<>();
            tid2lock.put(tid,hs);
            return false;
        }
        return tid2lock.get(tid).contains(pid);
    }

    /**
     * 方便起见，将这个方法同步
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
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException, InterruptedException {
        // some code goes here
        /**
         * 规则:
         * 一个transaction要读page，必须有shared锁
         * 一个transaction要写page，必须有exclusive锁
         * 如果一个transaction是一个page的shared锁的唯一拥有者，可以将其提升至exclusive
         */

        int needLock=-1;//判断是否需要加锁的标志，默认-1为不需要，0为ex锁，1为shared锁
        //读 并且 不拥有锁 需要进一步判断
        if(perm==Permissions.READ_ONLY&&!ownLock(tid,pid))
        {
            //如果page上有锁
            if(pid2lock.containsKey(pid))
            {
                //如果page上有shared锁
                if(pid2lock.get(pid)>0)
                    needLock=1;//加shared锁
                //如果page上是exclusive锁
                else
                {
                    //阻塞直到page上没锁
                    while(pid2lock.containsKey(pid))
                        Thread.sleep(SLEEP_INTERVAL);
                    needLock=1;//加shared锁
                }
            }
            //如果page上没锁
            else
            {
                needLock=1;//加shared锁
            }
        }

        if(perm==Permissions.READ_WRITE)
        {
            //tid拥有对page的锁，如果是shared，需要进一步判断；如果是ex，直接getpage
            if(ownLock(tid,pid)&&pid2lock.get(pid)>0)
            {
                //如果非唯一拥有，阻塞直到唯一拥有
                while(pid2lock.get(pid)>1)
                    Thread.sleep(SLEEP_INTERVAL);
                needLock=0;//加ex锁
            }
            //tid没有对page的锁
            if(!ownLock(tid,pid))
            {
                //如果page上有锁，阻塞直到没锁；没锁就不用阻塞
                while(pid2lock.containsKey(pid))
                    Thread.sleep(SLEEP_INTERVAL);
                needLock=0;//加ex锁
            }
        }

        //加shared锁
        if(needLock==1)
        {
            //若已有shared锁,自增1表示多了一个shared锁
            if(pid2lock.containsKey(pid))
                pid2lock.put(pid,pid2lock.get(pid)+1);
            else
            {
                pid2lock.put(pid,1);
                HashSet<PageId> temp=tid2lock.get(tid);
                temp.add(pid);
            }
        }
        //加ex锁
        if(needLock==0)
        {
            pid2lock.put(pid,0);
            HashSet<PageId> temp=tid2lock.get(tid);
            temp.add(pid);
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
            id2date.put(pid,new Date());//记录请求当前页的时间
            return newPage;
        }
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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        //如果是ex锁，直接remove；如果是shared锁，得分情况

        //获取transaction拥有的锁
        HashSet<PageId> t_locks=tid2lock.get(tid);
        Integer lockType=pid2lock.get(pid);
        //如果是shared锁
        if(lockType>0)
        {
            //如果唯一拥有，直接remove
            if(lockType==1)
                pid2lock.remove(pid);
            //非唯一拥有，把自己的一份减去
            else
                pid2lock.put(pid,lockType-1);
        }
        //如果是ex锁，remove
        else
            pid2lock.remove(pid);
        //tid不拥有对此page的锁
        t_locks.remove(pid);
        //更新tid的所有锁
        tid2lock.put(tid,t_locks);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        HashSet<PageId> t_locks=tid2lock.get(tid);
        for (PageId tLock : t_locks) releasePage(tid, tLock);

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
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
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
        //疑惑：为什么要把dirty page放到cache中？
        //而且BuffferPoolWriteTest中handleManyDirtyPages也很奇怪，为什么插几个
        //需要考虑多个page受影响的情况
        pgs=table.insertTuple(tid,t);
        //将dirty page加入cache,并更新其访问时间 潜在隐患：for循环内更新的多个page date可能相同
        //不过这个时间应该也不需要太精确
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
        if(!id2pg.containsKey(pid))//如果bp里面没有这个page
            return;
        Page pg=id2pg.get(pid);//取出page
        if(pg.isDirty()==null)//如果不dirty
            return;
        pg.markDirty(false,null);//去掉dirty标记
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pg);//写入文件
        id2pg.put(pid,pg);//更新BufferPool
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException,IllegalArgumentException {
        // some code goes here
        // not necessary for lab1
        // LRU policy / MRU policy
        // 遍历id2pg找出latest/most recently
        PageId evictPgId=null;//待驱逐PageId
        Set<PageId> pids = new HashSet<PageId>(id2date.keySet());
        Date evictDate=new Date();
        //根据policy获取evictPgId
        switch (Policy)
        {
            case 1:{
                for(PageId pid : pids) {
                    Date currDate=id2date.get(pid);
                    if(currDate.compareTo(evictDate)<0)
                    {
                        evictDate=currDate;
                        evictPgId=pid;
                    }
                }
                break;
            }
            case 2:{
                evictDate=new Date(0);//初始化为最早的时间
                for(PageId pid : pids) {
                    Date currDate=id2date.get(pid);
                    if(currDate.compareTo(evictDate)>0)
                    {
                        evictDate=currDate;
                        evictPgId=pid;
                    }
                }
                break;
            }
            case 3:{
                //在id2pg中随机取一个
                Random rand = new Random();
                int pos=rand.nextInt(pids.size()),i=0;
                for(PageId pid:pids){
                    if(i==pos)
                        evictPgId=pid;
                    i++;
                }
                break;
            }
            default:throw new IllegalArgumentException();
        }

        Page target=id2pg.get(evictPgId);
        if(target.isDirty()!=null)//如果是dirty page，需要flush
        {
            try{
                flushPage(evictPgId);
            } catch (IOException e){
                e.printStackTrace();
            }
        }
        id2date.remove(evictPgId);//evictPgId一定不为null
        id2pg.remove(evictPgId);
    }
    /**
     * Set eviction policy
     * @param p policy number 1：LRU 2：MRU 3：随机策略
     */
    public void setPolicy(int p) throws IllegalArgumentException{
        //some code goes here
        // not necessary for lab1
        if(p>3||p<0)
            throw new IllegalArgumentException();
        Policy=p;
    }
}

