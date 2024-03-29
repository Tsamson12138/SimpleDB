package simpledb;

import java.io.*;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    enum LockType{
        Excluscive,Shared
    }
    class Lock{
        private PageId pid;
        private LockType type;
        private ArrayList<TransactionId> holders;
        public Lock(PageId pid,LockType type,ArrayList<TransactionId>holders){
            this.pid=pid;
            this.type=type;
            this.holders=holders;
        }
        public void upgradeLockType(){
            type=LockType.Excluscive;
        }
        public PageId getPid() {
            return pid;
        }
        public void setPid(PageId pid) {
            this.pid = pid;
        }
        public LockType getType() {
            return type;
        }
        public void setType(LockType type) {
            this.type = type;
        }
        public ArrayList<TransactionId> getHolders() { return holders; }
        public void setHolders(ArrayList<TransactionId> holders) { this.holders = holders; }
    }
    class LockManager{
        private  Map<TransactionId,ArrayList<PageId>> transactionLocks;
        private  Map<PageId, Lock> pageLocks;
        private  Map<TransactionId,ArrayList<PageId>> waitTable;//依赖图
        public LockManager(){
//            transactionLocks=new HashMap<>();
//            pageLocks=new HashMap<>();
            transactionLocks=new ConcurrentHashMap<>();
            pageLocks=new ConcurrentHashMap<>();
            waitTable=new ConcurrentHashMap<>();
        }
        public synchronized void updateTransactionLocks(TransactionId tid,PageId pid){
            ArrayList<PageId> curLockList = transactionLocks.get(tid);
            if (curLockList == null) {//新事务
                ArrayList<PageId> newLockList = new ArrayList<>();
                newLockList.add(pid);
                transactionLocks.put(tid, newLockList);
            } else {//旧事务
                curLockList.add(pid);
            }
        }
        public synchronized boolean independencyGraphDeadLockCheck(TransactionId tid,PageId pid){
            //直接
            boolean ifDeadLock=false;
            ArrayList<TransactionId> nowLockHolders=pageLocks.get(pid).getHolders();
            ArrayList<PageId>nowOccupyResources=transactionLocks.get(tid);
            if(nowLockHolders!=null&&!nowLockHolders.isEmpty()) {
                for (TransactionId nowLockHolder : nowLockHolders) {
                    if(nowLockHolder!=tid) {
                        if (nowOccupyResources != null && !nowOccupyResources.isEmpty()) {
                            for (PageId nowOccupyResource : nowOccupyResources) {
                                if (waitTable.containsKey(nowLockHolder)) {
                                    if (waitTable.get(nowLockHolder).contains(nowOccupyResource)) {
                                        ifDeadLock = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if (ifDeadLock)
                        break;
                }
            }
            if(ifDeadLock)
                return true;
            else{
                //间接
                if(nowLockHolders!=null&&!nowLockHolders.isEmpty()) {
                    for (TransactionId nowLockHolder : nowLockHolders) {
                        if(nowLockHolder!=tid) {
                            if (waitTable.containsKey(nowLockHolder)) {
                                ArrayList<PageId>tempPages=waitTable.get(nowLockHolder);
                                for(PageId tempPage:tempPages){
                                    ifDeadLock=independencyGraphDeadLockCheck(tid,tempPage);
                                    if (ifDeadLock)
                                        break;
                                }
                            }
                        }
                        if (ifDeadLock)
                            break;
                    }
                }
                return ifDeadLock;
            }

        }
        public synchronized void acquireLock(TransactionId tid,PageId pid,LockType type) throws TransactionAbortedException {
            //System.out.println(tid+" "+pid.getPageNumber()+" "+type);
            long begin=System.currentTimeMillis();
            long timeout=new Random().nextInt(5001) ;
            while (true) {
                Lock curLock = pageLocks.get(pid);
                if (curLock == null) {//无锁
                    ArrayList<TransactionId> new_holders = new ArrayList<>();
                    new_holders.add(tid);
                    Lock newLock = new Lock(pid, type, new_holders);
                    pageLocks.put(pid, newLock);
                    updateTransactionLocks(tid,pid);
                    break;
                } else {//有锁
                    if (curLock.getType() == LockType.Shared ) {//有共享锁
                        if(type==LockType.Shared) {//继续共享
                            if(!curLock.getHolders().contains(tid)){
                                ArrayList<TransactionId> curHolders = curLock.getHolders();
                                curHolders.add(tid);
                                updateTransactionLocks(tid, pid);
                            }
                            break;
                        }else{//独自共享时可更新成独占
                            //System.out.println(tid+"  Reading request write");
                            if(curLock.getHolders().size()==1&&curLock.getHolders().get(0)==tid){
                                curLock.upgradeLockType();
                                //System.out.println("***********************");
                                //System.out.println(tid+"  upgrade successfully");
                                break;
                            }
                            //System.out.println(tid+"  block");
                        }
                    }else{//有独占锁
                        if(curLock.getHolders().get(0)==tid){//还是该事务重新请求独占锁
                            //do nothing
                            break;
                        }
                    }
                }
                //修改waitTable
                if(waitTable.containsKey(tid)){
                    waitTable.get(tid).add(pid);
                }else{
                    ArrayList<PageId>waitPages=new ArrayList<>();
                    waitPages.add(pid);
                    waitTable.put(tid,waitPages);
                }
                //依赖图死锁检查
                if(independencyGraphDeadLockCheck(tid,pid))
                    throw new TransactionAbortedException();
                //超时timeout死锁检查
//                if (System.currentTimeMillis() - begin > timeout) {
//                    throw new TransactionAbortedException();
//                }
                try {
                    wait(timeout);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //已获得锁，从waitTable中删除
            if(waitTable.containsKey(tid)){
                waitTable.get(tid).remove(pid);
                if(waitTable.get(tid).isEmpty())
                    waitTable.remove(tid);
            }

        }
        public synchronized void releaseLock(TransactionId tid,PageId pid){
            //System.out.println("release   "+tid+" "+pid.getPageNumber());
            if (transactionLocks.containsKey(tid)) {
                transactionLocks.get(tid).remove(pid);
                if (transactionLocks.get(tid).size() == 0) {
                    transactionLocks.remove(tid);
                }
            }

            // remove from locktable
            if (pageLocks.containsKey(pid)) {
                pageLocks.get(pid).getHolders().remove(tid);
                if (pageLocks.get(pid).getHolders().size() == 0) {
                    pageLocks.remove(pid);
                }
            }
        }
        public synchronized void releaseAllLocks(TransactionId tid){
            ArrayList<PageId>curLockList=transactionLocks.get(tid);
            if(curLockList!=null) {
                for (int i = 0; i < curLockList.size(); i++) {
                    releaseLock(tid, curLockList.get(i));
                }
            }
        }
        public synchronized boolean holdsLock(TransactionId tid,PageId pid){
            if(transactionLocks.containsKey(tid)){
                if(transactionLocks.get(tid).contains(pid))
                    return true;
                else
                    return false;
            }else
                return false;
        }

    }
    private LockManager lockManager;
    /** Bytes per page, including header. */
    private Map<PageId,Page> bufferpool;
    private int numPages;
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
//        bufferpool=new LinkedHashMap<>(numPages);
        bufferpool=new ConcurrentHashMap<>(numPages);
        this.numPages=numPages;
        lockManager=new LockManager();
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
            throws TransactionAbortedException, DbException {
        LockType type;
        if(perm==Permissions.READ_ONLY)
            type=LockType.Shared;
        else
            type=LockType.Excluscive;
        lockManager.acquireLock(tid,pid,type);
        //System.out.println("finish");
        if(bufferpool.get(pid)==null){
            int tabelID=pid.getTableId();
            DbFile file=Database.getCatalog().tables.get(tabelID).file;
            Page page=file.readPage(pid);
            if(bufferpool.size()>=numPages) evictPage();
            bufferpool.put(pid,page);
            return page;
        }
        else
            return bufferpool.get(pid);
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
        lockManager.releaseLock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid,p);
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
        if(commit){
            flushPages(tid);
        }
        ArrayList<PageId>curLockList=lockManager.transactionLocks.get(tid);
        if(curLockList!=null) {
            for (int i = 0; i < curLockList.size(); i++) {
                Page curPage = bufferpool.get(curLockList.get(i));
                if (curPage != null) {
                    if (curPage.isDirty() != null) {
                        curPage.markDirty(false, null);
                        discardPage(curLockList.get(i));
                    }
                }
            }
        }
        lockManager.releaseAllLocks(tid);
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
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page>arrayList;
        DbFile file=Database.getCatalog().tables.get(tableId).file;
        arrayList=file.insertTuple(tid,t);
        for (int i = 0; i < arrayList.size(); i++) {
            Page page=arrayList.get(i);
            if(page.isDirty()!=null) {
                bufferpool.remove(page.getId(),page);
                bufferpool.put(page.getId(), page);
            }
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
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page>arrayList;
        RecordId recordId=t.getRecordId();
        PageId pageId=recordId.getPageId();
        DbFile file=Database.getCatalog().tables.get(pageId.getTableId()).file;
        arrayList=file.deleteTuple(tid,t);
        for (int i = 0; i < arrayList.size(); i++) {
            Page page=arrayList.get(i);
            if(page.isDirty()!=null) {
                bufferpool.remove(page.getId(),page);
                bufferpool.put(page.getId(), page);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(Map.Entry<PageId,Page>entry:bufferpool.entrySet()){
            PageId flush_pid=entry.getKey();
            Page flush_page=entry.getValue();
            flushPage(flush_pid);
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
        bufferpool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        DbFile dbFile=Database.getCatalog().tables.get(pid.getTableId()).file;
        Page page=bufferpool.get(pid);
        if(page!=null) {
            if (page.isDirty() != null) {
                dbFile.writePage(page);
                page.markDirty(false, null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        ArrayList<PageId>curLockList=lockManager.transactionLocks.get(tid);
        if(curLockList!=null) {
            for (int i = 0; i < curLockList.size(); i++) {
                flushPage(curLockList.get(i));
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        boolean flag=false;
        for(Map.Entry<PageId,Page>entry:bufferpool.entrySet()) {
            PageId evict_pid = entry.getKey();
            Page evict_page = entry.getValue();
            if (evict_page.isDirty() == null) {
                //flushPage(evict_pid);
                discardPage(evict_pid);
                flag=true;
                break;
            }
        }
        if(!flag)
            throw new DbException("No page can be evicted!");
    }

}
