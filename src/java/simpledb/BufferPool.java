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
        private ArrayList<TransactionId> holeders;
        public Lock(PageId pid,LockType type,ArrayList<TransactionId>holeders){
            this.pid=pid;
            this.type=type;
            this.holeders=holeders;
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
        public ArrayList<TransactionId> getHoleders() { return holeders; }
        public void setHoleders(ArrayList<TransactionId> holeders) { this.holeders = holeders; }
    }
    class LockManager{
        private  Map<TransactionId,ArrayList<PageId>> transactionLocks;
        private  Map<PageId, Lock> pageLocks;
        public LockManager(){
//            transactionLocks=new HashMap<>();
//            pageLocks=new HashMap<>();
            transactionLocks=new ConcurrentHashMap<>();
            pageLocks=new ConcurrentHashMap<>();
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
        public synchronized void acquireLock(TransactionId tid,PageId pid,LockType type) throws TransactionAbortedException {
            System.out.println(tid);
            System.out.println(pid.getPageNumber());
            System.out.println(type);
            long begin=System.currentTimeMillis();
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
                            if(!curLock.getHoleders().contains(tid)){
                                ArrayList<TransactionId> curHolders = curLock.getHoleders();
                                curHolders.add(tid);
                                updateTransactionLocks(tid, pid);
                            }
                            break;
                        }else{//独自共享时可更新成独占
                            if(curLock.getHoleders().size()==1&&curLock.getHoleders().get(0)==tid){
                                curLock.upgradeLockType();
                                break;
                            }
                        }
                    }else{//有独占锁
                        if(curLock.getHoleders().get(0)==tid){//还是该事务重新请求独占锁
                            //do nothing
                            break;
                        }
                    }
                }
                if(System.currentTimeMillis()-begin>2000)
                    throw new TransactionAbortedException();
                try {
                    Random random = new Random();
                    int waitTime = random.nextInt(50) + 50;
                    wait(waitTime);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }
        public synchronized void releaseLock(TransactionId tid,PageId pid){
            Lock curLock=pageLocks.get(pid);
            if(curLock.getHoleders().size()==1){
                pageLocks.remove(pid);
                transactionLocks.get(tid).remove(pid);
            }else{
                ArrayList<TransactionId> curHolders = curLock.getHoleders();
                curHolders.remove(tid);
                curLock.setHoleders(curHolders);
                pageLocks.put(pid, curLock);
                transactionLocks.get(tid).remove(pid);
            }
        }
        public synchronized void releaseAllLocks(TransactionId tid){
            ArrayList<PageId>curLockList=transactionLocks.get(tid);
            for(int i=0;i<curLockList.size();i++){
                releaseLock(tid,curLockList.get(i));
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
        }else{
            ArrayList<PageId>curLockList=lockManager.transactionLocks.get(tid);
            for(int i=0;i<curLockList.size();i++){
                Page curPage=bufferpool.get(curLockList.get(i));
                if(curPage!=null) {
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
            if(bufferpool.size()>=numPages) evictPage();
            bufferpool.put(page.getId(),page);
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
            if(bufferpool.size()>=numPages) evictPage();
            bufferpool.put(page.getId(),page);
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
                page.markDirty(false, null);
                dbFile.writePage(page);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        ArrayList<PageId>curLockList=lockManager.transactionLocks.get(tid);
        for(int i=0;i<curLockList.size();i++){
            flushPage(curLockList.get(i));
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
