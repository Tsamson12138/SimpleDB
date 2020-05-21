package simpledb;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

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
        this.f=f;
        this.td=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize=BufferPool.getPageSize();
//        int tableId=pid.getTableId();
        int pgNo=pid.getPageNumber();
//        Catalog.Table table=Database.getCatalog().tables.get(tableId);
//        DbFile file=table.file;
//        File f;
//        HeapFile heapFile=(HeapFile)file;
//        f=heapFile.getFile();
        byte[]bytes;
        HeapPage heapPage=null;//初始化用null
        try {
            RandomAccessFile raf=new RandomAccessFile(f,"r");
            raf.seek(pgNo*pageSize);
            bytes=new byte[pageSize];
            raf.read(bytes);
            raf.close();
            heapPage=new HeapPage((HeapPageId) pid,bytes);
        }catch (IOException o){
            o.printStackTrace();
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        byte[]data=page.getPageData();
        RandomAccessFile raf=new RandomAccessFile(f, "rw");
        raf.seek(page.getId().getPageNumber()*BufferPool.getPageSize());
        raf.write(data);
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int pageSize=BufferPool.getPageSize();
        byte[] buffer = null;
        try {
            FileInputStream fis = new FileInputStream(f);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] b = new byte[1024];
            int n;
            while ((n = fis.read(b)) != -1) {
                bos.write(b, 0, n);
            }
            fis.close();
            bos.close();
            buffer = bos.toByteArray();
        }catch (IOException o){
            o.printStackTrace();
        }
        return Math.floorDiv(buffer.length,pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page>arrayList=new ArrayList<>();
        HeapPageId heapPageId;
        BufferPool bufferPool=Database.getBufferPool();
        HeapPage heapPage;
        boolean flag=true;
        for (int i = 0; i < numPages(); i++) {
            heapPageId=new HeapPageId(getId(),i);
            heapPage=(HeapPage)bufferPool.getPage(tid,heapPageId,Permissions.READ_WRITE);
            try{
                heapPage.insertTuple(t);
                heapPage.markDirty(true,tid);
                arrayList.add(heapPage);
                break;
            }catch (DbException e){
                arrayList.add(heapPage);
                bufferPool.releasePage(tid,heapPageId);
                if(i==numPages()-1)
                    flag=false;
            }
        }
        if(numPages()==0) flag=false;
        if(!flag){
            heapPageId=new HeapPageId(getId(),numPages());
            byte[]data=HeapPage.createEmptyPageData();
            heapPage=new HeapPage(heapPageId,data);
            writePage(heapPage);
            heapPage=(HeapPage)bufferPool.getPage(tid,heapPageId,Permissions.READ_WRITE);
            heapPage.insertTuple(t);
            heapPage.markDirty(true,tid);
            arrayList.add(heapPage);
        }
        return arrayList;
//        return null;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page>arrayList=new ArrayList<>();
        BufferPool bufferPool=Database.getBufferPool();
        RecordId recordId=t.getRecordId();
        HeapPage page=(HeapPage)bufferPool.getPage(tid,recordId.getPageId(),Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true,tid);
        arrayList.add(page);
//        return null;
        return arrayList;
    }


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            int tableID;
            int pgNo;
            HeapPageId heapPageId;
            Page page;
            HeapPage heapPage;
            Iterator<Tuple> tupleIterator;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                tableID=getId();
                pgNo=0;
                heapPageId=new HeapPageId(tableID,pgNo);
                page=Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_ONLY);
                heapPage=(HeapPage)page;
                tupleIterator=heapPage.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(tupleIterator==null) return false;
                if(tupleIterator.hasNext())
                    return true;
                else{
                    if(pgNo>=numPages()-1) return false;
                    else{
                        pgNo++;
                        heapPageId=new HeapPageId(tableID,pgNo);
                        page=Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_ONLY);
                        heapPage=(HeapPage)page;
                        tupleIterator=heapPage.iterator();
                        if(tupleIterator.hasNext()) return true;
                        else return false;
                    }
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(tupleIterator==null) throw new NoSuchElementException();
                return tupleIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pgNo=0;
                heapPageId=new HeapPageId(tableID,pgNo);
                page=Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_ONLY);
                heapPage=(HeapPage)page;
                tupleIterator=heapPage.iterator();
            }

            @Override
            public void close() {
                heapPageId=null;
                page=null;
                heapPage=null;
                tupleIterator=null;
            }
        };
    }

}

