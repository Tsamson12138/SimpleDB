package simpledb;

import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private int count;
    private TupleDesc tupleDesc;
    private Tuple result;
    private int index;
    private Tuple[] tuples;
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tid=t;
        this.child=child;
        this.tableId=tableId;
        Type[]typeAr=new Type[1];
        typeAr[0]=Type.INT_TYPE;
        this.tupleDesc=new TupleDesc(typeAr);
        this.count=0;
        this.tuples=new Tuple[1];
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        index=0;
        child.open();
        while(child.hasNext()) {
            Tuple insert_tuple=child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, insert_tuple);
            }catch (IOException ioe){
                ioe.printStackTrace();
            }
            count++;
        }
        result= new Tuple(tupleDesc);
        IntField field = new IntField(count);
        result.setField(0, field);
        tuples[0]=result;
        child.close();
    }

    public void close() {
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        index=0;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(index==0)
            return tuples[index++];
        else
            return null;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] children=new OpIterator[1];
        children[0]=child;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child=children[0];
    }
}
