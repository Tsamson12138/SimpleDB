package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId tid;
    private OpIterator child;
    private int count;
    private TupleDesc tupleDesc;
    private Tuple result;
    private int index;
    private Tuple[] tuples;
    public Delete(TransactionId t, OpIterator child) {
        this.tid=t;
        this.child=child;
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
            Tuple delete_tuple=child.next();
            //Database.getBufferPool().releasePage(tid,delete_tuple.getRecordId().getPageId());
            try {
                Database.getBufferPool().deleteTuple(tid,delete_tuple);
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
