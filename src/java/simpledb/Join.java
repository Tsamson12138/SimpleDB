package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    JoinPredicate p;
    OpIterator child1;
    OpIterator child2;
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
       this.p=p;
       this.child1=child1;
       this.child2=child2;
    }

    public JoinPredicate getJoinPredicate() {
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(child1.getTupleDesc(),child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child1.open();
        child2.open();
    }

    public void close() {
        super.close();
        child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
       child1.rewind();
       child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    private Tuple t1=null;
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple result=null;
        while (true) {
            if(t1==null)//第一次调用fetchNext()函数时的t1初始化
                if(child1.hasNext())
                    t1 = child1.next();
                else
                    break;
            while (child2.hasNext()) {
                Tuple t2 = child2.next();
                if (p.filter(t1, t2)) {
                    int n1 = child1.getTupleDesc().numFields();
                    int n2 = child2.getTupleDesc().numFields();
                    result = new Tuple(getTupleDesc());
                    for (int i = 0; i < n1; i++) {
                        result.setField(i, t1.getField(i));
                    }
                    for (int i = 0; i < n2; i++) {
                        result.setField(i + n1, t2.getField(i));
                    }
                    break;
                }
            }
            if(result!=null) break;
            else {
                child2.rewind();
                if(child1.hasNext())
                    t1=child1.next();
                else
                    break;
            }
        }
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] children=new OpIterator[2];
        children[0]=child1;
        children[1]=child2;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child1=children[0];
        child2=children[1];
    }

}
