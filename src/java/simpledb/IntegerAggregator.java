package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc tupleDesc;
    private List<Tuple> tuples;
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        this.tuples=new ArrayList<>();
        if(gbfield!=-1) {
            Type[] typeAr = new Type[2];
            typeAr[gbfield]=gbfieldtype;
            typeAr[afield]=Type.INT_TYPE;
            tupleDesc = new TupleDesc(typeAr);
        }
        else{
            Type[] typeAr = new Type[1];
            typeAr[afield]=Type.INT_TYPE;
            tupleDesc = new TupleDesc(typeAr);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Tuple tuple=new Tuple(tupleDesc);
        if(gbfield!=-1)
            tuple.setField(gbfield,tup.getField(gbfield));
        switch(what){
            case MIN:
                if(tuples.isEmpty()) {
                    tuple.setField(afield,tup.getField(afield));
                }
                else{
                    IntField merge_field=(IntField) tup.getField(afield);
                    int merge_value=merge_field.getValue();
                    IntField now_field=(IntField)tuples.get(tuples.size()-1).getField(afield);
                    int now_value=now_field.getValue();
                    IntField new_field=new IntField(Math.min(now_value,merge_value));
                    tuple.setField(afield,new_field);
                }
                tuples.add(tuple);
                break;
            case MAX:
                if(tuples.isEmpty()) {
                    tuple.setField(afield,tup.getField(afield));
                }
                else{
                    IntField merge_field=(IntField) tup.getField(afield);
                    int merge_value=merge_field.getValue();
                    IntField now_field=(IntField)tuples.get(tuples.size()-1).getField(afield);
                    int now_value=now_field.getValue();
                    IntField new_field=new IntField(Math.max(now_value,merge_value));
                    tuple.setField(afield,new_field);
                }
                tuples.add(tuple);
                break;
            case SUM:
                if(tuples.isEmpty()) {
                    tuple.setField(afield,tup.getField(afield));
                }
                else{
                    IntField merge_field=(IntField) tup.getField(afield);
                    int merge_value=merge_field.getValue();
                    IntField now_field=(IntField)tuples.get(tuples.size()-1).getField(afield);
                    int now_value=now_field.getValue();
                    IntField new_field=new IntField(now_value+merge_value);
                    tuple.setField(afield,new_field);
                }
                tuples.add(tuple);
                break;
            case COUNT:
                if(tuples.isEmpty()) {
                    IntField new_field=new IntField(0);
                    tuple.setField(afield,new_field);
                }
                else{
                    IntField now_field=(IntField)tuples.get(tuples.size()-1).getField(afield);
                    int now_value=now_field.getValue();
                    IntField new_field=new IntField(now_value+1);
                    tuple.setField(afield,new_field);
                }
                tuples.add(tuple);
                break;
            case AVG:
                if(tuples.isEmpty()) {
                    tuple.setField(afield,tup.getField(afield));
                }
                else{
                    IntField merge_field=(IntField) tup.getField(afield);
                    int merge_value=merge_field.getValue();
                    IntField now_field=(IntField)tuples.get(tuples.size()-1).getField(afield);
                    int now_value=now_field.getValue();
                    int sum=now_value*tuples.size();
                    IntField new_field=new IntField(sum+merge_value/(tuples.size()+1));
                    tuple.setField(afield,new_field);
                }
                tuples.add(tuple);
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new Operator() {
            int now_index=0;
            @Override
            protected Tuple fetchNext() throws DbException, TransactionAbortedException {
                if(now_index>tuples.size()-1) return null;
                else return tuples.get(now_index++);
            }

            @Override
            public OpIterator[] getChildren() {
                return new OpIterator[0];
            }

            @Override
            public void setChildren(OpIterator[] children) {

            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                 now_index=0;
            }
        };
    }

}
