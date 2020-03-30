package simpledb;

import com.sun.deploy.security.SelectableSecurityManager;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    private OpIterator child;
    private int afield;
    private int gbfield;
    private Aggregator.Op aop;
    private TupleDesc tupleDesc;
    private Aggregator aggregator;
    private OpIterator opIterator;
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	    this.child=child;
	    this.afield=afield;
	    this.gbfield=gfield;
	    this.aop=aop;
        tupleDesc=child.getTupleDesc();
        Type gbfieldtype=null;
        if(this.gbfield!=-1)
            gbfieldtype=tupleDesc.getFieldType(this.gbfield);
        Type afieldtype=tupleDesc.getFieldType(this.afield);
        if(afieldtype==Type.INT_TYPE){
            aggregator=new IntegerAggregator(this.gbfield,gbfieldtype,this.afield,this.aop);
            opIterator=aggregator.iterator();
        }else{
            aggregator=new StringAggregator(this.gbfield,gbfieldtype,this.afield,this.aop);
            opIterator=aggregator.iterator();
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    return gbfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        if(gbfield!=-1)
            return tupleDesc.getFieldName(gbfield);
        else
            return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	   return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return tupleDesc.getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	   return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    super.open();
	    child.open();
	    while (child.hasNext())
	        aggregator.mergeTupleIntoGroup(child.next());
	    child.close();
        opIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    if(opIterator.hasNext())
	        return opIterator.next();
	    else return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	   opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc output_tupleDesc;
        if(gbfield!=-1){
            Type[] typeAr=new Type[2];
            typeAr[1]=Type.INT_TYPE;
            typeAr[0]=tupleDesc.getFieldType(gbfield);
            String[] nameAr=new String[2];
//            if(aggregator instanceof IntegerAggregator) {
//                nameAr[afield]="IntegerAggregator("+aop.toString()+") ("+aggregateFieldName()+"))";
//            }else{
//                nameAr[afield]="StringAggregator("+aop.toString()+") ("+aggregateFieldName()+"))";
//            }
            nameAr[1]=aggregateFieldName();
            nameAr[0]=groupFieldName();
            output_tupleDesc=new TupleDesc(typeAr,nameAr);
        }else{
            Type[] typeAr=new Type[1];
            typeAr[0]=Type.INT_TYPE;
            String[] nameAr=new String[1];
//            if(aggregator instanceof IntegerAggregator) {
//                nameAr[afield]="IntegerAggregator("+aop.toString()+") ("+aggregateFieldName()+"))";
//            }else{
//                nameAr[afield]="StringAggregator("+aop.toString()+") ("+aggregateFieldName()+"))";
//            }
            nameAr[0]=aggregateFieldName();
            output_tupleDesc=new TupleDesc(typeAr,nameAr);
        }
        return output_tupleDesc;
    }

    public void close() {
	   super.close();
	   opIterator.close();
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
