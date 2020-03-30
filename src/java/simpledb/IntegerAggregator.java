package simpledb;

import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.util.*;

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
    private Map<String, Integer> string_sum;
    private Map<Integer, Integer> int_sum;
    private int nogb_sum=0;
    private Map<String, Integer> string_size;
    private Map<Integer, Integer> int_size;
    private int nogb_size=0;
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        this.tuples=new ArrayList<>();
        if(gbfield!=-1) {
            Type[] typeAr = new Type[2];
            typeAr[0]=gbfieldtype;
            typeAr[1]=Type.INT_TYPE;
            tupleDesc = new TupleDesc(typeAr);
        }
        else{
            Type[] typeAr = new Type[1];
            typeAr[0]=Type.INT_TYPE;
            tupleDesc = new TupleDesc(typeAr);
        }
        if(gbfieldtype==Type.INT_TYPE){
            int_sum=new HashMap<>();
            string_sum=null;
        }else
            if(gbfieldtype==Type.STRING_TYPE){
            string_sum=new HashMap<>();
            int_sum=null;
        }
        if(gbfieldtype==Type.INT_TYPE){
            int_size=new HashMap<>();
            string_size=null;
        }else{
            string_size=new HashMap<>();
            int_size=null;
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
        int index=-1;//待删除元素的index
        int now_value=0;
        IntField merge_field=(IntField) tup.getField(afield);
        int merge_value=merge_field.getValue();
        boolean flag=true;
        if(gbfield!=-1) {
            tuple.setField(0, tup.getField(gbfield));
            if(!tuples.isEmpty()) {
                if (gbfieldtype == Type.INT_TYPE) {
                    IntField merge_gbfield = (IntField) tup.getField(gbfield);
                    int merge_gbvalue = merge_gbfield.getValue();
                    for (int i = 0; i < tuples.size(); i++)  {
                        IntField now_gbfield = (IntField) tuples.get(i).getField(0);
                        int now_gbvalue = now_gbfield.getValue();
                        if(merge_gbvalue==now_gbvalue) {
                            IntField now_afield=(IntField)tuples.get(i).getField(1);
                            now_value=now_afield.getValue();
                            flag=false;
                            index=i;
                            break;
                        }
                    }
                } else {
                    StringField merge_gbfield = (StringField) tup.getField(gbfield);
                    String merge_gbvalue = merge_gbfield.getValue();
                    for (int i = 0; i < tuples.size(); i++)  {
                        StringField now_gbfield = (StringField) tuples.get(i).getField(0);
                        String now_gbvalue = now_gbfield.getValue();
                        if(merge_gbvalue.equals(now_gbvalue)) {
                            IntField now_afield=(IntField)tuples.get(i).getField(1);
                            now_value=now_afield.getValue();
                            flag=false;
                            index=i;
                            break;
                        }
                    }
                }
            }
        }else{
            if(!tuples.isEmpty()) {
                IntField now_afield = (IntField) tuples.get(tuples.size() - 1).getField(0);
                now_value = now_afield.getValue();
                index=tuples.size() - 1;
            }
            flag=false;
        }
        if(tuples.isEmpty()||flag) {
            if(what==Op.COUNT){
                IntField new_field = new IntField(1);
                if(gbfield!=-1)
                    tuple.setField(1, new_field);
                else
                    tuple.setField(0, new_field);
            }
            else {
                if (gbfield != -1)
                    tuple.setField(1, tup.getField(afield));
                else
                    tuple.setField(0, tup.getField(afield));
            }
            if(gbfield!=-1)
                tuple.setField(0, tup.getField(gbfield));
        }else {
            int afield_index;
            if (gbfield != -1)
                afield_index=1;
            else
                afield_index=0;
            IntField new_field;
            switch (what) {
                case MIN:
                    new_field= new IntField(Math.min(now_value, merge_value));
                    tuple.setField(afield_index, new_field);
                    break;
                case MAX:
                    new_field = new IntField(Math.max(now_value, merge_value));
                    tuple.setField(afield_index, new_field);
                    break;
                case SUM:
                    new_field = new IntField(now_value + merge_value);
                    tuple.setField(afield_index, new_field);
                    break;
                case COUNT:
                    new_field = new IntField(now_value + 1);
                    tuple.setField(afield_index, new_field);
                    break;
                case AVG:
                    int size=0;
                    int sum=0;
                    if (gbfieldtype == Type.INT_TYPE) {
                        IntField merge_gbfield = (IntField) tup.getField(gbfield);
                        int merge_gbvalue = merge_gbfield.getValue();
                        size=int_size.get(merge_gbvalue);
                        sum=int_sum.get(merge_gbvalue);
                    }else
                        if(gbfieldtype==Type.STRING_TYPE){
                            StringField merge_gbfield = (StringField) tup.getField(gbfield);
                            String merge_gbvalue = merge_gbfield.getValue();
                            size=string_size.get(merge_gbvalue);
                            sum=string_sum.get(merge_gbvalue);
                        }else{
                            size=nogb_size;
                            sum=nogb_sum;
                        }
                    new_field = new IntField((sum+merge_value)/(size+1));
                    tuple.setField(afield_index, new_field);
                    break;
            }
        }
        if(index!=-1)
            tuples.remove(index);
        tuples.add(tuple);
        if (gbfieldtype == Type.INT_TYPE) {
            IntField merge_gbfield = (IntField) tup.getField(gbfield);
            int merge_gbvalue = merge_gbfield.getValue();
            if(int_sum.get(merge_gbvalue)==null) int_sum.put(merge_gbvalue,merge_value);
            else {
                int new_sum=int_sum.get(merge_gbvalue)+merge_value;
                int_sum.put(merge_gbvalue,new_sum);
            }
        }else
            if(gbfieldtype==Type.STRING_TYPE){
                StringField merge_gbfield = (StringField) tup.getField(gbfield);
                String merge_gbvalue = merge_gbfield.getValue();
                if(string_sum.get(merge_gbvalue)==null) string_sum.put(merge_gbvalue,merge_value);
                else {
                    int new_sum=string_sum.get(merge_gbvalue)+merge_value;
                    string_sum.put(merge_gbvalue,new_sum);
                }
            }else{
                nogb_sum+=merge_value;
            }
        if (gbfieldtype == Type.INT_TYPE) {
            IntField merge_gbfield = (IntField) tup.getField(gbfield);
            int merge_gbvalue = merge_gbfield.getValue();
            if(int_size.get(merge_gbvalue)==null) int_size.put(merge_gbvalue,1);
            else {
                int new_size=int_size.get(merge_gbvalue)+1;
                int_size.put(merge_gbvalue,new_size);
            }
        }else
            if(gbfieldtype==Type.STRING_TYPE){
                StringField merge_gbfield = (StringField) tup.getField(gbfield);
                String merge_gbvalue = merge_gbfield.getValue();
                if(string_size.get(merge_gbvalue)==null) string_size.put(merge_gbvalue,1);
                else {
                    int new_size=string_size.get(merge_gbvalue)+1;
                    string_size.put(merge_gbvalue,new_size);
                }
             } else{
                nogb_size++;
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
