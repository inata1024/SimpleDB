package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

import static simpledb.Aggregator.Op.COUNT;
import static simpledb.Type.INT_TYPE;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private HashMap<Field,Integer> field2int;
    private TupleDesc td;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.op=what;
        if(what!=Op.COUNT)
            throw new IllegalArgumentException();
        field2int=new HashMap<>();
        td = gbfieldtype != null?
                new TupleDesc(new Type[]{gbfieldtype,INT_TYPE}):
                new TupleDesc(new Type[]{INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //Ĭ��ΪCOUNT
        Field currField=(gbfield!=NO_GROUPING)?//��ǰgbfield,Ϊ����Խ�磬��Ҫ�ж�
                tup.getField(gbfield):
                new IntField(0);//��ΪNO_GROUPINGʱ�ĸ���field

        int size=field2int.size();
        if(field2int.containsKey(currField))
        {
            int pre=field2int.get(currField);//���е�countֵ
            field2int.put(currField,pre+1);
        }
        else
            field2int.put(currField,1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (HashMap.Entry<Field, Integer> map : field2int.entrySet()) {
            Tuple t = new Tuple(td);
            if (gbfieldtype == null)
            {
                t.setField(0, new IntField(map.getValue()));
            }
            else
            {
                t.setField(0, map.getKey());
                t.setField(1, new IntField(map.getValue()));
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}
