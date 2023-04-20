package simpledb;
import java.util.HashMap;
import java.util.ArrayList;

import static simpledb.Aggregator.Op.*;
import static simpledb.Type.INT_TYPE;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private HashMap<Field,Integer> field2int;//gbfield到聚合结果的hashmap
    private HashMap<Field,Integer> avgCount;//记录avg中，每组的元素个数
    private HashMap<Field,Integer> avgSum;//记录avg的sum
    private HashMap<Field,Integer> varRec;//用于方差计算，记录平方和
    private HashMap<Field,Integer> varAvg;//用于方差计算，记录期望

    private TupleDesc td;
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

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.op=what;
        field2int=new HashMap<>();
        avgCount=new HashMap<>();
        avgSum=new HashMap<>();
        varRec=new HashMap<>();
        varAvg=new HashMap<>();
        td = gbfield != NO_GROUPING?
                new TupleDesc(new Type[]{gbfieldtype,INT_TYPE}):
                new TupleDesc(new Type[]{INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field currField=(gbfield!=NO_GROUPING)?//当前gbfield,为避免越界，需要判断
                tup.getField(gbfield):
                new IntField(0);//作为NO_GROUPING时的辅助field
        int curr=((IntField)tup.getField(afield)).getValue();//当前AggregateField中的值
        if(op==MIN)
        {
            if(field2int.containsKey(currField))
            {
                int pre=field2int.get(currField);//已有的min值
                field2int.put(currField,Math.min(pre,curr));
            }
            else
                field2int.put(currField,curr);
        }
        if(op==MAX)
        {
            if (field2int.containsKey(currField)) {
                int pre = field2int.get(currField);//已有的max值
                field2int.put(currField, Math.max(pre, curr));
            } else
                field2int.put(currField, curr);
        }
        if(op==SUM)
        {
            if(field2int.containsKey(currField))
            {
                int pre=field2int.get(currField);//已有的sum值
                field2int.put(currField,pre+curr);
            }
            else
                field2int.put(currField,curr);
        }
        if(op==AVG)//必须用sum/count，不能每次都除，因为每次都有舍入误差
        {
            if(field2int.containsKey(currField))
            {
                int size=avgCount.get(currField);
                int pre=avgSum.get(currField);
                avgSum.put(currField,pre+curr);//更新当前sum
                avgCount.put(currField,size+1);//更新当前count
                field2int.put(currField,avgSum.get(currField)/avgCount.get(currField));//更新当前avg
            }
            else
            {
                avgCount.put(currField, 1);
                avgSum.put(currField, curr);
                field2int.put(currField, curr);
            }
        }

        if(op==COUNT)
        {
            int size=field2int.size();
            if(field2int.containsKey(currField))
            {
                int pre=field2int.get(currField);//已有的count值
                field2int.put(currField,pre+1);
            }
            else
                field2int.put(currField,1);
        }
        if(op==VAR)//公式 EX^2-(EX)^2
        {
            if(field2int.containsKey(currField))
            {
                int size=avgCount.get(currField)+1;
                avgCount.put(currField,size);//更新当前group元素个数
                int pre=avgSum.get(currField);//之前的组内sum
                avgSum.put(currField,pre+curr);//更新当前组内sum
                varAvg.put(currField,avgSum.get(currField)/size);//更新当前期望
                pre=varRec.get(currField);//之前的平方和
                varRec.put(currField,pre+curr*curr);//更新平方和
                int EX=varAvg.get(currField);
                int EXX=varRec.get(currField)/size;
                field2int.put(currField,EXX-EX*EX);
            }
            else
            {
                avgCount.put(currField, 1);
                avgSum.put(currField, curr);
                varRec.put(currField,curr*curr);
                varAvg.put(currField,curr);//期望为curr
                field2int.put(currField, 0);//方差为0
            }
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
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (HashMap.Entry<Field, Integer> map : field2int.entrySet()) {
            Tuple t = new Tuple(td);
            if (gbfield==NO_GROUPING)
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
