package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;
    private boolean hasInsert;//记录是否已经插入
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
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t=t;
        this.child=child;
        this.tableId=tableId;
        //String域必须为null
        td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{null});

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException, InterruptedException {
        // some code goes here
        hasInsert=false;
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException, InterruptedException {
        // some code goes here
        hasInsert=false;
        child.rewind();
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
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, InterruptedException {
        // some code goes here
        if(hasInsert)
            return null;
        BufferPool bp=Database.getBufferPool();
        int count=0;
        while(child.hasNext())
        {
            Tuple tp=child.next();
            //父类没有抛出IOException，所以这里必须处理一下bp.insertTuple可能抛出的IOException
            try{
                bp.insertTuple(t,tableId,tp);
            }
            catch (IOException e){
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            count++;
        }
        Tuple ans=new Tuple(getTupleDesc());
        ans.setField(0,new IntField(count));
        hasInsert=true;
        return ans;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child=children[0];
    }
}
