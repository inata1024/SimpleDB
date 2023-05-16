package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;
    private boolean hasDelete;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.t=t;
        this.child=child;
        //String域必须为null
        td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{null});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException, InterruptedException {
        // some code goes here
        hasDelete=false;
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException, InterruptedException {
        // some code goes here
        child.rewind();
        hasDelete=false;//插入可以rewind，delete可以吗
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
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, InterruptedException {
        // some code goes here
        if(hasDelete)
            return null;
        BufferPool bp=Database.getBufferPool();
        int count=0;
        while(child.hasNext())
        {
            Tuple tp=child.next();
            //父类没有抛出IOException，所以这里必须处理一下bp.insertTuple可能抛出的IOException
            try{
                bp.deleteTuple(t,tp);
            }
            catch (IOException e){
                e.printStackTrace();
            }
            count++;
        }
        Tuple ans=new Tuple(getTupleDesc());
        ans.setField(0,new IntField(count));
        hasDelete=true;
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
