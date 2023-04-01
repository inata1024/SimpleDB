package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;
        /**
         * implements Serializable：类对象存储时可序列化
         * serialVersionUID：验证序列化和反序列化的过程中,对象是否保持一致
         * final：相当于const
         **/

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        public String toString_() {
            return fieldType + "(" + fieldName + ")";
        }//用于TupleDesc类的toString

    }


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new TDItemIterator();
    }

    private class TDItemIterator implements Iterator<TDItem> {
        private int pos = 0;
        @Override
        public boolean hasNext() {
            return TDItems.length > pos;
        }
        @Override
        public TDItem next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return TDItems[pos++];
        }
    }
    private static final long serialVersionUID = 1L;

    private TDItem[] TDItems;//储存TDItem的数组

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        TDItems=new TDItem[typeAr.length];
        for(int i=0;i<typeAr.length;i++)
        {
            TDItems[i]=new TDItem(typeAr[i],fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        TDItems=new TDItem[typeAr.length];
        for(int i=0;i<typeAr.length;i++)
        {
            TDItems[i]=new TDItem(typeAr[i],null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return TDItems.length;

    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i >= 0 && i < TDItems.length)
            return TDItems[i].fieldName;
        else
            throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i >= 0 && i < TDItems.length)
            return TDItems[i].fieldType;
        else
            throw new NoSuchElementException();
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if(name==null)
            throw new NoSuchElementException();
        int ans=0;
        //String判等：==内存判等，equals()值判等
        while(ans<TDItems.length && !name.equals(TDItems[ans].fieldName))
            ans++;
        if(ans<TDItems.length)
            return ans;
        else
            throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int ans=0;
        for(int i=0;i<TDItems.length;i++)
            ans+=TDItems[i].fieldType.getLen();
        return ans;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int l=td1.numFields()+td2.numFields();
        String[] nameAr=new String[l];
        Type[] typeAr=new Type[l];
        for(int i=0;i<td1.numFields();i++) {
            nameAr[i] = td1.getFieldName(i);
            typeAr[i]=td1.getFieldType(i);
        }
        for(int i=td1.numFields();i<l;i++) {
            nameAr[i] = td2.getFieldName(i-td1.numFields());
            typeAr[i]=td2.getFieldType(i-td1.numFields());
        }
        return new TupleDesc(typeAr,nameAr);

    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(this==o){
            return true;
        }
        if(o instanceof TupleDesc){
            TupleDesc t = (TupleDesc) o;
            if(this.numFields()!=t.numFields())
                return false;
            for(int i=0;i<this.numFields();i++)
            {
                if(TDItems[i].fieldType!=t.getFieldType(i))
                    return false;
            }
            return true;
        }

        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        //重写equal()与hashcode() 参考：https://blog.csdn.net/qq_35387940/article/details/107757341
        int result = 17;
        String str="";
        for(int i=0;i<numFields();i++)
            str+=((Integer)TDItems[i].fieldType.getLen()).toString();

        result = 31 * result + str.hashCode();//每个type相同
        result = 31 * result + numFields();//长度相同
        return result;
        //throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String ans="";
        for(int i=0;i<TDItems.length-1;i++)
        {
            ans+=TDItems[i].toString_();
            ans+=",";
        }
        ans+=TDItems[TDItems.length-1].toString_();
        return ans;
    }
}
