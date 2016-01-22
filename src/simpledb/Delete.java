package simpledb;

import java.io.IOException;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {
	
	private TransactionId m_tid;
	private DbIterator m_child;
	private TupleDesc m_tddesc;
	private boolean m_deleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	m_tid = t;
    	m_child = child;
    	
    	String[] names = new String[] {"Deleted"};
    	Type[] types = new Type[] {Type.INT_TYPE};
    	m_tddesc = new TupleDesc(types, names);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return m_tddesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	m_child.open();
    	m_deleted = false;
    }

    public void close() {
        // some code goes here
    	m_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	m_child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (m_deleted) {
        	return null;
        }
        
        int m_count = 0;
        while (m_child.hasNext()) {
        	Tuple t = m_child.next();        	
        	Database.getBufferPool().deleteTuple(m_tid, t);
        	m_count++;
        }
        Tuple resultTuple = new Tuple(m_tddesc);
        resultTuple.setField(0, new IntField(m_count));
        m_deleted = true;
        return resultTuple;
    }
}