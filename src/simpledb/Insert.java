package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {
	
	private TransactionId m_tid;
	private DbIterator m_child;
	private int m_tableid;
	private boolean m_inserted;
	
	private TupleDesc m_formattedTD; 

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        // some code goes here
    	m_tid = t;
    	m_child = child;
    	m_tableid = tableid;
    	m_inserted = false;
    	
    	String[] names = new String[] {"Inserted"};
    	Type[] types = new Type[] {Type.INT_TYPE};
    	m_formattedTD = new TupleDesc(types, names);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return m_formattedTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	m_child.open();
    	m_inserted = false;
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
        // some code goes here
    	if (m_inserted) {
    		return null;
    	}

    	int m_count = 0;
    	while (m_child.hasNext()) {
    		Tuple t = m_child.next();
    		try {
    			Database.getBufferPool().insertTuple(m_tid, m_tableid, t);
    		} catch (IOException e) {
    			throw new DbException("IO Error when inserting");
    		}
    		m_count++;
    	}
    	Tuple resultTuple = new Tuple(m_formattedTD);
    	resultTuple.setField(0, new IntField(m_count));
    	m_inserted = true;
        return resultTuple;
    }
}