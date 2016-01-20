package simpledb;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
	
	private TransactionId m_tid;
	private int m_tableid;
	private String m_tableAlias;
	private DbFile m_dbfile;
	private DbFileIterator m_iterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
    	m_tid = tid;
    	m_tableid = tableid;
    	m_tableAlias = tableAlias;
    	m_dbfile = Database.getCatalog().getDbFile(tableid);
    	m_iterator = m_dbfile.iterator(tid);
    }

    public void open()
        throws DbException, TransactionAbortedException {
        // some code goes here
    	m_iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc origTuple = Database.getCatalog().getTupleDesc(m_tableid);
        int tdsize = origTuple.numFields();
        
        Type[] newTypes = new Type[tdsize];
        String[] newFields = new String[tdsize];
        
        for (int i = 0; i < tdsize; i++) {
        	newTypes[i] = origTuple.getType(i);
        	newFields[i] = m_tableAlias + "." + origTuple.getFieldName(i);
        }
        return new TupleDesc(newTypes, newFields);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return m_iterator.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        return m_iterator.next();
    }

    public void close() {
        // some code goes here
    	m_iterator.close();
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
    	m_iterator.rewind();
    }
}
