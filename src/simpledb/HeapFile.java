package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
	
	private int m_id;
	private File m_f;
	private TupleDesc m_td;
	
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	m_f = f;
    	m_id = f.getAbsolutePath().hashCode();
    	m_td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return m_f;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        // some code goes here
        return m_id;
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	// some code goes here
    	return m_td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
        	RandomAccessFile f = new RandomAccessFile(m_f, "r");
        	int offset = BufferPool.PAGE_SIZE * pid.pageno();
        	byte[] data = new byte[BufferPool.PAGE_SIZE];
        	
        	if (offset + BufferPool.PAGE_SIZE > f.length()) {
        		System.exit(1);
        	}
        	
        	f.seek(offset);
        	f.readFully(data);
        	f.close();
        	return new HeapPage((HeapPageId) pid, data);
        } catch (FileNotFoundException e) {
        	throw new IllegalArgumentException();
        } catch (IOException e) {
        	throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil((double) m_f.length()/BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
    
    private class HeapFileIterator implements DbFileIterator { 
    	private Iterator<Tuple> m_tuple;
    	private int m_currentPageNo;
    	private int m_tableid;
    	private TransactionId m_tid;
    	private HeapFile m_hf;
    	
    	private Iterator<Tuple> openInternal() throws DbException, TransactionAbortedException {
    		HeapPageId pid = new HeapPageId(m_tableid, m_currentPageNo);
    		HeapPage p = (HeapPage) Database.getBufferPool().getPage(m_tid, pid, Permissions.READ_ONLY);
    		return p.iterator();
    	}
    	
    	public HeapFileIterator(HeapFile hf, TransactionId tid) {
    		m_hf = hf;
    		m_tid = tid;
    		m_tableid = hf.getId();
    		m_currentPageNo = 0;
    	}
    	
    	public void open() throws DbException, TransactionAbortedException {
    		m_tuple = openInternal();
    	}    	
    	
    	public boolean hasNext() throws DbException,
        								TransactionAbortedException {
    		if (m_tuple == null) {
    			return false;
    		} else if (m_tuple.hasNext()) {
    			return true;
    		} else {
    			m_currentPageNo++;
    			if (m_currentPageNo < m_hf.numPages()) {
    				m_tuple = openInternal();
    				return m_tuple.hasNext();
    			} else {
    				return false;
    			}
    		}
    	}
    	
    	public Tuple next() throws DbException,
    							   TransactionAbortedException {
    		if (hasNext()) {
    			return m_tuple.next();
    		} else {
    			throw new NoSuchElementException();
    		}
    	}
    	
    	public void rewind() throws DbException,
    								TransactionAbortedException {
    		m_currentPageNo = 0;
    		m_tuple = openInternal();    		
    	}
    	
    	public void close() {
    		m_tuple = null;
    	}
    }
}