package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
	
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    
    public static final int DEFAULT_PAGES = 50;
    private HashMap<PageId, Page> m_cache;
    private HashMap<PageId, RWLock> m_lock;
    private int m_maxNumPages;      

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	m_cache = new HashMap<PageId, Page>();
    	m_lock = new HashMap<PageId, RWLock>();
    	
    	m_maxNumPages = numPages;
    }
    
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	
    	synchronized (this) {
    		if (!m_lock.containsKey(pid)) {
    			m_lock.put(pid, new RWLock());
    		}
    	}
    	
    	// Block until lock is acquired
    	while (!m_lock.get(pid).acquireLock(tid, perm));
    	
        if (m_cache.containsKey(pid)) {
        	return m_cache.get(pid);
        } else {
        	if (m_cache.size() >= m_maxNumPages) {
        		evictPage();
        	}        	
    		DbFile dbfile = Database.getCatalog().getDbFile(pid.getTableId());
    		Page newPage = dbfile.readPage(pid);
    		m_cache.put(pid, newPage);
    		return newPage;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	m_lock.get(pid).releaseLock(tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public  void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	for (PageId pid : m_cache.keySet()) {
    		releasePage(tid, pid);
    	}
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public   boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	return m_lock.get(pid).holdsLock(tid);    			
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public   void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile getFile = Database.getCatalog().getDbFile(tableId);
    	ArrayList<Page> modifiedPages = getFile.addTuple(tid, t);    
    	for (Page p : modifiedPages) {
    		p.markDirty(true, tid);
    		m_cache.put(p.getId(), p);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile getFile = Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
    	Page modifiedPage = getFile.deleteTuple(tid, t);
    	modifiedPage.markDirty(true, tid);
    	m_cache.put(modifiedPage.getId(), modifiedPage);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	for (PageId pid : m_cache.keySet()) {
    		flushPage(pid);
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	
    	// Put the page into disk, mark dirty to be false
    	Page flushPage = m_cache.get(pid);
    	TransactionId dirtyTID = flushPage.isDirty();
    	if (dirtyTID != null) {
    		DbFile getFile = Database.getCatalog().getDbFile(pid.getTableId());
    		getFile.writePage(flushPage);
    		flushPage.markDirty(false, dirtyTID);
    	}    	
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	boolean pageEvicted = false;
    	for (PageId pid : m_cache.keySet()) {
    		try {
    			flushPage(pid);
    			m_cache.remove(pid);
    			pageEvicted = true;
    			break;
    		} catch (IOException e) {
    			throw new DbException("Could not evict this page");
    		}
    	}
    	
    	if (!pageEvicted) {
    		throw new DbException("No page evicted");
    	}
    }
}