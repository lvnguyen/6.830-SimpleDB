package simpledb;

import java.util.HashSet;

public class RWLock {
	private TransactionId exclusiveId;
	private HashSet<TransactionId> sharedId;
	
	public RWLock() {
		exclusiveId = null;
		sharedId = new HashSet<TransactionId>();
	}
	
	// Return true if lock is granted
	public synchronized boolean acquireLock(TransactionId tid, Permissions perm) {
		// If perm is read only and tid has already acquired a lock, do nothing
		if (perm == Permissions.READ_ONLY) {
			if (tid.equals(exclusiveId) || sharedId.contains(tid)) {
				return true;
			}
		}
		else {
			// If perm is read/write, remove all previous locks
			if (tid.equals(exclusiveId)) {
				return true;
			}
			if (sharedId.contains(tid)) {
				sharedId.remove(tid);
			}
		}
		
		// If perm is read lock, check if no one is claiming exclusive lock
		if (perm == Permissions.READ_ONLY) {
			if (exclusiveId == null) {
				sharedId.add(tid);
				return true;
			}
			return false;
		}
		else {
			// If perm is write lock, check if no one is using shared lock
			if (sharedId.isEmpty()) {
				exclusiveId = tid;
				return true;
			}
			return false;
		}
	}
	
	public synchronized void releaseLock(TransactionId tid) {
		if (tid.equals(exclusiveId)) {
			exclusiveId = null;
		}
		if (sharedId.contains(tid)) {
			sharedId.remove(tid);
		}
	}
	
	public synchronized boolean holdsLock(TransactionId tid) {
		return tid.equals(exclusiveId) || sharedId.contains(tid);
	}
}