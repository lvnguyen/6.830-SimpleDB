package simpledb;

import java.util.*;

public class HeapPageIterator implements Iterator<Tuple> {
	
	private HeapPage m_heappage;
	private int m_numTuples;
	private int m_curTuple;
	
	public HeapPageIterator(HeapPage hp) {
		m_heappage = hp;
		m_numTuples = hp.getNumAvailableTuples();
		m_curTuple = 0;
	}
	
	public boolean hasNext() {
		return (m_curTuple < m_numTuples);
	}
	
	public Tuple next() {
		return m_heappage.tuples[m_curTuple++];
	}
}