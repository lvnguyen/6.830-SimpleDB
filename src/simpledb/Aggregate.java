package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {
	
	private DbIterator m_child;
	private int m_afield;
	private int m_gfield;
	private Aggregator.Op m_aop;
	private Aggregator m_agg;
	private DbIterator m_aggIterator;

    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
    	m_child = child;
    	m_afield = afield;
    	m_gfield = gfield;
    	m_aop = aop;
    	
    	Type gbType = (gfield == Aggregator.NO_GROUPING) ? null : child.getTupleDesc().getType(gfield);
    	Type aggType = child.getTupleDesc().getType(afield);
    	
    	switch (aggType) {
    	case INT_TYPE:
    		m_agg = new IntAggregator(gfield, gbType, afield, aop);
    		break;
    	case STRING_TYPE:
    		m_agg = new StringAggregator(gfield, gbType, afield, aop);
    		break;
    	default:
    		throw new IllegalArgumentException();
    	}
    	    	
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        // some code goes here
    	m_child.open();    	
    	while (m_child.hasNext()) {
    		m_agg.merge(m_child.next());
    	}
    	m_aggIterator = m_agg.iterator();
    	m_aggIterator.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (m_aggIterator.hasNext()) {
        	return m_aggIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	m_aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return m_child.getTupleDesc();
    }

    public void close() {
        // some code goes here
    	m_child.close();
    	m_aggIterator.close();
    }
}