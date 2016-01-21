package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	
	private int m_gbfield;
	private Type m_gbfieldtype;
	private int m_afield;
	private Op m_op;
	private TupleDesc m_td;
	
	private HashMap<Field, Integer> m_count;
	
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	m_gbfield = gbfield;
    	m_gbfieldtype = gbfieldtype;
    	m_afield = afield;
    	m_op = what;
    	m_count = new HashMap<Field, Integer>();
    	
    	assert (m_op == Op.COUNT);
    	m_td = createTupleDesc();
    }
    
	private TupleDesc createTupleDesc() {
		String[] fieldNames;
		Type[] fieldTypes;
		
		if (m_gbfield == NO_GROUPING) {
			fieldNames = new String[] {"aggregate values"};
			fieldTypes = new Type[] {Type.INT_TYPE};
		}
		else {
			fieldNames = new String[] {"groupby values", "aggregate values"};
			fieldTypes = new Type[] {m_gbfieldtype, Type.INT_TYPE};
		}
		return new TupleDesc(fieldTypes, fieldNames);
	}

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        Field key = (m_gbfield == NO_GROUPING) ? null : tup.getField(m_gbfield);
        if (!m_count.containsKey(key)) {
        	m_count.put(key, 0);
        }
        
        int cur_value = m_count.get(key);
        m_count.put(key, cur_value + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        for (Field key : m_count.keySet()) {
        	Tuple nextTuple = new Tuple(m_td);
        	Field recvField = new IntField(m_count.get(key));
        	
        	if (m_gbfield == NO_GROUPING) {
        		nextTuple.setField(0, recvField);
        	} else {
        		nextTuple.setField(0, key);
        		nextTuple.setField(1, recvField);
        	}
        	tuples.add(nextTuple);
        }
        return new TupleIterator(m_td, tuples); 
    }

}
