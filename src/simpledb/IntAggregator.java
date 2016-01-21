package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {
	
	private int m_gbfield;
	private Type m_gbtype;
	private int m_afield;
	private Op m_op;
	
	HashMap<Field, Integer> m_aggregateData;
	HashMap<Field, Integer> m_count;
	TupleDesc m_td;
	
	private TupleDesc createTupleDesc() {
		String[] fieldNames;
		Type[] fieldTypes;
		
		if (m_gbfield == NO_GROUPING) {
			fieldNames = new String[] {"aggregate values"};
			fieldTypes = new Type[] {Type.INT_TYPE};
		}
		else {
			fieldNames = new String[] {"groupby values", "aggregate values"};
			fieldTypes = new Type[] {m_gbtype, Type.INT_TYPE};
		}
		return new TupleDesc(fieldTypes, fieldNames);
	}
	
    /**
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * Aggregate constructor
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	// some code goes here
    	m_gbfield = gbfield;
    	m_gbtype = gbfieldtype;
    	m_afield = afield;
    	m_op = what;
    	m_aggregateData = new HashMap<Field, Integer>();
    	m_count = new HashMap<Field, Integer>();    	
    	m_td = createTupleDesc();
    }
    
    private int setInitData() {
    	switch (m_op) {
    		case MIN: return Integer.MAX_VALUE;
    		case MAX: return Integer.MIN_VALUE;
    		case SUM: case COUNT: case AVG: return 0;
    		default: throw new IllegalArgumentException();
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // some code goes here
    	Field key = (m_gbfield == NO_GROUPING) ? null : tup.getField(m_gbfield);
    	if (!m_aggregateData.containsKey(key)) {
    		m_aggregateData.put(key, setInitData());
    		m_count.put(key, 0);
    	}
    	 
    	int tup_value = ((IntField) tup.getField(m_afield)).getValue();
    	int cur_value = m_aggregateData.get(key);
    	int count_value = m_count.get(key);
    	
    	switch (m_op) {
    	case MIN:
    		cur_value = Math.min(cur_value, tup_value);
    		break;
    	case MAX:
    		cur_value = Math.max(cur_value, tup_value);
    		break;
    	case SUM:
    		cur_value += tup_value;
    		break;
    	case COUNT:
    		count_value++;
    		break;
    	case AVG:
    		count_value++;
    		cur_value += tup_value;
    		break;
    	default:
    		throw new IllegalArgumentException();
    	}
    	
    	m_aggregateData.put(key, cur_value);
    	m_count.put(key, count_value);
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
        for (Field key : m_aggregateData.keySet()) {
        	Tuple nextTuple = new Tuple(m_td);
        	int recvValue;
        	
        	switch (m_op) {
        	case MIN: case MAX: case SUM:
        		recvValue = m_aggregateData.get(key);
        		break;
        	case COUNT:
        		recvValue = m_count.get(key);
        		break;
        	case AVG:
        		recvValue = m_aggregateData.get(key) / m_count.get(key);
        		break;
        	default:
        		recvValue = setInitData();  // shouldn't reach here
        	}
        	
        	Field recvField = new IntField(recvValue);
        	if (m_gbfield == NO_GROUPING) {
        		nextTuple.setField(0, recvField);
        	}
        	else {
        		nextTuple.setField(0, key);
        		nextTuple.setField(1, recvField);
        	}
        	tuples.add(nextTuple);
        }
        return new TupleIterator(m_td, tuples);
    }

}