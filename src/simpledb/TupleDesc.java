package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
	
	private class TDItem {
		private final Type m_fieldType;
		private final String m_fieldName;
		
		public TDItem(Type fieldType, String fieldName) {
			this.m_fieldType = fieldType;
			this.m_fieldName = fieldName;
		}
		
		public Type getType() {
			return this.m_fieldType;
		}
		
		public String getName(){
			return this.m_fieldName;
		}
		
		public String toString() {
			return "(" + m_fieldType.toString() + "," + m_fieldName + ")";
		}
	}
	
	private ArrayList<TDItem> m_tditems;
	
    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int newsize = td1.numFields() +td2.numFields();
        Type[] typeAr = new Type[newsize];
        String[] stringAr = new String[newsize];
        
        for (int i = 0; i < td1.numFields(); i++) {
        	typeAr[i] = td1.getType(i);
        	stringAr[i] = td1.getFieldName(i);
        }
        
        for (int i = 0; i < td2.numFields(); i++) {
        	typeAr[td1.numFields() + i] = td2.getType(i);
        	stringAr[td1.numFields() + i] = td2.getFieldName(i);
        }
        
        return new TupleDesc(typeAr, stringAr);
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	this.m_tditems = new ArrayList<TDItem>();
    	for (int i = 0; i < typeAr.length; i++) {
    		this.m_tditems.add(new TDItem(typeAr[i], fieldAr[i]));
    	}
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this.m_tditems = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
        	this.m_tditems.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.m_tditems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > numFields()) {
        	throw new NoSuchElementException();
        }
        return this.m_tditems.get(i).getName();
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
        	throw new NoSuchElementException();
        }
        for (int i = 0; i < this.m_tditems.size(); i++) {
        	if (name.equals(this.m_tditems.get(i).getName())) {
        		return i;
        	}
        }
        throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields()) {
        	throw new NoSuchElementException();
        }
        return this.m_tditems.get(i).getType();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item : this.m_tditems) {
        	size += item.getType().getLen();
        }
        return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (!(o instanceof TupleDesc)) {
        	return false;
        }
        TupleDesc t_o = (TupleDesc) o;
        if (this.numFields() != t_o.numFields()) {
        	return false;
        }
        for (int i = 0; i < m_tditems.size(); i++) {
        	Type thisType = this.getType(i);
        	Type otherType = t_o.getType(i);
        	if (!thisType.equals(otherType)) {
        		return false;
        	}
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return 0;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String sb = "";
        for (TDItem td : this.m_tditems) {
        	sb += td.toString();
        }
        return sb;
    }
}
