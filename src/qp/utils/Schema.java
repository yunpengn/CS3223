package qp.utils;

import java.io.Serializable;
import java.util.Vector;

/**
 * Represents the schema of a table or query result. It is attached to every operator.
 */
public class Schema implements Serializable {
    // The attributes belonging to this schema.
    private Vector<Attribute> attributes;
    // The size (in bytes) of each tuple.
    private int tupleSize;

    /**
     * Creates a new schema with its attributes.
     *
     * @param attributes are the attributes of this schema.
     */
    public Schema(Vector<Attribute> attributes) {
        this.attributes = attributes;
    }

    /**
     * Setter for tuple size.
     *
     * @param size is the size (in bytes) of each tuple.
     */
    public void setTupleSize(int size) {
        tupleSize = size;
    }

    /**
     * Getter for tuple size.
     *
     * @return the size (in bytes) of each tuple.
     */
    public int getTupleSize() {
        return tupleSize;
    }

    /**
     * @return the number of columns in this schema.
     */
    public int getNumCols() {
        return attributes.size();
    }

    /**
     * Adds a new attribute to this schema.
     *
     * @param attr is the attribute to be added.
     */
    public void add(Attribute attr) {
        attributes.add(attr);
    }

    /**
     * Retrieves the attribute at a given index. O(1) operation.
     *
     * @param i is the index of the required attribute.
     * @return the attribute at that index.
     */
    public Attribute getAttribute(int i) {
        return attributes.elementAt(i);
    }

    /**
     * Checks the index of a given attribute. O(n) operation.
     *
     * @param attr is the attribute to be checked.
     * @return the index of the attribute.
     */
    public int indexOf(Attribute attr) {
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes.elementAt(i).equals(attr)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Checks whether a certain attribute exists in the schema and retrieves its getData type.
     *
     * @param target is the attribute to be checked.
     * @return the getData type of the attribute if exists; -1 otherwise.
     */
    public int typeOf(Attribute target) {
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = attributes.elementAt(i);
            if (attr.equals(target)) {
                return attr.getType();
            }
        }
        return -1;
    }

    /**
     * Retrieves the getData type of the attribute at a given index.
     *
     * @param attrAt is the index of the attribute.
     * @return the getData type of the attribute.
     */
    public int typeOf(int attrAt) {
        return attributes.elementAt(attrAt).getType();
    }

    /**
     * Checks whether given attribute is present in this schema.
     *
     * @param target is the index of the attribute.
     * @return true if it exists.
     */
    public boolean contains(Attribute target) {
        for (Attribute attr: attributes) {
            if (attr.equals(target)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Calculates the schema of resultant join operation. It does not consider the elimination
     * of duplicate columns.
     *
     * @param right is the other schema to be joined with.
     * @return the resultant schema.
     */
    public Schema joinWith(Schema right) {
        Vector<Attribute> newAttributes = new Vector<>(attributes);
        newAttributes.addAll(right.attributes);

        Schema Schema = new Schema(newAttributes);
        Schema.setTupleSize(getTupleSize() + right.getTupleSize());
        return Schema;
    }

    /**
     * Gets a sub-schema with a given list of attributes only. This is useful for project operator.
     *
     * @param attrList is the list of attributes to be included.
     * @return the sub-schema.
     */
    public Schema subSchema(Vector attrList) {
        Vector<Attribute> newVec = new Vector<>();
        int newTupleSize = 0;

        for (int i = 0; i < attrList.size(); i++) {
            Attribute resAttr = (Attribute) attrList.elementAt(i);
            int baseIndex = this.indexOf(resAttr);
            Attribute baseAttr = this.getAttribute(baseIndex);
            newVec.add(baseAttr);
            newTupleSize = newTupleSize + baseAttr.getAttrSize();
        }

        Schema newSchema = new Schema(newVec);
        newSchema.setTupleSize(newTupleSize);
        return newSchema;
    }

    /**
     * Creates a new copy of the schema.
     *
     * @return the copy of this schema.
     */
    @Override
    public Object clone() {
        Vector<Attribute> newAttr = new Vector<>();
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = (Attribute) attributes.elementAt(i).clone();
            newAttr.add(attr);
        }

        Schema newSchema = new Schema(newAttr);
        newSchema.setTupleSize(tupleSize);
        return newSchema;
    }
}
