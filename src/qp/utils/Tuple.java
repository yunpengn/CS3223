package qp.utils;

import java.io.Serializable;
import java.util.Date;
import java.util.Vector;

/**
 * Represents a tuple (i.e., a row).
 */
public class Tuple implements Serializable {
    // The data of this tuple.
    private Vector _data;

    /**
     * Creates a new tuple.
     *
     * @param d is the data in this tuple.
     */
    public Tuple(Vector d) {
        _data = d;
    }

    /**
     * Getter for data.
     *
     * @return the data.
     */
    public Vector getData() {
        return _data;
    }

    /**
     * Getters the data at a given index.
     *
     * @param index is the index.
     * @return the data at the given index.
     */
    public Object dataAt(int index) {
        return _data.elementAt(index);
    }

    /**
     * Checks whether the join condition is satisfied. This should be called before performing
     * actual join operation. Notice: JOIN means EQUAL operator.
     *
     * @param right is the other tuple to be joined with.
     * @param leftIndex is the index of the join attribute in the left table.
     * @param rightIndex is the index of the join attribute in the right table.
     * @return true if the join condition is satisfied
     */
    public boolean checkJoin(Tuple right, int leftIndex, int rightIndex) {
        Object leftData = dataAt(leftIndex);
        Object rightData = right.dataAt(rightIndex);

        return leftData.equals(rightData);
    }

    /**
     * Joins two tuples Without duplicate column elimination.
     *
     * @param right is the other tuple to be joined with.
     * @return the joined tuple
     */
    public Tuple joinWith(Tuple right) {
        Vector newData = new Vector(this.getData());
        newData.addAll(right.getData());
        return new Tuple(newData);
    }

    /**
     * Compare two tuples in the same table on the same given attribute.
     *
     * @param left is the left tuple.
     * @param right is the right tuple
     * @param index is the index of the attribute to be compared.
     * @return the comparision result.
     */
    public static int compareTuples(Tuple left, Tuple right, int index) {
        return compareTuples(left, right, index, index);
    }

    /**
     * Compare two tuples in different tables on two given attributes.
     *
     * @param left is the left tuple.
     * @param right is the right tuple.
     * @param leftIndex is the index of the attribute from the left tuple.
     * @param rightIndex is the index of the attribute from the right tuple.
     * @return the comparision result.
     */
    private static int compareTuples(Tuple left, Tuple right, int leftIndex, int rightIndex) {
        Object leftValue = left.dataAt(leftIndex);
        Object rightValue = right.dataAt(rightIndex);

        if (leftValue instanceof Integer) {
            return ((Integer) leftValue).compareTo((Integer) rightValue);
        } else if (leftValue instanceof String) {
            return ((String) leftValue).compareTo((String) rightValue);
        } else if (leftValue instanceof Float) {
            return ((Float) leftValue).compareTo((Float) rightValue);
        } else if (leftValue instanceof Date) {
            return ((Date) leftValue).compareTo((Date) rightValue);
        } else {
            System.out.println("Tuple: Unknown comparision of the tuples");
            System.exit(1);
            return 0;
        }
    }
}
