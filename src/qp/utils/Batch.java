package qp.utils;

import java.io.Serializable;
import java.util.Vector;

/**
 * Represents a batch of records, which is an abstraction of the physical "page" concept in hard disk.
 */
public class Batch implements Serializable {
    // The number of bytes per page.
    private static int pageSize;

    // The number of tuples per page.
    private int maxSize;
    // Stores the tuples in this page.
    private Vector<Tuple> tuples;

    /**
     * Creates a new batch.
     *
     * @param numOfTuples is the number of tuples in this batch.
     */
    public Batch(int numOfTuples) {
        maxSize = numOfTuples;
        tuples = new Vector<>(maxSize);
    }

    /**
     * Setter for page size (in bytes).
     *
     * @param size is the page size (in bytes).
     */
    public static void setPageSize(int size) {
        pageSize = size;
    }

    /**
     * Getter for page size.
     *
     * @return the page size (in bytes).
     */
    public static int getPageSize() {
        return pageSize;
    }

    /**
     * Inserts a new tuple in this batch at the next free location.
     *
     * @param t is the new tuple to be inserted.
     */
    public void add(Tuple t) {
        tuples.add(t);
    }

    /**
     * @return the maximum number of tuples this batch can store.
     */
    public int capacity() {
        return maxSize;

    }

    /**
     * @return the number of tuples that are currently being stored in this batch.
     */
    public int size() {
        return tuples.size();
    }

    /**
     * Checks whether the number of tuples currently being stored in this batch already reaches the maximum size.
     *
     * @return true if this batch is already full.
     */
    public boolean isFull() {
        return size() == capacity();
    }

    /**
     * @return true if this batch does not contain any tuple.
     */
    public boolean isEmpty() {
        return tuples.isEmpty();
    }

    /**
     * Checks whether this page contains a certain tuple.
     *
     * @param t is the tuple to be checked.
     * @return true if it contains this tuple.
     */
    public boolean contains(Tuple t) {
        return tuples.contains(t);
    }

    /**
     *  Retrieves the element at a certain index in this page. O(1) operation.
     *
     * @param i is the index of this element.
     * @return the element at the given index.
     */
    public Tuple elementAt(int i) {
        return tuples.elementAt(i);
    }

    /**
     * Removes the tuple at a given index.
     *
     * @param i is the index.
     */
    public void remove(int i) {
        tuples.remove(i);
    }

    /**
     * Getter for tuples.
     *
     * @return the underlying vector storing all tuples.
     */
    public Vector<Tuple> getTuples() {
        return tuples;
    }
}
