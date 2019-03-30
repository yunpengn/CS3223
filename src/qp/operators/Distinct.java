package qp.operators;

import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;

/**
 * Defines a distinct operator which eliminates duplicates from input.
 */
public class Distinct extends Operator {
    // The project list (based on which we distinguish the duplicates).
    private final Vector projectList;
    // The base operator.
    private Operator base;
    // The sort operator applied on the base operator.
    private Sort sortedBase;
    // The number of buffers available
    private int numOfBuffer;

    /**
     * Creates a new distinct operator.
     *
     * @param base is the base operator.
     */
    public Distinct(Operator base, Vector projectList) {
        super(OpType.DISTINCT);
        this.base = base;
        this.projectList = projectList;
    }

    @Override
    public boolean open() {
        Attribute attribute = (Attribute) projectList.elementAt(0);
        sortedBase = new Sort(base, attribute, numOfBuffer);
        return sortedBase.open();
    }

    @Override
    public Batch next() {
        return base.next();
    }

    @Override
    public boolean close() {
        return base.close();
    }

    /**
     * Setter for numOfBuffer.
     *
     * @param numOfBuffer is the number of buffer pages available.
     */
    public void setNumOfBuffer(int numOfBuffer) {
        this.numOfBuffer = numOfBuffer;
    }

    /**
     * Getter for base.
     *
     * @return the base operator.
     */
    public Operator getBase() {
        return base;
    }

    /**
     * Setter for base.
     *
     * @param base is the base operator.
     */
    public void setBase(Operator base) {
        this.base = base;
    }
}
