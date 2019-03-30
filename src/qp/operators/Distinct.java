package qp.operators;

import java.util.Vector;

import qp.utils.Batch;

/**
 * Defines a distinct operator which eliminates duplicates from input.
 */
public class Distinct extends Operator {
    // The project list (based on which we distinguish the duplicates).
    private final Vector projectList;
    // The base operator.
    private Operator base;
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
        return base.open();
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
