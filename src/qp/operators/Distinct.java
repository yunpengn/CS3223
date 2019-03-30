package qp.operators;

import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

/**
 * Defines a distinct operator which eliminates duplicates from input.
 */
public class Distinct extends Operator {
    // The project list (based on which we distinguish the duplicates).
    private final Vector projectList;
    // The indices of all attributes in the projectList.
    private Vector<Integer> projectIndices = new Vector<>();
    // The base operator.
    private Operator base;
    // The sort operator applied on the base operator.
    private Sort sortedBase;
    // The number of tuples per batch.
    private int batchSize;
    // The number of buffers available
    private int numOfBuffer;
    // Records whether we have reached end-of-stream.
    private boolean eos = false;
    // The input batch.
    private Batch inBatch = null;
    // The index for the current element being read from input batch.
    private int inIndex = 0;
    // The last tuple being outputted.
    private Tuple lastOutTuple = null;

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
        batchSize = Batch.getPageSize() / schema.getTupleSize();
        for (int i = 0; i < projectList.size(); i++) {
            Attribute attribute = (Attribute) projectList.elementAt(i);
            projectIndices.add(schema.indexOf(attribute));
        }

        sortedBase = new Sort(base, projectList, numOfBuffer);
        return sortedBase.open();
    }

    @Override
    public Batch next() {
        if (eos) {
            close();
            return null;
        } else if (inBatch == null) {
            inBatch = sortedBase.next();
        }

        Batch outBatch = new Batch(batchSize);
        while (!outBatch.isFull()) {
            if (inBatch == null || inBatch.size() <= inIndex) {
                eos = true;
                break;
            }

            Tuple current = inBatch.elementAt(inIndex);
            if (lastOutTuple == null || checkTuplesNotEqual(lastOutTuple, current)) {
                outBatch.add(current);
                lastOutTuple = current;
            }
            inIndex++;

            if (inIndex == batchSize) {
                inBatch = sortedBase.next();
                inIndex = 0;
            }
        }

        return outBatch;
    }

    /**
     * Checks whether two tuples are not equal.
     *
     * @param tuple1 is the first tuple.
     * @param tuple2 is the second tuple.
     * @return true if they are not equal.
     */
    private boolean checkTuplesNotEqual(Tuple tuple1, Tuple tuple2) {
        for (int index: projectIndices) {
            int result = Tuple.compareTuples(tuple1, tuple2, index);
            if (result != 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean close() {
        return sortedBase.close();
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
