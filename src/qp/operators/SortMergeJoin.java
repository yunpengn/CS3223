package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class SortMergeJoin extends Join {
    // The number of tuples per output batch.
    private int batchSize;

    // The buffer for the left input stream.
    private Batch leftBatch;
    // The buffer for the right input stream.
    private Batch rightBatch;
    // The output buffer.
    private Batch outBatch;

    // Index of the join attribute in left table
    private int leftIndex;
    // Index of the join attribute in right table
    private int rightIndex;

    // Cursor for left side buffer
    private int leftCursor;
    // Cursor for right side buffer
    private int rightCursor;
    // Whether end of stream is reached for the left table
    private boolean eosLeft;
    // Whether end of stream is reached for the right table
    private boolean eosRight;

    /**
     * Instantiates a new join operator using block-based nested loop algorithm.
     *
     * @param jn is the base join operator.
     */
    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        joinType = jn.getJoinType();
        numOfBuffer = jn.getNumOfBuffer();
    }

    /**
     * Opens this operator by performing the following operations:
     * 1. Sorts the left & right relation with external sort;
     * 2. Stores the sorted relations from both sides into files;
     * 3. Opens the connections.
     *
     * @return true if the operator is opened successfully.
     */
    @Override
    public boolean open() {
        // Sorts the left relation.
        left.open();

        // Selects the number of tuples per page based tuple size.
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        // Gets the join attribute from left & right table.
        Attribute leftAttr = con.getLeft();
        Attribute rightAttr = (Attribute) con.getRight();
        leftIndex = left.getSchema().indexOf(leftAttr);
        rightIndex = right.getSchema().indexOf(rightAttr);

        // Initializes the cursors of input buffers for both sides.
        leftCursor = 0;
        rightCursor = 0;
        eosLeft = false;
        eosRight = true;

        return super.open();
    }

    /**
     * Selects tuples satisfying the join condition from input buffers and returns.
     *
     * @return the next page of output tuples.
     */
    @Override
    public Batch next() {
        // Returns empty if the left table reaches end-of-stream.
        if (eosLeft) {
            close();
            return null;
        }

        outBatch = new Batch(batchSize);
        while (!outBatch.isFull()) {
            // Checks whether we need to read a new page from the left table.
            if (leftCursor == 0 && eosRight) {
                // Fetches a new page from left table.
                leftBatch = left.next();
                if (leftBatch == null) {
                    eosLeft = true;
                    return outBatch;
                }
                right.open();
                eosRight = false;
            }

            // Continuously probe the right table until we hit the end-of-stream.
            while (!eosRight) {
                if (leftCursor == 0 && rightCursor == 0) {
                    rightBatch = right.next();
                    if (rightBatch == null) {
                        eosRight = true;
                        break;
                    }
                }

                for (int i = leftCursor; i < leftBatch.size(); i++) {
                    for (int j = rightCursor; j < rightBatch.size(); j++) {
                        Tuple leftTuple = leftBatch.elementAt(i);
                        Tuple rightTuple = rightBatch.elementAt(j);

                        // Adds the tuple if satisfying the join condition.
                        if (leftTuple.checkJoin(rightTuple, leftIndex, rightIndex)) {
                            Tuple outTuple = leftTuple.joinWith(rightTuple);
                            outBatch.add(outTuple);

                            // Checks whether the output buffer is full.
                            if (outBatch.isFull()) {
                                if (i == leftBatch.size() - 1 && j == rightBatch.size() - 1) {
                                    leftCursor = 0;
                                    rightCursor = 0;
                                } else if (i != leftBatch.size() - 1 && j == rightBatch.size() - 1) {
                                    leftCursor = i + 1;
                                    rightCursor = 0;
                                } else {
                                    leftCursor = i;
                                    rightCursor = j + 1;
                                }

                                // Returns since we have already produced a complete page of matching tuples.
                                return outBatch;
                            }
                        }
                    }
                    rightCursor = 0;
                }
                leftCursor = 0;
            }
        }
        return outBatch;
    }

    /**
     * Closes this operator.
     *
     * @return true if the operator is closed successfully.
     */
    @Override
    public boolean close() {
        return true;
    }
}
