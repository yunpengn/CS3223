package qp.operators;

import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class SortMergeJoin extends Join {
    // The number of tuples per output batch.
    private int batchSize;

    // Index of the join attribute in left table.
    private int leftIndex;
    // Index of the join attribute in right table.
    private int rightIndex;
    // Type of the join attribute.
    private int attrType;

    // The buffer for the left input stream.
    private Batch leftBatch;
    // The buffer for the right input stream.
    private Batch rightBatch;

    // The tuple that is currently being processed from left input batch.
    private Tuple leftTuple = null;

    // Cursor for left side buffer.
    private int leftCursor = 0;
    // Cursor for right side buffer.
    private int rightCursor = 0;

    // The right partition that is currently being joined in.
    private Vector<Tuple> rightPartition = new Vector<>();
    // The index of the tuple that is currently being processed in the current right partition (0-based).
    private int rightPartitionIndex = 0;
    // The next right tuple (i.e., the first element of the next right partition).
    private Tuple nextRightTuple = null;

    // Whether end of stream is reached for the left table.
    private boolean eosLeft = false;
    // Whether end of stream is reached for the right table.
    private boolean eosRight = false;

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
        right.open();

        // Selects the number of tuples per page based tuple size.
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        // Gets the join attribute from left & right table.
        Attribute leftAttr = con.getLeft();
        Attribute rightAttr = (Attribute) con.getRight();
        leftIndex = left.getSchema().indexOf(leftAttr);
        rightIndex = right.getSchema().indexOf(rightAttr);

        // Gets the type of the join attribute.
        attrType = left.getSchema().typeOf(leftAttr);

        return super.open();
    }

    /**
     * Selects tuples satisfying the join condition from input buffers and returns.
     *
     * @return the next page of output tuples.
     */
    @Override
    public Batch next() {
        // Returns empty if either left or right table reaches end-of-stream.
        if (eosLeft || eosRight) {
            close();
            return null;
        } else {
            if (leftBatch == null) {
                leftBatch = left.next();
            }
            if (rightBatch == null) {
                rightBatch = right.next();
            }
        }

        // The output buffer.
        Batch outBatch = new Batch(batchSize);

        while (!outBatch.isFull()) {
            // The tuple that is currently being processed from right input batch.
            Tuple rightTuple;

            // Left tuple remains unchanged if it has not attempted to match with all tuples in the current right partition.
            if (rightPartitionIndex < rightPartition.size() - 1) {
                rightPartitionIndex++;
                rightTuple = rightPartition.elementAt(rightPartitionIndex);
            } else {
                // Reads in another batch if necessary.
                if (leftBatch == null) {
                    eosLeft = true;
                    break;
                } else if (leftCursor == leftBatch.size()) {
                    leftBatch = left.next();
                    leftCursor = 0;
                }

                // Checks whether the left batch still has tuples left.
                if (leftBatch == null || leftBatch.size() <= leftCursor) {
                    eosLeft = true;
                    break;
                }

                // Reads in the next tuple from left batch.
                Tuple nextLeftTuple = leftBatch.elementAt(leftCursor);
                int comparisionResult = leftTuple == null ? -1 : compareTuples(leftTuple, nextLeftTuple);
                leftTuple = nextLeftTuple;
                leftCursor++;

                // Moves back to the beginning of right partition if the next left tuple remains the same value as the current one.
                if (comparisionResult == 0) {
                    rightPartitionIndex = 0;
                    rightTuple = rightPartition.elementAt(0);
                } else {
                    // Proceeds and creates a new right partition otherwise.
                    rightPartition = createNextRightPartition();
                    if (rightPartition.isEmpty()) {
                        eosRight = true;
                        break;
                    }

                    // Updates the right tuple.
                    rightPartitionIndex = 0;
                    rightTuple = rightPartition.elementAt(rightPartitionIndex);
                }
            }

            // Checks whether the current left & right tuple matches and adds to output batch if so.
            if (leftTuple == null || rightTuple == null) {
                break;
            } else if (compareTuples(leftTuple, rightTuple) == 0) {
                outBatch.add(leftTuple.joinWith(rightTuple));
            }
        }

        return outBatch;
    }

    /**
     * Creates the next partition from the right input batch based on the current right cursor value.
     *
     * @return a vector containing all tuples in the next right partition.
     */
    private Vector<Tuple> createNextRightPartition() {
        Vector<Tuple> partition = new Vector<>();
        int comparisionResult = 0;
        if (nextRightTuple == null) {
            nextRightTuple = readNextRightTuple();
            if (nextRightTuple == null) {
                eosRight = true;
                return partition;
            }
        }

        // Continues until the next tuple carries a different value.
        while (comparisionResult == 0) {
            partition.add(nextRightTuple);

            nextRightTuple = readNextRightTuple();
            if (nextRightTuple == null) {
                eosRight = true;
                break;
            }
            comparisionResult = compareTuples(partition.elementAt(0), nextRightTuple);
        }

        return partition;
    }

    /**
     * Reads the next tuple from right input batch.
     *
     * @return the next tuple if available; null otherwise.
     */
    private Tuple readNextRightTuple() {
        // Reads another batch if necessary.
        if (rightBatch == null) {
            return null;
        } else if (rightCursor == rightBatch.size()) {
            rightBatch = right.next();
            rightCursor = 0;
        }

        // Checks whether the right batch still has tuples left.
        if (rightBatch == null || rightBatch.size() <= rightCursor) {
            return null;
        }

        // Reads the next tuple.
        Tuple next = rightBatch.elementAt(rightCursor);
        rightCursor++;
        return next;
    }

    /**
     * Compares two tuples based on the join attribute.
     *
     * @param tuple1 is the first tuple.
     * @param tuple2 is the second tuple.
     * @return an integer indicating the comparision result, compatible with the {@link java.util.Comparator} interface.
     */
    private int compareTuples(Tuple tuple1, Tuple tuple2) {
        Object value1 = tuple1.dataAt(leftIndex);
        Object value2 = tuple2.dataAt(rightIndex);

        switch (attrType) {
            case Attribute.INT:
                return Integer.compare((int) value1, (int) value2);
            case Attribute.STRING:
                return ((String) value1).compareTo((String) value2);
            case Attribute.REAL:
                return Float.compare((float) value1, (float) value2);
            default:
                return 0;
        }
    }

    /**
     * Closes this operator.
     *
     * @return true if the operator is closed successfully.
     */
    @Override
    public boolean close() {
        left.close();
        right.close();
        return super.close();
    }
}
