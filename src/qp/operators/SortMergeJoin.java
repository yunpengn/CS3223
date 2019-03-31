package qp.operators;

import java.util.Vector;

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

    // Index of the join attribute in left table.
    private int leftIndex;
    // Index of the join attribute in right table.
    private int rightIndex;

    // Type of the join attribute.
    private int attrType;

    // Cursor for left side buffer.
    private int leftCursor;
    // Cursor for right side buffer.
    private int rightCursor;

    // Cursor for left & right partition.
    private int i, j;
    private Vector<Tuple> leftPartition, rightPartition;

    // Whether end of stream is reached for the left table.
    private boolean eosLeft;
    // Whether end of stream is reached for the right table.
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

        // Initializes the cursors of input buffers for both sides.
        leftCursor = 0;
        rightCursor = 0;
        eosLeft = false;
        eosRight = false;

        i = 0;
        j = 0;
        leftPartition = new Vector<>();
        rightPartition = new Vector<>();

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
        if (eosLeft || eosRight) {
            close();
            return null;
        }

        outBatch = new Batch(batchSize);
        while (!outBatch.isFull()) {
            // Joins tuples in the partition if partition is not empty.
            if (leftPartition.size() != 0 && rightPartition.size() != 0) {
                for (; i < leftPartition.size(); i++) {
                    for (; j < rightPartition.size(); ) {
                        Tuple outTuple = leftPartition.get(i).joinWith(rightPartition.get(j));
                        j++;
                        outBatch.add(outTuple);
                        if (outBatch.isFull()) {
                            return outBatch;
                        }
                    }
                }
            }
            leftPartition.removeAllElements();
            rightPartition.removeAllElements();
            i = 0;
            j = 0;

            if (leftCursor == 0) {
                leftBatch = left.next();
                if (leftBatch == null) {
                    eosLeft = true;
                    return outBatch;
                }
            }

            if (rightCursor == 0) {
                rightBatch = right.next();
                if (rightBatch == null) {
                    eosRight = true;
                    return outBatch;
                }
            }

            while (leftCursor < leftBatch.size() && rightCursor < rightBatch.size()) {
                Tuple leftTuple = leftBatch.elementAt(leftCursor);
                Tuple rightTuple = rightBatch.elementAt(rightCursor);
                if (compareTuples(leftTuple, rightTuple, leftIndex, rightIndex) < 0) {
                    leftCursor++;
                } else if (compareTuples(leftTuple, rightTuple, leftIndex, rightIndex) > 0) {
                    rightCursor++;
                } else {
                    Object value = leftTuple.dataAt(leftIndex);

                    do {
                        leftPartition.add(leftTuple);
                        leftCursor++;
                        if (leftCursor == leftBatch.size()) {
                            leftCursor = 0;
                            leftBatch = left.next();
                            if (leftBatch == null) {
                                break;
                            }
                        }
                        leftTuple = leftBatch.elementAt(leftCursor);
                    } while (leftTuple.dataAt(leftIndex).equals(value));

                    do {
                        rightPartition.add(rightTuple);
                        rightCursor++;
                        if (rightCursor == rightBatch.size()) {
                            rightCursor = 0;
                            rightBatch = right.next();
                            if (rightBatch == null) {
                                break;
                            }
                        }
                        rightTuple = rightBatch.elementAt(rightCursor);
                    } while (rightTuple.dataAt(rightIndex).equals(value));

                    break;
                }
            }
            if (leftBatch != null && leftCursor == leftBatch.size()) {
                leftCursor = 0;
            }
            if (rightBatch != null && rightCursor == rightBatch.size()) {
                rightCursor = 0;
            }
        }
        return outBatch;
    }

    /**
     * Compares two tuples based on the join attribute.
     *
     * @param tuple1 is the first tuple.
     * @param tuple2 is the second tuple.
     * @return an integer indicating the comparision result, compatible with the {@link java.util.Comparator} interface.
     */
    private int compareTuples(Tuple tuple1, Tuple tuple2, int index1, int index2) {
        Object value1 = tuple1.dataAt(index1);
        Object value2 = tuple2.dataAt(index2);

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
        return left.close() && right.close();
    }
}
