package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;

public class SortMergeJoin extends Join {
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
        // Sorts the left & right relation.
        left.open();
        right.open();

        // Gets the join attribute from left & right table.
        Attribute leftAttr = con.getLeft();
        Attribute rightAttr = (Attribute) con.getRight();
        leftIndex = left.getSchema().indexOf(leftAttr);
        rightIndex = right.getSchema().indexOf(rightAttr);

        // Initializes the cursors of input buffers for both sides.
        leftCursor = 0;
        rightCursor = 0;
        eosLeft = false;
        eosRight = false;

        return super.open();
    }

    /**
     * Selects tuples satisfying the join condition from input buffers and returns.
     *
     * @return the next page of output tuples.
     */
    @Override
    public Batch next() {
        return null;
    }

    /**
     * Closes this operator by closing left & right operators.
     *
     * @return true if both left and right operators are closed successfully.
     */
    @Override
    public boolean close() {
        boolean isLeftClosed = left.close();
        boolean isRightClosed = right.close();
        return isLeftClosed & isRightClosed;
    }
}
