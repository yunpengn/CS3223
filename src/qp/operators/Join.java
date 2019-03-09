package qp.operators;

import qp.utils.Condition;
import qp.utils.Schema;

/**
 * Defines the base class for all join operators.
 */
public class Join extends Operator {
    // The left child of the join operator.
    Operator left;
    // The right child of the join operator.
    Operator right;
    // The join condition.
    Condition con;
    // The number of buffers available
    int numOfBuffer;

    // The type of the join (such as NestedJoin, SortMerge or HashJoin).
    int joinType;
    // The index of the current join node.
    private int nodeIndex;

    /**
     * Creates a new join operator.
     *
     * @param left is the left child of the join operator.
     * @param right is the right child of the join operator.
     * @param cn is the join condition.
     * @param type is the join type.
     */
    public Join(Operator left, Operator right, Condition cn, int type) {
        super(type);
        this.left = left;
        this.right = right;
        this.con = cn;
    }

    /**
     * Setter for the numOfBuffer.
     *
     * @param num is the number of buffers available to this join operator.
     */
    public void setNumOfBuffer(int num) {
        this.numOfBuffer = num;
    }

    /**
     * Getter for the numOfBuffer.
     *
     * @return the number of buffers available to this join operator.
     */
    int getNumOfBuffer() {
        return numOfBuffer;
    }

    /**
     * Getter for nodeIndex.
     *
     * @return the index of this node in the query plan tree.
     */
    public int getNodeIndex() {
        return nodeIndex;
    }

    /**
     * Setter for nodeIndex.
     *
     * @param num is the index of this node in the query plan tree.
     */
    public void setNodeIndex(int num) {
        this.nodeIndex = num;

    }

    /**
     * Getter for joinType.
     *
     * @return the type of this join operator.
     */
    public int getJoinType() {
        return joinType;
    }

    /**
     * Setter for joinType.
     *
     * @param type is the type of this join operator.
     */
    public void setJoinType(int type) {
        this.joinType = type;
    }

    /**
     * Getter for left.
     *
     * @return the left child of this join operator.
     */
    public Operator getLeft() {
        return left;
    }

    /**
     * Setter for left.
     *
     * @param left is the left child of this join operator.
     */
    public void setLeft(Operator left) {
        this.left = left;
    }

    /**
     * Getter for right.
     *
     * @return the right child of this join operator.
     */
    public Operator getRight() {
        return right;
    }

    /**
     * Setter for right.
     *
     * @param right is the right child of this join operator.
     */
    public void setRight(Operator right) {
        this.right = right;
    }

    /**
     * Setter for condition.
     *
     * @param cond is the join condition.
     */
    public void setCondition(Condition cond) {
        this.con = cond;
    }

    /**
     * Getter for condition.
     *
     * @return the join condition.
     */
    public Condition getCondition() {
        return con;
    }

    /**
     * Creates a copy of this operator.
     *
     * @return the deep clone of this join operator (recursively clone left & right-hand side).
     */
    @Override
    public Object clone() {
        Operator newLeft = (Operator) left.clone();
        Operator newRight = (Operator) right.clone();
        Condition newCondition = (Condition) con.clone();
        Schema newSchema = newLeft.getSchema().joinWith(newRight.getSchema());

        Join jn = new Join(newLeft, newRight, newCondition, opType);
        jn.setSchema(newSchema);
        jn.setJoinType(joinType);
        jn.setNodeIndex(nodeIndex);
        jn.setNumOfBuffer(numOfBuffer);
        return jn;
    }
}
