package qp.utils;

/**
 * Represents the join or select condition in a {@link SQLQuery}.
 */
public class Condition {
    // The operator in a condition.
    public static final int LESS_THAN = 1;
    public static final int GREATER_THAN = 2;
    public static final int LTOE = 3;
    public static final int GTOE = 4;
    public static final int EQUAL = 5;
    public static final int NOTEQUAL = 6;

    // The type of a condition (either select condition or join condition).
    public static final int SELECT = 1;
    public static final int JOIN = 2;

    // The left side of this condition (must be an attribute).
    private Attribute left;
    // The right side of this condition (an attribute for join, but a string for select).
    private Object right;
    // The type of this condition (either select condition or join condition).
    private int condType;
    // The operator in this condition, such as >, <, =, etc.
    private int operator;

    /**
     * Creates a new condition.
     *
     * @param left is the left side of this condition.
     * @param operator is the operator in this condition.
     * @param right is the right side of this condition.
     */
    public Condition(Attribute left, int operator, Object right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

    /**
     * Creates a new condition.
     *
     * @param operator is the operator in this condition.
     */
    public Condition(int operator) {
        this.operator = operator;
    }

    /**
     * Getter for left side.
     *
     * @return the left side of this condition.
     */
    public Attribute getLeft() {
        return left;
    }

    /**
     * Setter for left side.
     *
     * @param attr is the left side of this condition.
     */
    public void setLeft(Attribute attr) {
        left = attr;
    }

    /**
     * Setter for condition type.
     *
     * @param num is the type of this condition (either select or join).
     */
    public void setCondType(int num) {
        condType = num;
    }

    /**
     * Getter for condition type.
     *
     * @return the type of this condition (either select or join).
     */
    public int getCondType() {
        return condType;
    }

    /**
     * Setter for operator.
     *
     * @param num is the operator in this condition.
     */
    public void setOperator(int num) {
        operator = num;
    }

    /**
     * Getter for operator.
     *
     * @return the operator in this condition.
     */
    public int getOperator() {
        return operator;
    }

    /**
     * Setter for right side.
     *
     * @param value is the right side of this condition.
     */
    public void setRight(Object value) {
        right = value;
    }

    /**
     * Getter for right side.
     *
     * @return the right side of this condition.
     */
    public Object getRight() {
        return right;
    }

    /**
     * Flips a join condition by changing its left & right side.
     *
     * @implNote this is only applicable for a join condition.
     */
    public void flip() {
        if (condType != JOIN) {
            return;
        }
        Attribute temp = left;
        left = (Attribute) right;
        right = temp;
    }

    /**
     * Creates a copy of this condition.
     *
     * @return the copy.
     */
    @Override
    public Object clone() {
        Attribute newLeft = (Attribute) left.clone();
        Object newRight;
        if (condType == SELECT) {
            newRight = right;
        } else {
            newRight = ((Attribute) right).clone();
        }

        Condition newCond = new Condition(newLeft, operator, newRight);
        newCond.setCondType(condType);
        return newCond;
    }
}
