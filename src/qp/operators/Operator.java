package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;

/**
 * Defines the base class for all operators.
 */
public class Operator {
    // The type of the operator (such as SELECT, PROJECT or JOIN).
    int opType;
    // The result schema at this operator.
    Schema schema;

    /**
     * Instantiates a operator of a certain type.
     *
     * @param type is the type of this operator.
     */
    public Operator(int type) {
        this.opType = type;
    }

    /**
     * Getter for the schema.
     *
     * @return the result schema at this operator.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Setter for the schema.
     *
     * @param schema is the result schema at this operator.
     */
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    /**
     * Getter for the operator type.
     *
     * @return the type of this operator.
     */
    public int getOpType() {
        return opType;
    }

    /**
     * Setter for the operator type.
     *
     * @param type is type of this operator.
     */
    public void setOpType(int type) {
        this.opType = type;
    }

    /**
     * @return whether this operator is open.
     */
    public boolean open() {
        return true;
    }

    /**
     * @return the next page of tuples.
     */
    public Batch next() {
        System.out.println("Operator:  ");
        return null;
    }

    /**
     * @return whether this operator is close.
     */
    public boolean close() {
        return true;
    }

    /**
     * Creates a copy of this operator.
     *
     * @return the clone of this operator.
     */
    @Override
    public Object clone() {
        return new Operator(opType);
    }
}
