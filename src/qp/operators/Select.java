package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

/**
 * Defines the scan operator.
 */
public class Select extends Operator {
    // The base operator.
    private Operator base;
    // The select condition.
    private Condition con;
    // The number of pages per batch.
    private int batchSize;

    // To indicate whether end of stream is reached
    private boolean eos;

    // The input buffer
    private Batch inBatch;
    // The output buffer
    private Batch outBatch;
    // The position of the cursor in the input buffer
    private int start;

    /**
     * Creates a new select operator.
     *
     * @param base is the base operator.
     * @param con is the select condition.
     * @param type is the type of the operator (i.e., SELECT).
     */
    public Select(Operator base, Condition con, int type) {
        super(type);
        this.base = base;
        this.con = con;

    }

    /**
     * Setter for base.
     *
     * @param base is the base operator.
     */
    public void setBase(Operator base) {
        this.base = base;
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
     * Setter for condition.
     *
     * @param cond is the select condition.
     */
    public void setCondition(Condition cond) {
        this.con = cond;
    }

    /**
     * Getter for condition.
     *
     * @return the select condition.
     */
    public Condition getCondition() {
        return con;
    }

    /**
     * Opens the connection to the base operator.
     *
     * @return true if the operator is opened successfully.
     */
    public boolean open() {
        // Sets to false since the stream is just opened
        eos = false;
        // Sets the cursor to starting position in input buffer
        start = 0;

        // Sets the batch size based on the tuple size.
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        // Opens the base operator as well.
        return base.open();
    }

    /**
     * Selects those tuples satisfying the select condition. This operation is performed on the fly.
     *
     * @return the next page of tuples.
     */
    public Batch next() {
        // Closes the operator if reaching the end-of-stream.
        if (eos) {
            close();
            return null;
        }

        // Initiates an output buffer.
        outBatch = new Batch(batchSize);

        // Continues until the the output buffer is full.
        while (!outBatch.isFull()) {
            // Retrieves a new page from input if the cursor is at the beginning of a new page.
            if (start == 0) {
                inBatch = base.next();
                // Returns if reaching the end-of-stream.
                if (inBatch == null) {
                    eos = true;
                    return outBatch;
                }
            }

            // Continues until the input buffer is fully observed or output buffer is full.
            int i;
            for (i = start; i < inBatch.size() && !outBatch.isFull(); i++) {
                Tuple present = inBatch.elementAt(i);
                // Adds the current tuple to the output buffer if satisfying the select condition.
                if (checkCondition(present)) {
                    outBatch.add(present);
                }
            }

            // Modifies the cursor to the position required when the operator is called next time.
            if (i == inBatch.size()) {
                start = 0;
            } else {
                start = i;
            }
        }
        return outBatch;
    }

    /**
     * Closes the output connection when there is no more page to output.
     *
     * @return true if the connection is closed successfully.
     */
    public boolean close() {
        return true;
        // return base.close();
    }

    /**
     * Checks whether the selection condition is satisfied for the current tuple.
     *
     * @param tuple is the tuple to be checked.
     * @return true if the condition is satisfied.
     */
    private boolean checkCondition(Tuple tuple) {
        Attribute attr = con.getLeft();
        int index = schema.indexOf(attr);
        int dataType = schema.typeOf(attr);
        Object srcValue = tuple.dataAt(index);
        String checkValue = (String) con.getRight();
        int expressionType = con.getOperator();

        if (dataType == Attribute.INT) {
            int srcVal = (Integer) srcValue;
            int checkVal = Integer.parseInt(checkValue);

            if (expressionType == Condition.LESS_THAN) {
                return srcVal < checkVal;
            } else if (expressionType == Condition.GREATER_THAN) {
                return srcVal > checkVal;
            } else if (expressionType == Condition.LTOE) {
                return srcVal <= checkVal;
            } else if (expressionType == Condition.GTOE) {
                return srcVal >= checkVal;
            } else if (expressionType == Condition.EQUAL) {
                return srcVal == checkVal;
            } else if (expressionType == Condition.NOTEQUAL) {
                return srcVal != checkVal;
            } else {
                System.out.println("Select:incorrect condition operator");
            }
        } else if (dataType == Attribute.STRING) {
            String srcVal = (String) srcValue;
            int flag = srcVal.compareTo(checkValue);

            if (expressionType == Condition.LESS_THAN) {
                return flag < 0;
            } else if (expressionType == Condition.GREATER_THAN) {
                return flag > 0;
            } else if (expressionType == Condition.LTOE) {
                return flag <= 0;
            } else if (expressionType == Condition.GTOE) {
                return flag >= 0;
            } else if (expressionType == Condition.EQUAL) {
                return flag == 0;
            } else if (expressionType == Condition.NOTEQUAL) {
                return flag != 0;
            } else {
                System.out.println("Select: incorrect condition operator");
            }
        } else if (dataType == Attribute.REAL) {
            float srcVal = (Float) srcValue;
            float checkVal = Float.parseFloat(checkValue);

            if (expressionType == Condition.LESS_THAN) {
                return srcVal < checkVal;
            } else if (expressionType == Condition.GREATER_THAN) {
                return srcVal > checkVal;
            } else if (expressionType == Condition.LTOE) {
                return srcVal <= checkVal;
            } else if (expressionType == Condition.GTOE) {
                return srcVal >= checkVal;
            } else if (expressionType == Condition.EQUAL) {
                return srcVal == checkVal;
            } else if (expressionType == Condition.NOTEQUAL) {
                return srcVal != checkVal;
            } else {
                System.out.println("Select: incorrect condition operator");
            }
        }
        return false;
    }

    @Override
    public Object clone() {
        Operator newBase = (Operator) base.clone();
        Condition newCond = (Condition) con.clone();
        Select newSelect = new Select(newBase, newCond, opType);
        newSelect.setSchema(newBase.getSchema());
        return newSelect;
    }
}
