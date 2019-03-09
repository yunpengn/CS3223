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
     * Opens the connection to the base operator
     **/

    public boolean open() {
        eos = false;     // Since the stream is just opened
        start = 0;   // set the cursor to starting position in input buffer

        /** set number of tuples per page**/
        int tuplesize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tuplesize;


        if (base.open())
            return true;
        else
            return false;
    }


    /**
     * returns a batch of tuples that satisfies the
     * * condition specified on the tuples coming from base operator
     * * NOTE: This operation is performed on the fly
     **/

    public Batch next() {
        //System.out.println("Select:-----------------in next--------------");

        int i = 0;

        if (eos) {
            close();
            return null;
        }

        /** An output buffer is initiated**/
        outBatch = new Batch(batchSize);

        /** keep on checking the incoming pages until
         ** the output buffer is full
         **/
        while (!outBatch.isFull()) {
            if (start == 0) {
                inBatch = base.next();
                /** There is no more incoming pages from base operator **/
                if (inBatch == null) {

                    eos = true;
                    return outBatch;
                }
            }

            /** Continue this for loop until this page is fully observed
             ** or the output buffer is full
             **/

            for (i = start; i < inBatch.size() && (!outBatch.isFull()); i++) {
                Tuple present = inBatch.elementAt(i);
                /** If the condition is satisfied then
                 ** this tuple is added tot he output buffer
                 **/
                if (checkCondition(present))
                    //if(present.checkCondn(con))
                    outBatch.add(present);
            }

            /** Modify the cursor to the position requierd
             ** when the base operator is called next time;
             **/

            if (i == inBatch.size())
                start = 0;
            else
                start = i;

            //  return outBatch;
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
        Attribute attr = con.getLhs();
        int index = schema.indexOf(attr);
        int datatype = schema.typeOf(attr);
        Object srcValue = tuple.dataAt(index);
        String checkValue = (String) con.getRhs();
        int exprtype = con.getExprType();

        if (datatype == Attribute.INT) {
            int srcVal = ((Integer) srcValue).intValue();
            int checkVal = Integer.parseInt(checkValue);
            if (exprtype == Condition.LESSTHAN) {
                if (srcVal < checkVal)
                    return true;
            } else if (exprtype == Condition.GREATERTHAN) {
                if (srcVal > checkVal)
                    return true;
            } else if (exprtype == Condition.LTOE) {
                if (srcVal <= checkVal)
                    return true;
            } else if (exprtype == Condition.GTOE) {
                if (srcVal >= checkVal)
                    return true;
            } else if (exprtype == Condition.EQUAL) {
                if (srcVal == checkVal)
                    return true;
            } else if (exprtype == Condition.NOTEQUAL) {
                if (srcVal != checkVal)
                    return true;
            } else {
                System.out.println("Select:Incorrect condition operator");
            }
        } else if (datatype == Attribute.STRING) {
            String srcVal = (String) srcValue;
            int flag = srcVal.compareTo(checkValue);

            if (exprtype == Condition.LESSTHAN) {
                if (flag < 0) return true;

            } else if (exprtype == Condition.GREATERTHAN) {
                if (flag > 0) return true;
            } else if (exprtype == Condition.LTOE) {
                if (flag <= 0) return true;
            } else if (exprtype == Condition.GTOE) {
                if (flag >= 0) return true;
            } else if (exprtype == Condition.EQUAL) {
                if (flag == 0) return true;
            } else if (exprtype == Condition.NOTEQUAL) {
                if (flag != 0) return true;
            } else {
                System.out.println("Select: Incorrect condition operator");
            }

        } else if (datatype == Attribute.REAL) {
            float srcVal = ((Float) srcValue).floatValue();
            float checkVal = Float.parseFloat(checkValue);
            if (exprtype == Condition.LESSTHAN) {
                if (srcVal < checkVal) return true;
            } else if (exprtype == Condition.GREATERTHAN) {
                if (srcVal > checkVal) return true;
            } else if (exprtype == Condition.LTOE) {
                if (srcVal <= checkVal) return true;
            } else if (exprtype == Condition.GTOE) {
                if (srcVal >= checkVal) return true;
            } else if (exprtype == Condition.EQUAL) {
                if (srcVal == checkVal) return true;
            } else if (exprtype == Condition.NOTEQUAL) {
                if (srcVal != checkVal) return true;
            } else {
                System.out.println("Select:Incorrect condition operator");
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
