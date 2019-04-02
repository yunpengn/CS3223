package qp.operators;

import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;

/**
 * Defines a groupby operator which groups the table records by attribute(s).
 */
public class Groupby extends Operator {
    // The groupby list.
    private final Vector groupbyList;
    // The base operator.
    private Operator base;
    // The sort operator applied on the base operator.
    private Sort sortedBase;
    // The number of buffers available.
    private int numOfBuffer;
    // Records whether we have reached end-of-stream.
    private boolean eos = false;

    /**
     * Creates a new groupby operator.
     *
     * @param base is the base operator.
     */
    public Groupby(Operator base, Vector groupbyList) {
        super(OpType.GROUPBY);
        this.base = base;
        this.groupbyList = groupbyList;
    }

    @Override
    public boolean open() {
        sortedBase = new Sort(base, groupbyList, numOfBuffer);
        return sortedBase.open();
    }

    @Override
    public Batch next() {
        if (eos) {
            close();
            return null;
        }

        Batch outBatch = sortedBase.next();
        if (outBatch == null) {
            eos = true;
        }

        return outBatch;
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

    @Override
    public Object clone() {
        Operator newBase = (Operator) base.clone();
        Vector<Attribute> newGroupbyList = new Vector<>();
        for (int i = 0; i < groupbyList.size(); i++) {
            Attribute attribute = (Attribute) ((Attribute) groupbyList.elementAt(i)).clone();
            newGroupbyList.add(attribute);
        }

        Groupby newGroupby = new Groupby(base, newGroupbyList);
        Schema newSchema = newBase.getSchema();
        newGroupby.setSchema(newSchema);
        return newGroupby;
    }
}
