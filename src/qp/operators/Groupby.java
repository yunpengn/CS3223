package qp.operators;

import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;

/**
 * Defines a groupby operator which groups the table records by attribute(s).
 */
public class Groupby extends Distinct {
    /**
     * Creates a new GROUP_BY operator.
     *
     * @param base is the base operator.
     */
    public Groupby(Operator base, Vector groupbyList) {
        super(base, groupbyList);
    }
}
