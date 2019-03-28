package qp.operators;

/**
 * Applies external sort on a given relation.
 */
public class Sort extends Operator {
    /**
     * Instantiates a operator of a certain type.
     *
     * @param type is the type of this operator.
     */
    public Sort(int type) {
        super(type);
    }
}
