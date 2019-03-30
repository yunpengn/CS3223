package qp.operators;

/**
 * Defines a distinct operator which eliminates duplicates from input.
 */
public class Distinct extends Operator {
    // The base operator.
    private final Operator base;

    /**
     * Creates a new distinct operator.
     *
     * @param base is the base operator.
     */
    public Distinct(Operator base) {
        super(OpType.DISTINCT);
        this.base = base;
    }
}
