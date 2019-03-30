package qp.operators;

import java.util.Vector;

/**
 * Defines a distinct operator which eliminates duplicates from input.
 */
public class Distinct extends Operator {
    // The base operator.
    private final Operator base;
    // The project list (based on which we distinguish the duplicates).
    private final Vector projectList;

    /**
     * Creates a new distinct operator.
     *
     * @param base is the base operator.
     */
    public Distinct(Operator base, Vector projectList) {
        super(OpType.DISTINCT);
        this.base = base;
        this.projectList = projectList;
    }
}
