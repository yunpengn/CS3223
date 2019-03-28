package qp.operators;

/**
 * Applies external sort on a given relation.
 */
public class Sort extends Operator {
    // The base operator (i.e., the unsorted relation).
    private final Operator base;
    // The number of buffer pages available.
    private final int numOfBuffers;

    public Sort(Operator base, int numOfBuffers) {
        super(OpType.SORT);
        this.base = base;
        this.numOfBuffers = numOfBuffers;
    }

    @Override
    public boolean open() {
        // Makes sure the base operator can open successfully first.
        if (!base.open()) {
            return false;
        }

        // Phase 1: generate sorted runs using in-memory sorting algorithms.
        if (!generateSortedRuns()) {
            return false;
        }
        // Phase 2: merge sorted runs together (could be in multiple passes).
        return mergeRuns();
    }

    private boolean generateSortedRuns() {
        return true;
    }

    private boolean mergeRuns() {
        return true;
    }
}
