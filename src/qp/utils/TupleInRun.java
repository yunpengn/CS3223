package qp.utils;

/**
 * Defines a wrapper class for an {@link Tuple} in a sorted run. This is helpful during the merge phase
 * of external sort.
 */
public class TupleInRun {
    // The current tuple.
    public final Tuple tuple;
    // The ID of the sorted run which this tuple belongs to.
    public final int runID;
    // The ID of the tuple in its sorted run.
    public final int tupleID;

    /**
     * Creates a new {@link TupleInRun} object.
     *
     * @param tuple is the tuple.
     * @param runID is the ID of the sorted run.
     * @param tupleID is the ID of the tuple.
     */
    public TupleInRun(Tuple tuple, int runID, int tupleID) {
        this.tuple = tuple;
        this.runID = runID;
        this.tupleID = tupleID;
    }
}
