package qp.operators;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

/**
 * Applies external sort on a given relation.
 */
public class Sort extends Operator {
    // The base operator (i.e., the unsorted relation).
    private final Operator base;
    // The number of buffer pages available.
    private final int numOfBuffers;
    // The index of the attribute to sort based on.
    private final int sortKeyIndex;
    // The data type of the attribute to sort based on.
    private final int sortKeyType;

    /**
     * Creates a new sort operator.
     *
     * @param base is the base operator.
     * @param numOfBuffers is the number of buffers (in pages) available.
     */
    public Sort(Operator base, Attribute sortKey, int numOfBuffers) {
        super(OpType.SORT);
        this.base = base;
        this.sortKeyIndex = base.schema.indexOf(sortKey);
        this.sortKeyType = base.schema.getAttribute(sortKeyIndex).getType();
        this.numOfBuffers = numOfBuffers;
        this.schema = base.schema;
    }

    @Override
    public boolean open() {
        // Makes sure the base operator can open successfully first.
        if (!base.open()) {
            return false;
        }

        // Phase 1: generate sorted runs using in-memory sorting algorithms.
        int numOfRuns = generateSortedRuns();
        // Phase 2: merge sorted runs together (could be in multiple passes).
        return mergeRuns(numOfRuns);
    }

    /**
     * Generates sorted runs and stores them to disk. Currently, we utilize all buffer pages available
     * and thus minimizes the number of runs generated.
     *
     * @return the number of sorted runs generated.
     */
    private int generateSortedRuns() {
        // Reads in the next page from base operator.
        Batch inBatch = base.next();

        int numOfRuns = 0;
        while (inBatch != null) {
            // Stores the tuples that will be in the current run.
            Vector<Tuple> tuplesInRun = new Vector<>();

            // Reads in as many tuples as possible (until either there is no more tuples or reaches buffer limit).
            for (int i = 0; i < numOfBuffers && inBatch != null; i++) {
                tuplesInRun.addAll(inBatch.getTuples());
                inBatch = base.next();
            }

            // Sorts the tuples using in-memory sorting algorithms.
            tuplesInRun.sort((tuple1, tuple2) -> {
                Object value1 = tuple1.dataAt(sortKeyIndex);
                Object value2 = tuple2.dataAt(sortKeyIndex);

                switch (sortKeyType) {
                    case Attribute.INT:
                        return Integer.compare((int) value1, (int) value2);
                    case Attribute.STRING:
                        return ((String) value1).compareTo((String) value2);
                    case Attribute.REAL:
                        return Float.compare((float) value1, (float) value2);
                    default:
                        return 0;
                }
            });

            // Stores the sorted result into disk.
            String fileName = getSortedRunFileName(numOfRuns);
            try {
                ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(fileName));
                stream.writeObject(tuplesInRun);
            } catch (IOException e) {
                System.err.println("Sort: unable to write sortedRun with ID=" + numOfRuns + " due to " + e);
                System.exit(1);
            }

            // Reads in another page and prepares for the next iteration.
            inBatch = base.next();
            numOfRuns++;
        }

        return numOfRuns;
    }

    private boolean mergeRuns(int numOfRuns) {
        return true;
    }

    /**
     * Provides the file name of a generated sorted run based on its run ID.
     *
     * @param runID is the ID of the sorted run.
     */
    private String getSortedRunFileName(int runID) {
        return "Sort-run-" + runID;
    }
}
