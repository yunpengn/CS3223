package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

/**
 * Implements the block-based nested loop join algorithm.
 */
public class BlockNestedJoin extends Join {
    // The number of tuples per batch.
    private int batchSize;

    /**
     * Defines some fields useful during the execution of the block-based nested
     * loop algorithm.
     */
    // Index of the join attribute in left table
    private int leftIndex;
    // Index of the join attribute in right table
    private int rightIndex;

    // The file name where the right table is materialize
    private String rightFileName;
    // File pointer to the right hand materialized file
    private ObjectInputStream in;
    // To get unique fileNum for this operation
    private static int fileNum = 0;

    // The buffers for the left input stream.
    private Batch[] leftBatches;
    // The buffer for the right input stream.
    private Batch rightBatch;
    // The output buffer.
    private Batch outBatch;

    // Cursor for left side buffer
    private int leftCursor;
    // Cursor for right side buffer
    private int rightCursor;
    // Whether end of stream is reached for the left table
    private boolean eosLeft;
    // Whether end of stream is reached for the right table
    private boolean eosRight;

    /**
     * Instantiates a new join operator using block-based nested loop algorithm.
     *
     * @param jn is the base join operator.
     */
    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        joinType = jn.getJoinType();
        numOfBuffer = jn.getNumOfBuffer();
    }

    /**
     * Opens this operator by performing the following operations:
     * 1. Finds the index of the join attributes;
     * 2. Materializes the right hand side into a file;
     * 3. Opens the connections.
     *
     * @return true if the operator is opened successfully.
     */
    @Override
    public boolean open() {
        // Selects the number of tuples per page based tuple size.
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        // Gets the join attribute from left & right table.
        Attribute leftAttr = con.getLeft();
        Attribute rightAttr = (Attribute) con.getRight();
        leftIndex = left.getSchema().indexOf(leftAttr);
        rightIndex = right.getSchema().indexOf(rightAttr);
        Batch rightPage;

        // Initializes the cursors of input buffers for both sides.
        leftCursor = 0;
        rightCursor = 0;
        eosLeft = false;
        // Right stream would be repetitively scanned. If it reaches the end, we have to start new scan.
        eosRight = true;

        // Materializes the right table for the algorithm to perform.
        if (!right.open()) {
            return false;
        } else {
            /*
             * If the right operator is not a base table, then materializes the intermediate result
             * from right into a file.
             */
            // if(right.getCondType() != OpType.SCAN){
            fileNum++;
            rightFileName = "BNJtemp-" + fileNum;
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rightFileName));
                rightPage = right.next();
                while (rightPage != null) {
                    out.writeObject(rightPage);
                    rightPage = right.next();
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: writing the temporal file error");
                return false;
            }
            // }

            if (!right.close()) {
                return false;
            }

        }
        return left.open();
    }

    /**
     * Selects tuples satisfying the join condition from input buffers and returns.
     *
     * @return the next page of output tuples.
     */
    @Override
    public Batch next() {
        // Returns empty if the left table reaches end-of-stream.
        if (eosLeft) {
            close();
            return null;
        }

        outBatch = new Batch(batchSize);
        while (!outBatch.isFull()) {
            // Checks whether we need to read a new block of pages from the left table.
            if (leftCursor == 0 && eosRight) {
                leftBatches = new Batch[numOfBuffer - 2];
                leftBatches[0] = left.next();
                // Checks if there is no more pages from the left table.
                if (leftBatches[0] == null) {
                    eosLeft = true;
                    return outBatch;
                }
                for (int i = 1; i < leftBatches.length; i++) {
                    leftBatches[i] = left.next();
                    if (leftBatches[i] == null) {
                        break;
                    }
                }

                // Starts the scanning of right table whenever a new block of left pages comes.
                try {
                    in = new ObjectInputStream(new FileInputStream(rightFileName));
                    eosRight = false;
                } catch (IOException io) {
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }
            }

            // Continuously probe the right table until we hit the end-of-stream.
            while (!eosRight) {
                try {
                    if (leftCursor == 0 && rightCursor == 0) {
                        rightBatch = (Batch) in.readObject();
                    }

                    int numOfLeftTuple = leftBatches[0].size();
                    for (int i = 1; i < leftBatches.length; i++) {
                        if (leftBatches[i] == null) {
                            break;
                        }
                        numOfLeftTuple += leftBatches[i].size();
                    }

                    for (int i = leftCursor; i < numOfLeftTuple; i++) {
                        int leftBatchIndex = i / leftBatches[0].size();
                        int leftTupleIndex = i % leftBatches[0].size();
                        Tuple leftTuple = leftBatches[leftBatchIndex].elementAt(leftTupleIndex);

                        for (int j = rightCursor; j < rightBatch.size(); j++) {
                            Tuple rightTuple = rightBatch.elementAt(j);

                            // Adds the tuple if satisfying the join condition.
                            if (leftTuple.checkJoin(rightTuple, leftIndex, rightIndex)) {
                                Tuple outTuple = leftTuple.joinWith(rightTuple);
                                outBatch.add(outTuple);

                                // Checks whether the output buffer is full.
                                if (outBatch.isFull()) {
                                    if (i == numOfLeftTuple - 1 && j == rightBatch.size() - 1) {
                                        leftCursor = 0;
                                        rightCursor = 0;
                                    } else if (i != numOfLeftTuple - 1 && j == rightBatch.size() - 1) {
                                        leftCursor = i + 1;
                                        rightCursor = 0;
                                    } else if (i == numOfLeftTuple - 1 && j != rightBatch.size() - 1) {
                                        leftCursor = i;
                                        rightCursor = j + 1;
                                    } else {
                                        leftCursor = i;
                                        rightCursor = j + 1;
                                    }

                                    // Returns since we have already produced a complete page of matching tuples.
                                    return outBatch;
                                }
                            }
                        }
                        rightCursor = 0;
                    }
                    leftCursor = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: error in temporary file reading");
                    }
                    eosRight = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: some error in deserialization");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: temporary file reading error");
                    System.exit(1);
                }
            }
        }
        return outBatch;
    }

    /**
     * Closes this operator by deleting the file generated.
     *
     * @return true if the operator is closed successfully.
     */
    @Override
    public boolean close() {
        File f = new File(rightFileName);
        f.delete();
        return true;
    }
}
