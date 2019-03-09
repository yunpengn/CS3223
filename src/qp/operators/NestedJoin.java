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
 * Implements the page-based nested loop join algorithm.
 */
public class NestedJoin extends Join {
    // The number of tuples per batch.
    private int batchSize;

    /**
     * Defines some fields useful during the execution of the page-based nested
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

    // The buffer for the left input stream.
    private Batch leftBatch;
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
     * Instantiates a new join operator using page-based nested loop algorithm.
     *
     * @param jn is the base join operator.
     */
    public NestedJoin(Join jn) {
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

        Attribute leftAttr = con.getLhs();
        Attribute rightAttr = (Attribute) con.getRhs();
        leftIndex = left.getSchema().indexOf(leftAttr);
        rightIndex = right.getSchema().indexOf(rightAttr);
        Batch rightPage;

        // Initializes the cursors of input buffers for both sides.
        leftCursor = 0;
        rightCursor = 0;
        eosLeft = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosRight = true;

        // Materializes the right table for the algorithm to perform.
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/

            //if(right.getOpType() != OpType.SCAN){
            fileNum++;
            rightFileName = "NJtemp-" + String.valueOf(fileNum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rightFileName));
                while ((rightPage = right.next()) != null) {
                    out.writeObject(rightPage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("NestedJoin:writing the temporay file error");
                return false;
            }
            //}
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }


    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/


    @Override
    public Batch next() {
        //System.out.print("NestedJoin:--------------------------in next----------------");
        //Debug.PPrint(con);
        //System.out.println();
        int i, j;
        if (eosLeft) {
            close();
            return null;
        }
        outBatch = new Batch(batchSize);


        while (!outBatch.isFull()) {

            if (leftCursor == 0 && eosRight == true) {
                /** new left page is to be fetched**/
                leftBatch = (Batch) left.next();
                if (leftBatch == null) {
                    eosLeft = true;
                    return outBatch;
                }
                /** Whenver a new left page came , we have to start the
                 ** scanning of right table
                 **/
                try {

                    in = new ObjectInputStream(new FileInputStream(rightFileName));
                    eosRight = false;
                } catch (IOException io) {
                    System.err.println("NestedJoin:error in reading the file");
                    System.exit(1);
                }

            }

            while (!eosRight) {

                try {
                    if (rightCursor == 0 && leftCursor == 0) {
                        rightBatch = (Batch) in.readObject();
                    }

                    for (i = leftCursor; i < leftBatch.size(); i++) {
                        for (j = rightCursor; j < rightBatch.size(); j++) {
                            Tuple lefttuple = leftBatch.elementAt(i);
                            Tuple righttuple = rightBatch.elementAt(j);
                            if (lefttuple.checkJoin(righttuple, leftIndex, rightIndex)) {
                                Tuple outtuple = lefttuple.joinWith(righttuple);

                                //Debug.PPrint(outtuple);
                                //System.out.println();
                                outBatch.add(outtuple);
                                if (outBatch.isFull()) {
                                    if (i == leftBatch.size() - 1 && j == rightBatch.size() - 1) {//case 1
                                        leftCursor = 0;
                                        rightCursor = 0;
                                    } else if (i != leftBatch.size() - 1 && j == rightBatch.size() - 1) {//case 2
                                        leftCursor = i + 1;
                                        rightCursor = 0;
                                    } else if (i == leftBatch.size() - 1 && j != rightBatch.size() - 1) {//case 3
                                        leftCursor = i;
                                        rightCursor = j + 1;
                                    } else {
                                        leftCursor = i;
                                        rightCursor = j + 1;
                                    }
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
                        System.out.println("NestedJoin:Error in temporary file reading");
                    }
                    eosRight = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("NestedJoin:Some error in deserialization ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("NestedJoin:temporary file reading error");
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
