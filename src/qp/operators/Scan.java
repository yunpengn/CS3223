package qp.operators;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

/**
 * Defines the scan operator, which reads data from a file by scanning.
 */
public class Scan extends Operator {
    // The name of the file being scanned
    private String fileName;
    // The name of the table
    private String tableName;

    // The number of tuples per batch
    private int batchSize;

    // The input file stream
    private ObjectInputStream in;

    // To indicate whether end of stream reached or not
    private boolean eos;

    /**
     * Creates a new scan operator.
     *
     * @param tableName is the name of the table.
     * @param type is the operator type (i.e., SCAN).
     */
    public Scan(String tableName, int type) {
        super(type);
        this.tableName = tableName;
        fileName = tableName + ".tbl";
    }

    /**
     * Getter for tableName.
     *
     * @return the table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Opens connection.
     *
     * @return true if the operator is opened successfully.
     */
    @Override
    public boolean open() {
        // Gets the batch size from tuple size.
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;
        eos = false;

        // Tries to open the input stream.
        try {
            in = new ObjectInputStream(new FileInputStream(fileName));
        } catch (Exception e) {
            System.err.println(" Error reading " + fileName);
            return false;
        }
        return true;
    }

    /**
     * @return the next page of tuples in this relation.
     */
    public Batch next() {
        // Returns since we have reached the end of the whole file.
        if (eos) {
            close();
            return null;
        }

        Batch outBatch = new Batch(batchSize);
        while (!outBatch.isFull()) {
            try {
                Tuple data = (Tuple) in.readObject();
                outBatch.add(data);
            } catch (ClassNotFoundException cnf) {
                System.err.println("Scan: class not found for reading file  " + fileName);
                System.exit(1);
            } catch (EOFException EOF) {
                // Sends the incomplete page and close in the next call.
                eos = true;
                return outBatch;
            } catch (IOException e) {
                System.err.println("Scan: error reading " + fileName);
                System.exit(1);
            }
        }
        return outBatch;
    }

    /**
     * Closes the connection.
     *
     */
    public boolean close() {
        try {
            in.close();
        } catch (IOException e) {
            System.err.println("Scan: error closing " + fileName);
            return false;
        }
        return true;
    }

    @Override
    public Object clone() {
        String newTable = tableName;
        Scan newScan = new Scan(newTable, opType);
        newScan.setSchema((Schema) schema.clone());
        return newScan;
    }
}
