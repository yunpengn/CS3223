import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import qp.operators.Debug;
import qp.operators.Operator;
import qp.optimizer.BufferManager;
import qp.optimizer.RandomII;
import qp.optimizer.RandomOptimizer;
import qp.parser.Scanner;
import qp.parser.parser;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.SQLQuery;
import qp.utils.Schema;
import qp.utils.Tuple;

/**
 * This is main driver program of the query processor.
 */
public class QueryMain {
    // A writer for output file.
    private static PrintWriter out;
    // The number of attributes in the given relation.
    private static int numOfAttrs;

    /**
     * The entry point of this RandomDB class.
     *
     * @param args are the CLI arguments supplied by the user.
     */
    public static void main(String[] args) {
        // Premature exit if the number of supplied arguments is wrong.
        if (args.length != 2) {
            System.out.println("usage: java QueryMain <queryFileName> <resultFile>");
            System.exit(1);
        }

        // Asks user to enter the number of bytes per page.
        System.out.println("enter the number of bytes per page");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String temp;
        try {
            temp = in.readLine();
            int pageSize = Integer.parseInt(temp);
            Batch.setPageSize(pageSize);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Defines the file name of the input & output.
        String queryFile = args[0];
        String resultFile = args[1];
        FileInputStream source = null;

        // Reads the query from the input file.
        try {
            source = new FileInputStream(queryFile);
        } catch (FileNotFoundException ff) {
            System.out.println("File not found: " + queryFile);
            System.exit(1);
        }

        // Scans the query.
        Scanner sc = new Scanner(source);
        parser p = new parser();
        p.setScanner(sc);

        // Parses the query.
        try {
            p.parse();
        } catch (Exception e) {
            System.out.println("Exception occurred while parsing");
            System.exit(1);
        }

        // SQLQuery is the result of the parsing.
        SQLQuery sqlQuery = p.getSQLQuery();
        int numOfJoin = sqlQuery.getNumJoin();

        /*
         * If there are joins, then assigns buffers to each join operator while preparing
         * the plan. As buffer manager is not implemented, just input the number of buffers
         * available.
         */
        if (numOfJoin != 0 || sqlQuery.getIsDistinct()) {
            System.out.println("enter the number of buffers available");

            try {
                temp = in.readLine();
                int numBuff = Integer.parseInt(temp);
                if (numBuff < 3) {
                    System.out.println("Minimum 3 buffers are required for join or external sort.");
                    System.exit(1);
                }
                BufferManager bm = new BufferManager(numBuff, numOfJoin);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Checks whether the number of buffers available is enough.
        int numBuff = BufferManager.getBuffersPerJoin();
        if (numOfJoin > 0 && numBuff < 3) {
            System.out.println("Minimum 3 buffers are required per a join operator.");
            System.exit(1);
        }

        // Uses random Optimization algorithm to get a random optimized execution plan.
        RandomOptimizer randomOptimizer = new RandomII(sqlQuery);
        Operator logicalRoot = randomOptimizer.getOptimizedPlan();
        if (logicalRoot == null) {
            System.out.println("root is null");
            System.exit(1);
        }

        // Prepares the execution plan.
        Operator root = RandomOptimizer.makeExecPlan(logicalRoot);

        // Prints the final execution plan.
        System.out.println("----------------------Execution Plan----------------");
        Debug.PPrint(root);
        System.out.println();

        // Asks user whether to continue execution of the program.
        System.out.println("enter 1 to continue, 0 to abort");
        try {
            temp = in.readLine();
            int flag = Integer.parseInt(temp);
            if (flag == 0) {
                System.exit(1);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Records down the start time of the query execution.
        long startTime = System.currentTimeMillis();

        // Tries to open the logical root.
        if (!root.open()) {
            System.out.println("Root: error in opening of root");
            System.exit(1);
        }

        // Creates a writer for output file.
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(resultFile)));
        } catch (IOException io) {
            System.out.println("QueryMain: error in opening result file: " + resultFile);
            System.exit(1);
        }

        // Prints the schema of the result.
        Schema schema = root.getSchema();
        numOfAttrs = schema.getNumCols();
        printSchema(schema);

        // Prints each tuple in the result.
        Batch resultBatch = root.next();
        while (resultBatch != null) {
            for (int i = 0; i < resultBatch.size(); i++) {
                printTuple(resultBatch.elementAt(i));
            }
            resultBatch = root.next();
        }

        // Closes the resources gracefully.
        root.close();
        out.close();

        // Records down the end time of the query execution and thus calculates the execution time.
        long endTime = System.currentTimeMillis();
        double executionTime = (endTime - startTime) / 1000.0;
        System.out.println("Execution time = " + executionTime);

    }

    /**
     * Prints out a given schema.
     *
     * @param schema is the schema to be printed.
     */
    private static void printSchema(Schema schema) {
        for (int i = 0; i < numOfAttrs; i++) {
            Attribute attr = schema.getAttribute(i);
            out.print(attr.getTabName() + "." + attr.getColName() + "  ");
        }
        out.println();
    }

    /**
     * Prints out a given tuple.
     *
     * @param t is the tuple to be printed.
     */
    private static void printTuple(Tuple t) {
        for (int i = 0; i < numOfAttrs; i++) {
            Object data = t.dataAt(i);
            if (data instanceof Integer) {
                out.print(((Integer) data).intValue() + "\t");
            } else if (data instanceof Float) {
                out.print(((Float) data).floatValue() + "\t");
            } else {
                out.print(((String) data) + "\t");
            }
        }
        out.println();
    }
}
