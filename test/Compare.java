import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

/**
 * Compares the contents of two files (expected output & actual output) without considering
 * the internal order.
 */
public class Compare {
    public static void main(String[] args) {
        // Checks the number of arguments.
        if (args.length != 2) {
            System.out.println("Usage: java Compare <expected_output_file> <actual_output_file>");
            System.exit(1);
        }

        try {
            // Gets the lists of expected and actual tuples from files.
            Vector<String> expectedTuples = getExpectedTuples(args[0]);
            Vector<String> actualTuples = getActualTuples(args[1]);

            // Checks whether the number of tuples are the same.
            if (expectedTuples.size() != actualTuples.size()) {
                System.out.println("Expected and actual outputs are different!");
                System.exit(1);
            }

            // Checks whether actual tuples list contains all the tuples from expected tuples list.
            for (String tuple : expectedTuples) {
                if (!actualTuples.contains(tuple)) {
                    System.out.println("Expected and actual outputs are different!");
                    System.exit(1);
                }

                // Removes the tuple from actual tuples list.
                actualTuples.remove(tuple);
            }

            // Checks whether actual tuples list contains any extra tuples.
            if (actualTuples.size() != 0) {
                System.out.println("Expected and actual outputs are different!");
                System.exit(1);
            }

            System.out.println("Expected and actual outputs are the same!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets a list of expected output tuples from the expected output file.
     *
     * @param fileName is the name of the expected output file.
     * @return a list of expected output tuples.
     * @throws IOException if an I/O error occurs.
     */
    private static Vector<String> getExpectedTuples(String fileName) throws IOException {
        BufferedReader expected = new BufferedReader(new FileReader(new File(fileName)));
        Vector<String> expectedTuples = new Vector<>();

        while (true) {
            String temp = expected.readLine();
            if (temp == null) {
                break;
            }

            expectedTuples.add(temp);
        }
        return expectedTuples;
    }

    /**
     * Gets a list of actual output tuples from the actual output file.
     *
     * @param fileName is the name of the actual output file.
     * @return a list of actual output tuples.
     * @throws IOException if an I/O error occurs.
     */
    private static Vector<String> getActualTuples(String fileName) throws IOException {
        BufferedReader actual = new BufferedReader(new FileReader(new File(fileName)));
        Vector<String> actualTuples = new Vector<>();

        while (true) {
            String temp = actual.readLine();
            if (temp == null) {
                break;
            }

            actualTuples.add(temp);
        }
        return actualTuples;
    }
}
