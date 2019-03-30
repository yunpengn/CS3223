import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

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
            // Gets the sets of expected and actual tuples from files.
            HashMap<String, Integer> expectedTuples = getTuples(args[0]);
            HashMap<String, Integer> actualTuples = getTuples(args[1]);

            // Checks whether the number of tuples are the same.
            int numOfExpectedTuples = 0;
            int numOfActualTuples = 0;
            for (int num : expectedTuples.values()) {
                numOfExpectedTuples += num;
            }
            for (int num : actualTuples.values()) {
                numOfActualTuples += num;
            }
            if (numOfActualTuples != numOfExpectedTuples) {
                System.out.println("Expected and actual outputs are different!");
                System.exit(1);
            }

            // Checks whether actual tuples list contains all the tuples from expected tuples list.
            for (String tuple : expectedTuples.keySet()) {
                if (!actualTuples.containsKey(tuple) || !actualTuples.get(tuple).equals(expectedTuples.get(tuple))) {
                    System.out.println("Expected and actual outputs are different!");
                    System.exit(1);
                }
            }

            System.out.println("Expected and actual outputs are the same!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets a set of output tuples from the output file.
     *
     * @param fileName is the name of the output file.
     * @return a set of output tuples.
     * @throws IOException if an I/O error occurs.
     */
    private static HashMap<String, Integer> getTuples(String fileName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)));
        HashMap<String, Integer> tuples = new HashMap<>();

        while (true) {
            String temp = reader.readLine();
            if (temp == null) {
                break;
            }

            if (tuples.containsKey(temp)) {
                tuples.put(temp, tuples.get(temp) + 1);
                continue;
            }

            tuples.put(temp, 1);
        }
        return tuples;
    }
}
