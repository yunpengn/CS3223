import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Schema;

/**
 * Generates serialized schema files like <pre>XXXX.md</pre> and getData files in textual format
 * like <pre>XXXX.txt</pre>.
 */
public class RandomDB {
    // A random generator with the current time as the seed.
    private static Random random;
    // An array of public keys
    private static boolean[] pk;
    // An array of foreign keys
    private static boolean[] fk;

    /**
     * Creates a new RandomDB class based on the current time (in millseconds).
     */
    public RandomDB() {
        random = new Random(System.currentTimeMillis());
    }

    /**
     * The entry point of this RandomDB class.
     *
     * @param args are the CLI arguments supplied by the user.
     */
    public static void main(String[] args) {
        // Premature exit if the number of supplied arguments is wrong.
        if (args.length != 2) {
            System.out.println("Usage: java RandomDB <dbname> <numrecords>");
            System.exit(1);
        }

        // Creates a new RandomDB instance.
        RandomDB rdb = new RandomDB();

        // Gets the file name for all outputs.
        String tblName = args[0];
        String srcFile = args[0] + ".det";
        String metaFile = args[0] + ".md";
        String dataFile = args[0] + ".txt";
        String statFile = args[0] + ".stat";

        // Gets the number of tuples generated from user input.
        int numOfTuples = Integer.parseInt(args[1]);

        try {
            BufferedReader input = new BufferedReader(new FileReader(srcFile));
            ObjectOutputStream outMeta = new ObjectOutputStream(new FileOutputStream(metaFile));
            PrintWriter outData = new PrintWriter(new BufferedWriter(new FileWriter(dataFile)));
            PrintWriter outStat = new PrintWriter(new BufferedWriter(new FileWriter(statFile)));

            // 1st line: <number of columns>
            outStat.print(numOfTuples);
            outStat.println();

            // Read the number of columns.
            String line = input.readLine();
            int numCol = Integer.parseInt(line);
            String[] dataType = new String[numCol];
            int[] range = new int[numCol];
            String[] keyType = new String[numCol];

            // 2nd line: <size of tuple (in bytes)>
            line = input.readLine();
            int size = Integer.parseInt(line);

            // Defines some variables for schema generation later.
            Vector<Attribute> attrList = new Vector<>();
            Attribute currentAttr;
            int i = 0;

            /*
             * Reads information about every attribute, each of which is described on a new line. Each line
             * is in the format of <pre>colName colType range keyType attrSize</pre>.
             */
            line = input.readLine();
            while (line != null) {
                StringTokenizer tokenizer = new StringTokenizer(line);
                // Gets the column name.
                String colName = tokenizer.nextToken();
                // Gets the getData type.
                dataType[i] = tokenizer.nextToken();

                int type;
                if (dataType[i].equals("INTEGER")) {
                    type = Attribute.INT;
                } else if (dataType[i].equals("STRING")) {
                    type = Attribute.STRING;
                } else if (dataType[i].equals("REAL")) {
                    type = Attribute.REAL;
                } else {
                    type = -1;
                    System.err.println("invalid getData type");
                    System.exit(1);
                }

                // Gets the range of values allowed.
                range[i] = Integer.parseInt(tokenizer.nextToken());

                // Gets the key type (i.e., PK/FK/NK).
                keyType[i] = tokenizer.nextToken();
                int typeOfKey;
                if (keyType[i].equals("PK")) {
                    pk = new boolean[range[i]];
                    typeOfKey = Attribute.PK;
                } else if (keyType[i].equals("FK")) {
                    fk = new boolean[range[i]];
                    typeOfKey = Attribute.FK;
                } else {
                    typeOfKey = -1;
                }

                // Gets the number of bytes for the current column.
                int numOfBytes = Integer.parseInt(tokenizer.nextToken());


                if (typeOfKey != -1) {
                    currentAttr = new Attribute(tblName, colName, type);
                } else {
                    currentAttr = new Attribute(tblName, colName, type, typeOfKey);
                }
                currentAttr.setAttrSize(numOfBytes);
                attrList.add(currentAttr);
                i++;
                line = input.readLine();
            }

            // Creates a schema object based on the information for each column.
            Schema schema = new Schema(attrList);
            schema.setTupleSize(size);
            outMeta.writeObject(schema);
            outMeta.close();

            // Generates the getData and outputs in <pre>XXXX.txt</pre> file.
            for (i = 0; i < numOfTuples; i++) {
                System.out.println("input table generation: " + i);

                for (int j = 0; j < numCol; j++) {
                    switch (dataType[j]) {
                        case "STRING":
                            outData.print(rdb.randString(range[j]) + "\t");
                            break;
                        case "FLOAT":
                            outData.print(range[j] * random.nextFloat() + "\t");
                            break;
                        case "INTEGER":
                            int value = random.nextInt(range[j]);

                            // PK does not allow duplicates.
                            if (attrList.elementAt(j).isPrimaryKey()) {
                                while (pk[value]) {
                                    value = random.nextInt(range[j]);
                                }
                                pk[value] = true;
                            }
                            // FK allows duplicates.
                            if (keyType[j].equals("FK")) {
                                fk[value] = true;
                            }

                            outData.print(value + "\t");
                            break;
                    }
                }
                if (i != numOfTuples - 1) {
                    outData.println();
                }
            }
            outData.close();
            System.out.println("end of table generation");

            /*
             * Prints the number of distinct values for each column in <pre>XXXX.stat</pre> file.
             */
            for (i = 0; i < numCol; i++) {
                switch (dataType[i]) {
                    case "STRING":
                        outStat.print(numOfTuples + "\t");
                        break;
                    case "FLOAT":
                        outStat.print(numOfTuples + "\t");
                        break;
                    case "INTEGER":
                        if (keyType[i].equals("PK")) {
                            outStat.print(rdb.getNumDistinct(pk) + "\t");
                        } else if (keyType[i].equals("FK")) {
                            outStat.print(rdb.getNumDistinct(fk) + "\t");
                        } else {
                            if (numOfTuples < range[i]) {
                                outStat.print(numOfTuples + "\t");
                            } else {
                                outStat.print(range[i] + "\t");
                            }
                        }
                        break;
                }
            }
            outStat.close();
            input.close();
        } catch (IOException io) {
            System.out.println("error in IO");
            System.exit(1);
        }

    }

    /**
     * Generates a random string of length equal to range.
     *
     * @param range is the length of the generated string.
     * @return a random string of the expected length.
     */
    private String randString(int range) {
        StringBuilder s = new StringBuilder();
        for (int j = 0; j < range; j++) {
            s.append(Character.toChars(97 + random.nextInt(26)));
        }

        return s.toString();
    }

    /**
     * Counts the number of elements that are true in an array of boolean values.
     *
     * @param key is array of boolean values.
     * @return the number of elements that are true in the input array.
     */
    private int getNumDistinct(boolean[] key) {
        int count = 0;

        for (boolean item : key) {
            if (item) {
                count++;
            }
        }

        return count;
    }
}
