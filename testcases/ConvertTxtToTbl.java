import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Time;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Schema;
import qp.utils.Tuple;

/**
 * Assume that the first line of the file contain the names of the attributes of the relation, and each subsequent line
 * represents 1 tuple of the relation. We also assume that the fields of each line is delimited by tabs ("\t").
 */
public class ConvertTxtToTbl {
    /**
     * The entry point of this RandomDB class.
     *
     * @param args are the CLI arguments supplied by the user.
     * @throws IOException if there is an exception caused by file I/O.
     */
    public static void main(String[] args) throws IOException {
        // Premature exit if the number of supplied arguments is wrong.
        if (args.length != 1) {
            System.out.println("usage: java ConvertTxtToTbl <tablename> \n creats <tablename>.tbl files");
            System.exit(1);
        }

        String tblName = args[0];
        String tblFile = tblName + ".tbl";
        String metaFile = tblName + ".md";

        // Opens the input & output stream.
        BufferedReader in = new BufferedReader(new FileReader(tblName + ".txt"));
        ObjectOutputStream outTbl = new ObjectOutputStream(new FileOutputStream(tblFile));

        // Reads the schema from the metadata file.
        Schema schema = null;
        try {
            ObjectInputStream ins = new ObjectInputStream(new FileInputStream(metaFile));
            schema = (Schema) ins.readObject();
        } catch (ClassNotFoundException ce) {
            System.out.println("class not found exception --- error in schema object file");
            System.exit(1);
        }

        // Reads each line in the txt file.
        String line = in.readLine();
        while (line != null) {
            // Reads each attribute in the current row.
            StringTokenizer tokenizer = new StringTokenizer(line);
            Vector<Object> data = new Vector<>();
            for (int i = 0; tokenizer.hasMoreElements(); i++) {
                String dataElement = tokenizer.nextToken();
                switch (schema.typeOf(i)) {
                    case Attribute.INT:
                        data.add(Integer.valueOf(dataElement));
                        break;
                    case Attribute.TIME:
                        int value = Integer.valueOf(dataElement);
                        data.add(new Date((long) value));
                        break;
                    case Attribute.REAL:
                        data.add(Float.valueOf(dataElement));
                        break;
                    case Attribute.STRING:
                        data.add(dataElement);
                        break;
                    default:
                        System.err.println("Invalid getData type");
                        System.exit(1);
                }
            }

            // Writes out the attributes in the current row.
            Tuple tuple = new Tuple(data);
            outTbl.writeObject(tuple);
            line = in.readLine();
        }

        // Closes all streams gracefully.
        in.close();
        outTbl.close();
    }
}
