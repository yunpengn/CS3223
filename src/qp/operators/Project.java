package qp.operators;

import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

/**
 * Defines the project operator, which projects out the required attributes from the
 * result.
 */
public class Project extends Operator {
    // The base operator
    Operator base;
    // The set of attributes
    private Vector attrSet;
    // The number of tuples per outBatch
    private int batchSize;

    // The input buffer
    private Batch inBatch;
    // The output buffer
    private Batch outBatch;

    // The index of the attributes in the base operator that are to be projected
    private int[] attrIndex;

    /**
     * Creates a new project operator.
     *
     * @param base is the base operator
     * @param attrSet is the set of attributes
     * @param type is the type of the operator (i.e., PROJECT)
     */
    public Project(Operator base, Vector attrSet, int type) {
        super(type);
        this.base = base;
        this.attrSet = attrSet;
    }

    /**
     * Setter for base.
     *
     * @param base is the base operator.
     */
    public void setBase(Operator base) {
        this.base = base;
    }

    /**
     * Getter for base.
     *
     * @return the base operator.
     */
    public Operator getBase() {
        return base;
    }

    /**
     * Getter for attrSet.
     *
     * @return a vector containing all attributes.
     */
    public Vector getProjectAttr() {
        return attrSet;
    }

    /** Opens the connection to the base operator
     ** Also figures out what are the columns to be
     ** projected from the base operator
     **/

    public boolean open() {
        /** setnumber of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tuplesize;


        /** The followingl loop findouts the index of the columns that
         ** are required from the base operator
         **/

        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrSet.size()];
        //System.out.println("Project---Schema: ----------in open-----------");
        //System.out.println("base Schema---------------");
        //Debug.PPrint(baseSchema);
        for (int i = 0; i < attrSet.size(); i++) {
            Attribute attr = (Attribute) attrSet.elementAt(i);
            int index = baseSchema.indexOf(attr);
            attrIndex[i] = index;

            //  Debug.PPrint(attr);
            //System.out.println("  "+index+"  ");
        }

        if (base.open())
            return true;
        else
            return false;
    }

    /**
     * @return the next page of tuples produced by the project operator.
     */
    public Batch next() {
        // Creates a new output buffer.
        outBatch = new Batch(batchSize);

        // Returns empty if the input buffer is empty.
        inBatch = base.next();
        if (inBatch == null) {
            return null;
        }

        for (int i = 0; i < inBatch.size(); i++) {
            // Goes through each required attribute.
            Tuple baseTuple = inBatch.elementAt(i);
            Vector<Object> present = new Vector<>();
            for (int j = 0; j < attrSet.size(); j++) {
                Object data = baseTuple.dataAt(attrIndex[j]);
                present.add(data);
            }

            Tuple outTuple = new Tuple(present);
            outBatch.add(outTuple);
        }

        return outBatch;
    }

    /**
     * Closes the operator.
     *
     * @return true if the operator is closed successfully.
     */
    @Override
    public boolean close() {
        return true;
        // return base.close();
    }

    @Override
    public Object clone() {
        Operator newBase = (Operator) base.clone();
        Vector newAttrSet = new Vector();
        for (int i = 0; i < attrSet.size(); i++) {
            newAttrSet.add(((Attribute) attrSet.elementAt(i)).clone());
        }

        Project newProject = new Project(newBase, newAttrSet, opType);
        Schema newSchema = newBase.getSchema().subSchema(newAttrSet);
        newProject.setSchema(newSchema);
        return newProject;
    }
}
