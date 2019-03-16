package qp.optimizer;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.OpType;
import qp.operators.Operator;
import qp.operators.Project;
import qp.operators.Scan;
import qp.operators.Select;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.RandNumb;
import qp.utils.SQLQuery;
import qp.utils.Schema;

/**
 * Prepares a random initial plan for the given SQL query. See the README file for more details.
 */
public class RandomInitialPlan {
    // Vector of Vectors of Select + From + Where + GroupBy.
    private SQLQuery sqlQuery;

    // TODO: add generic types to all collection classes.
    // List of project conditions.
    private Vector projectList;
    // List of from conditions.
    private Vector fromList;
    // List of select conditions.
    private Vector selectionList;
    // List of join conditions.
    private Vector joinList;
    // List of group by conditions.
    private Vector groupByList;
    // Number of joins in this query.
    private int numOfJoin;

    // Name to the Operator hashtable.
    private Hashtable nameToOpr;

    // Root of the query plan tree.
    private Operator root;

    /**
     * Constructor of RandomInitialPlan.
     */
    public RandomInitialPlan(SQLQuery sqlQuery) {
        this.sqlQuery = sqlQuery;

        projectList = sqlQuery.getProjectList();
        fromList = sqlQuery.getFromList();
        selectionList = sqlQuery.getSelectionList();
        joinList = sqlQuery.getJoinList();
        groupByList = sqlQuery.getGroupByList();
        numOfJoin = joinList.size();
    }

    /**
     * Getter of numOfJoin.
     */
    public int getNumJoins() {
        return numOfJoin;
    }

    /**
     * Prepares the initial plan for the query.
     *
     * @return root of the query plan tree.
     */
    public Operator prepareInitialPlan() {
        nameToOpr = new Hashtable();

        createScanOp();
        createSelectOp();

        if (numOfJoin != 0) {
            createJoinOp();
        }
        createProjectOp();
        return root;
    }

    /**
     * Creates Scan Operator for each of the table mentioned in fromList.
     */
    private void createScanOp() {
        int numOfTable = fromList.size();
        Scan tempOp = null;

        for (int i = 0; i < numOfTable; i++) {
            String tableName = (String) fromList.elementAt(i);
            Scan op1 = new Scan(tableName, OpType.SCAN);
            tempOp = op1;

            // Reads the schema of the table from tableName.md file. md stands for metadata.
            String fileName = tableName + ".md";
            try {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName));
                Schema schema = (Schema) ois.readObject();
                op1.setSchema(schema);
                ois.close();
            } catch (Exception e) {
                System.err.println("RandomInitialPlan:Error reading Schema of the table" + fileName);
                System.exit(1);
            }
            nameToOpr.put(tableName, op1);
        }

        // 12 July 2003 (whtok)
        // To handle the case where there is no where clause
        // selectionList is empty, hence we set the root to be
        // the scan operator. the projectOp would be put on top of
        // this later in CreateProjectOp
        if (selectionList.size() == 0) {
            root = tempOp;
            return;
        }
    }

    /**
     * Creates Selection Operators for each of the selection condition mentioned in the
     * condition list.
     */
    private void createSelectOp() {
        Select opr = null;

        for (int j = 0; j < selectionList.size(); j++) {
            Condition condition = (Condition) selectionList.elementAt(j);
            if (condition.getOpType() == Condition.SELECT) {
                String tableName = condition.getLhs().getTabName();

                Operator tempOp = (Operator) nameToOpr.get(tableName);
                opr = new Select(tempOp, condition, OpType.SELECT);
                // Sets the schema same as base relation.
                opr.setSchema(tempOp.getSchema());

                modifyHashtable(tempOp, opr);
                // nameToOpr.put(tableName,opr);
            }
        }
        // The last selection is the root of the plan tree constructed thus far.
        if (selectionList.size() != 0) {
            root = opr;
        }
    }

    /**
     * Creates join operators.
     */
    private void createJoinOp() {
        BitSet bitCList = new BitSet(numOfJoin);
        int joinNum = RandNumb.randInt(0, numOfJoin - 1);
        Join join = null;

        // Repeats until all the join conditions are considered.
        while (bitCList.cardinality() != numOfJoin) {
            // chooses another join condition if this condition is already considered.
            while (bitCList.get(joinNum)) {
                joinNum = RandNumb.randInt(0, numOfJoin - 1);
            }
            Condition condition = (Condition) joinList.elementAt(joinNum);
            String leftTable = condition.getLhs().getTabName();
            String rightTable = ((Attribute) condition.getRhs()).getTabName();

            Operator leftOp = (Operator) nameToOpr.get(leftTable);
            Operator rightOp = (Operator) nameToOpr.get(rightTable);
            join = new Join(leftOp, rightOp, condition, OpType.JOIN);
            join.setNodeIndex(joinNum);
            Schema newSchema = leftOp.getSchema().joinWith(rightOp.getSchema());
            join.setSchema(newSchema);
            // Randomly selects a join type.
            int numOfJoinTypes = JoinType.numJoinTypes();
            int joinType = RandNumb.randInt(0, numOfJoinTypes - 1);
            join.setJoinType(joinType);

            modifyHashtable(leftOp, join);
            modifyHashtable(rightOp, join);
            // nameToOpr.put(leftTable,join);
            // nameToOpr.put(rightTable,join);

            bitCList.set(joinNum);
        }

        // The last join operation is the root for the constructed till now.
        if (numOfJoin != 0) {
            root = join;
        }
    }

    /**
     * Crates a project Operator.
     */
    private void createProjectOp() {
        Operator base = root;
        if (projectList == null) {
            projectList = new Vector();
        }

        if (!projectList.isEmpty()) {
            root = new Project(base, projectList, OpType.PROJECT);
            Schema newSchema = base.getSchema().subSchema(projectList);
            root.setSchema(newSchema);
        }
    }

    /**
     * Replaces the old Operator with new Operator.
     * TODO [TBD]: refactor hashtable access to list access.
     *
     * @param oldOp is the old Operator to be replaced.
     * @param newOp is the new Operator.
     */
    private void modifyHashtable(Operator oldOp, Operator newOp) {
        Enumeration e = nameToOpr.keys();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            Operator temp = (Operator) nameToOpr.get(key);
            if (temp == oldOp) {
                nameToOpr.put(key, newOp);
            }
        }
    }
}
