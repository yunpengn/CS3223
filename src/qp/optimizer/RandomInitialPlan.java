package qp.optimizer;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.Map;

import qp.operators.Distinct;
import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.OpType;
import qp.operators.Operator;
import qp.operators.Project;
import qp.operators.Scan;
import qp.operators.Select;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.RandomNum;
import qp.utils.SQLQuery;
import qp.utils.Schema;

/**
 * Prepares a random initial plan for the given SQL query. See the README file for more details.
 */
public class RandomInitialPlan {
    // The SQL query to create initial plan for.
    private final SQLQuery sqlQuery;
    // Mapping from table name to operator.
    private Hashtable<String, Operator> tableNameToOperator;
    // Root of the query plan tree.
    private Operator root;

    /**
     * Constructor of RandomInitialPlan.
     *
     * @param sqlQuery is the SQL query to create initial plan for.
     */
    public RandomInitialPlan(SQLQuery sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    /**
     * Getter of numOfJoin.
     */
    int getNumJoins() {
        return sqlQuery.getNumJoin();
    }

    /**
     * Prepares the initial plan for the query.
     *
     * @return root of the query plan tree.
     */
    Operator prepareInitialPlan() {
        tableNameToOperator = new Hashtable<>();

        // Follows the execution order: SCAN -> WHERE -> JOIN -> PROJECT -> DISTINCT.
        createScanOperators();
        createSelectOperators();
        createJoinOperators();
        createProjectOperator();
        createDistinctOperator();

        return root;
    }

    /**
     * Creates Scan Operator for each of the table mentioned in fromList.
     */
    private void createScanOperators() {
        Scan tempOp = null;

        for (Object table: sqlQuery.getFromList()) {
            String tableName = (String) table;
            Scan operator = new Scan(tableName);
            tempOp = operator;

            // Reads the schema of the table from tableName.md file. md stands for metadata.
            String fileName = tableName + ".md";
            try {
                ObjectInputStream inStream = new ObjectInputStream(new FileInputStream(fileName));
                Schema schema = (Schema) inStream.readObject();
                operator.setSchema(schema);
                inStream.close();
            } catch (Exception e) {
                System.err.println("RandomInitialPlan: Error reading Schema of the table " + fileName);
                System.exit(1);
            }
            tableNameToOperator.put(tableName, operator);
        }

        // To handle the case when there is no WHERE clause (i.e., selectionList is empty), we set the root
        // to be the scan operator. The projectOp would be put on top of this later in createProjectOperator.
        if (sqlQuery.getSelectionList().size() == 0) {
            root = tempOp;
        }
    }

    /**
     * Creates Selection Operators for each of the selection condition mentioned in the
     * condition list.
     */
    private void createSelectOperators() {
        if (sqlQuery.getSelectionList().size() == 0) {
            return;
        }

        Operator operator = null;
        for (Object cond: sqlQuery.getSelectionList()) {
            Condition condition = (Condition) cond;
            if (condition.getCondType() != Condition.SELECT) {
                continue;
            }

            String tableName = condition.getLeft().getTabName();
            Operator base = tableNameToOperator.get(tableName);
            operator = new Select(base, condition, OpType.SELECT);

            operator.setSchema(base.getSchema());
            modifyHashtable(base, operator);
        }

        // The last selection is the root of the plan tree constructed thus far.
        root = operator;
    }

    /**
     * Creates join operators.
     */
    private void createJoinOperators() {
        int numOfJoin = getNumJoins();
        if (numOfJoin == 0) {
            return;
        }

        BitSet bitCList = new BitSet(numOfJoin);
        int joinNum = RandomNum.randInt(0, numOfJoin - 1);
        Join join = null;

        // Repeats until all the join conditions are considered.
        while (bitCList.cardinality() != numOfJoin) {
            // Chooses another join condition if this condition is already considered.
            while (bitCList.get(joinNum)) {
                joinNum = RandomNum.randInt(0, numOfJoin - 1);
            }
            Condition condition = (Condition) sqlQuery.getJoinList().elementAt(joinNum);
            String leftTable = condition.getLeft().getTabName();
            String rightTable = ((Attribute) condition.getRight()).getTabName();

            Operator leftOp = tableNameToOperator.get(leftTable);
            Operator rightOp = tableNameToOperator.get(rightTable);
            join = new Join(leftOp, rightOp, condition, OpType.JOIN);
            join.setNodeIndex(joinNum);
            Schema newSchema = leftOp.getSchema().joinWith(rightOp.getSchema());
            join.setSchema(newSchema);

            // Randomly selects a join type.
            int numOfJoinTypes = JoinType.numJoinTypes();
            int joinType = RandomNum.randInt(0, numOfJoinTypes - 1);
            join.setJoinType(joinType);

            modifyHashtable(leftOp, join);
            modifyHashtable(rightOp, join);

            bitCList.set(joinNum);
        }

        // The last join operation is the root for the constructed till now.
        root = join;
    }

    /**
     * Crates a project Operator.
     */
    private void createProjectOperator() {
        if (sqlQuery.getProjectList() == null) {
            return;
        }

        Operator base = root;
        root = new Project(base, sqlQuery.getProjectList());
        Schema newSchema = base.getSchema().subSchema(sqlQuery.getProjectList());
        root.setSchema(newSchema);
    }

    /**
     * Creates a distinct operator.
     */
    private void createDistinctOperator() {
        if (!sqlQuery.getIsDistinct()) {
            return;
        }
        root = new Distinct(root);
    }

    /**
     * Replaces the old Operator with new Operator.
     *
     * @param oldOp is the old Operator to be replaced.
     * @param newOp is the new Operator.
     */
    private void modifyHashtable(Operator oldOp, Operator newOp) {
        for (Map.Entry<String, Operator> entry: tableNameToOperator.entrySet()) {
            if (entry.getValue() == oldOp) {
                tableNameToOperator.put(entry.getKey(), newOp);
            }
        }
    }
}
