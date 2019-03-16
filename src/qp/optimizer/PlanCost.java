package qp.optimizer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.StringTokenizer;

import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.OpType;
import qp.operators.Operator;
import qp.operators.Project;
import qp.operators.Scan;
import qp.operators.Select;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Schema;

/**
 * Calculates the cost of the generated plans, and also estimates the statistics of
 * the result relation.
 */
public class PlanCost {
    // The cost of the generated plans.
    private int cost;
    // The number of tuples in estimated results.
    private int numOfTuple;
    // A plan is not feasible if the buffers are not enough for a selected join.
    private boolean isFeasible;
    // A mapping from attribute name to the number of distinct values for this attribute.
    private Hashtable ht;

    /**
     * Constructor of PlanCost.
     */
    public PlanCost() {
        ht = new Hashtable();
        cost = 0;
    }

    /**
     * Getter for cost.
     *
     * @param root is the root of query plan tree.
     * @return the cost of the plan or infinity value if unavailable.
     */
    public int getCost(Operator root) {
        isFeasible = true;
        numOfTuple = calculateCost(root);

        if (isFeasible) {
            return cost;
        }
        return Integer.MAX_VALUE;
    }

    /**
     * Getter for numOfTuple.
     *
     * @return the number of tuples in estimated results.
     */
    public int getNumOfTuple() {
        return numOfTuple;
    }

    /**
     * Calculates the cost of the plan.
     *
     * @param node is the generated plan.
     * @return the number of tuples in the root.
     */
    private int calculateCost(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            return getStatistics((Join) node);
        } else if (node.getOpType() == OpType.SELECT) {
            return getStatistics((Select) node);
        } else if (node.getOpType() == OpType.PROJECT) {
            return getStatistics((Project) node);
        } else if (node.getOpType() == OpType.SCAN) {
            return getStatistics((Scan) node);
        }
        return -1;
    }

    /**
     * Projection will not change any statistics. No cost involved as done on the fly.
     *
     * @param node is the plan for Project Operator.
     * @return the cost of the plan.
     */
    private int getStatistics(Project node) {
        return calculateCost(node.getBase());
    }

    /**
     * Calculates the statistics, and cost of join operation.
     *
     * @param node is the plan for Join Operator.
     * @return the cost of the plan.
     */
    private int getStatistics(Join node) {
        int leftTuples = calculateCost(node.getLeft());
        int rightTuples = calculateCost(node.getRight());

        if (!isFeasible) {
            return -1;
        }

        Condition condition = node.getCondition();
        Schema leftSchema = node.getLeft().getSchema();
        Schema rightSchema = node.getRight().getSchema();

        // Gets the size of the tuple in output, and calculates corresponding buffer capacity.
        // i.e., number of tuples per page.
        int tupleSize = node.getSchema().getTupleSize();
        int outCapacity = Batch.getPageSize() / tupleSize;
        int leftTupleSize = leftSchema.getTupleSize();
        int leftCapacity = Batch.getPageSize() / leftTupleSize;
        int rightTupleSize = rightSchema.getTupleSize();
        int rightCapacity = Batch.getPageSize() / rightTupleSize;

        int leftPages = (int) Math.ceil(((double) leftTuples) / (double) leftCapacity);
        int rightPages = (int) Math.ceil(((double) rightTuples) / (double) rightCapacity);

        Attribute leftJoinAttr = condition.getLeft();
        Attribute rightJoinAttr = (Attribute) condition.getRight();
        int leftAttrIndex = leftSchema.indexOf(leftJoinAttr);
        int rightAttrIndex = rightSchema.indexOf(rightJoinAttr);
        leftJoinAttr = leftSchema.getAttribute(leftAttrIndex);
        rightJoinAttr = rightSchema.getAttribute(rightAttrIndex);

        // Number of distinct values of left and right join attribute.
        int leftAttrDistNum = (Integer) ht.get(leftJoinAttr);
        int rightAttrDistNum = (Integer) ht.get(rightJoinAttr);

        int numOfOutTuple = (int) Math.ceil(((double) leftTuples * rightTuples) / (double) Math.max(leftAttrDistNum, rightAttrDistNum));

        int minDistinct = Math.min(leftAttrDistNum, rightAttrDistNum);
        ht.put(leftJoinAttr, minDistinct);
        ht.put(leftJoinAttr, minDistinct);

        // Calculates the cost of the operation.
        int joinType = node.getJoinType();

        // Gets the number of buffers allocated to this join.
        int numbuff = BufferManager.getBuffersPerJoin();

        int joinCost;

        switch (joinType) {
            case JoinType.NESTED_JOIN:
                joinCost = leftPages * rightPages;
                break;
            case JoinType.BLOCK_NESTED:
                joinCost = 0;
                break;
            case JoinType.SORT_MERGE:
                joinCost = 0;
                break;
            case JoinType.HASH_JOIN:
                joinCost = 0;
                break;
            default:
                joinCost = 0;
                break;
        }

        cost = cost + joinCost;
        return numOfOutTuple;
    }

    /**
     * Gets the number of incoming tuples using the selectivity # of output tuples and
     * statistics about the attributes. No cost involved as selection is performed on
     * the fly.
     *
     * @param node is the plan for Select Operator.
     * @return the cost of the plan.
     */
    private int getStatistics(Select node) {
        // System.out.println("PlanCost: here at line 127");
        int numOfInTuple = calculateCost(node.getBase());

        if (!isFeasible) {
            return Integer.MAX_VALUE;
        }

        Condition condition = node.getCondition();
        Schema schema = node.getSchema();

        Attribute attr = condition.getLeft();

        int index = schema.indexOf(attr);
        Attribute fullAttr = schema.getAttribute(index);

        int exprType = condition.getOperator();

        // Gets the number of distinct values of selection attributes.
        int numDistinct = (Integer) ht.get(fullAttr);
        int numOfOutTuple;

        // Calculates the number of tuples in result.
        if (exprType == Condition.EQUAL) {
            numOfOutTuple = (int) Math.ceil((double) numOfInTuple / (double) numDistinct);
        } else if (exprType == Condition.NOTEQUAL) {
            numOfOutTuple = (int) Math.ceil(numOfInTuple - ((double) numOfInTuple / (double) numDistinct));
        } else {
            numOfOutTuple = (int) Math.ceil(0.5 * numOfInTuple);
        }

        // Modifies the number of distinct values of each attribute. Assuming the values are
        // distributed uniformly along the entire relation.
        for (int i = 0; i < schema.getNumCols(); i++) {
            Attribute a = schema.getAttribute(i);
            int oldValue = (Integer) ht.get(a);
            int newValue = (int) Math.ceil(((double) numOfOutTuple / (double) numOfInTuple) * oldValue);
            ht.put(a, numOfOutTuple);
        }
        return numOfOutTuple;
    }

    /**
     * The statistics file <tablename>.stat is to find the statistics about the table, which
     * contains number of tuples in the table, and number of distinct values of each attribute.
     *
     * @param node is the plan for Scan Operator.
     * @return the cost of the plan.
     */
    private int getStatistics(Scan node) {
        String tableName = node.getTableName();
        String fileName = tableName + ".stat";
        Schema schema = node.getSchema();
        int numOfAttr = schema.getNumCols();
        BufferedReader in = null;

        try {
            in = new BufferedReader(new FileReader(fileName));
        } catch (IOException io) {
            System.out.println("Error in opening file" + fileName);
            System.exit(1);
        }
        String line = null;

        // First line = number of tuples.
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("Error in reading first line of " + fileName);
            System.exit(1);
        }
        StringTokenizer tokenizer = new StringTokenizer(line);

        if (tokenizer.countTokens() != 1) {
            System.out.println("incorrect format of statistics file " + fileName);
            System.exit(1);
        }

        String temp = tokenizer.nextToken();
        // Number of tuples in this table.
        int numOfTuples = Integer.parseInt(temp);

        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("error in reading second line of " + fileName);
            System.exit(1);
        }
        tokenizer = new StringTokenizer(line);

        if (tokenizer.countTokens() != numOfAttr) {
            System.out.println("incorrect format of statistics file " + fileName);
            System.exit(1);
        }

        for (int i = 0; i < numOfAttr; i++) {
            Attribute attr = schema.getAttribute(i);
            temp = tokenizer.nextToken();
            Integer distinctValues = Integer.valueOf(temp);
            ht.put(attr, distinctValues);
        }
        // Number of tuples per page.
        int tupleSize = schema.getTupleSize();
        int pageSize = Batch.getPageSize() / tupleSize;
        // Batch.capacity();
        int numOfPages = (int) Math.ceil((double) numOfTuples / (double) pageSize);
        cost = cost + numOfPages;

        try {
            in.close();
        } catch (IOException io) {
            System.out.println("error in closing the file " + fileName);
            System.exit(1);
        }

        return numOfTuples;
    }
}
