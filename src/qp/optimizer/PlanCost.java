package qp.optimizer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.StringTokenizer;

import qp.operators.Distinct;
import qp.operators.Groupby;
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
    private Hashtable<Attribute, Integer> ht;

    /**
     * Constructor of PlanCost.
     */
    public PlanCost() {
        ht = new Hashtable<>();
        cost = 0;
    }

    /**
     * Getter for cost.
     *
     * @param root is the root of query plan tree.
     * @return the cost of the plan or infinity value if unavailable.
     */
    int getCost(Operator root) {
        isFeasible = true;
        numOfTuple = calculateCost(root);

        return isFeasible ? cost : Integer.MAX_VALUE;
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
        } else if (node.getOpType() == OpType.DISTINCT) {
            return getStatistics((Distinct) node);
        } else if (node.getOpType() == OpType.GROUPBY) {
            return getStatistics((Groupby) node);
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

        Attribute leftJoinAttr = condition.getLeft();
        Attribute rightJoinAttr = (Attribute) condition.getRight();
        int leftAttrIndex = leftSchema.indexOf(leftJoinAttr);
        int rightAttrIndex = rightSchema.indexOf(rightJoinAttr);
        leftJoinAttr = leftSchema.getAttribute(leftAttrIndex);
        rightJoinAttr = rightSchema.getAttribute(rightAttrIndex);

        // Gets the size of the tuple and calculates corresponding buffer capacity.
        int leftTupleSize = leftSchema.getTupleSize();
        int leftCapacity = Batch.getPageSize() / leftTupleSize;
        int rightTupleSize = rightSchema.getTupleSize();
        int rightCapacity = Batch.getPageSize() / rightTupleSize;

        // Estimates the number of pages.
        int leftPages = (int) Math.ceil(1.0 * leftTuples / leftCapacity);
        int rightPages = (int) Math.ceil(1.0 * rightTuples / rightCapacity);

        // Number of distinct values of left and right join attribute.
        int leftAttrDistNum = ht.get(leftJoinAttr);
        int rightAttrDistNum = ht.get(rightJoinAttr);

        int numOfOutTuple = (int) Math.ceil(1.0 * leftTuples * rightTuples / Math.max(leftAttrDistNum, rightAttrDistNum));

        int minDistinct = Math.min(leftAttrDistNum, rightAttrDistNum);
        ht.put(leftJoinAttr, minDistinct);
        ht.put(leftJoinAttr, minDistinct);

        // Calculates the cost of the operation.
        int joinType = node.getJoinType();

        // Gets the number of buffers allocated to this join.
        int numOfBuffer = BufferManager.getBuffersPerJoin();

        int joinCost;

        switch (joinType) {
            case JoinType.PAGE_NESTED_JOIN:
                joinCost = leftPages * rightPages;
                break;
            case JoinType.BLOCK_NESTED_JOIN:
                int leftBlocks = (int) Math.ceil(leftPages / (numOfBuffer - 2));
                joinCost = leftBlocks * rightPages;
                break;
            case JoinType.SORT_MERGE_JOIN:
                int leftSortCost = getExternalSortCost(leftPages, numOfBuffer);
                int rightSortCost = getExternalSortCost(rightPages, numOfBuffer);
                joinCost = leftSortCost + rightSortCost + rightPages;
                break;
            case JoinType.HASH_JOIN:
                joinCost = 0;
                break;
            case JoinType.INDEX_NESTED_JOIN:
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
        int numOfInTuple = calculateCost(node.getBase());

        if (!isFeasible) {
            return Integer.MAX_VALUE;
        }

        Condition condition = node.getCondition();
        Schema schema = node.getSchema();

        // Gets the attribute on the left side of the condition.
        Attribute attr = condition.getLeft();
        int index = schema.indexOf(attr);
        Attribute fullAttr = schema.getAttribute(index);

        // Gets the number of distinct values of selection attributes.
        int numDistinct = ht.get(fullAttr);
        int numOfOutTuple;

        // Calculates the number of tuples in result.
        switch (condition.getOperator()) {
            case Condition.EQUAL:
                numOfOutTuple = (int) Math.ceil(1.0 * numOfInTuple / numDistinct);
                break;
            case Condition.NOTEQUAL:
                numOfOutTuple = (int) Math.ceil(numOfInTuple - 1.0 * numOfInTuple / numDistinct);
                break;
            default:
                numOfOutTuple = (int) Math.ceil(0.5 * numOfInTuple);
        }

        // Modifies the number of distinct values of each attribute. Assuming the values are
        // distributed uniformly along the entire relation.
        for (int i = 0; i < schema.getNumCols(); i++) {
            Attribute a = schema.getAttribute(i);
            int oldValue = ht.get(a);
            int newValue = (int) Math.ceil(1.0 * numOfOutTuple / numOfInTuple * oldValue);
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
        String fileName = node.getTableName() + ".stat";
        Schema schema = node.getSchema();
        int numOfAttr = schema.getNumCols();

        // Opens the file.
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(fileName));
        } catch (IOException io) {
            System.err.printf("PlanCost: error in opening file with name %s due to %s\n", fileName, io.toString());
            System.exit(1);
        }
        String line = null;

        // 1st line: number of tuples.
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.err.printf("PlanCost: error in reading first line of file with name %s due to %s\n", fileName, io.toString());
            System.exit(1);
        }

        StringTokenizer tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != 1) {
            System.err.printf("PlanCost: incorrect format of statistics file with name %s", fileName);
            System.exit(1);
        }
        String temp = tokenizer.nextToken();
        int numOfTuples = Integer.parseInt(temp);

        // 2nd line: number of distinct values for each attribute.
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("error in reading second line of " + fileName);
            System.exit(1);
        }
        tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != numOfAttr) {
            System.err.printf("PlanCost: incorrect format of statistics file with name %s\n", fileName);
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
        int numOfPages = (int) Math.ceil(1.0 * numOfTuples / pageSize);
        cost += numOfPages;

        // Closes the stream opened.
        try {
            in.close();
        } catch (IOException io) {
            System.err.printf("PlanCost: error when closing file with name %s due to %s\n", fileName, io.toString());
            System.exit(1);
        }

        return numOfTuples;
    }

    /**
     * Gets the cost of a distinct node.
     *
     * @param node is the plan for Distinct Operator.
     * @return the number of tuples after DISTINCT.
     */
    private int getStatistics(Distinct node) {
        // TODO: calculates the number of distinct tuples here for output.
        return getSort(node.getBase());
    }

    /**
     * Gets the cost of a GROUP_BY node.
     *
     * @param node is the plan for GROUP_BY Operator.
     * @return the number of tuples after GROUP_BY.
     */
    private int getStatistics(Groupby node) {
        return getSort(node.getBase());
    }

    private int getSort(Operator base) {
        // Calculates the input statistics.
        int numOfInTuples = calculateCost(base);
        int inCapacity = Batch.getPageSize() / base.getSchema().getTupleSize();
        int numOfInPages = (int) Math.ceil(1.0 * numOfInTuples / inCapacity);

        // Calculates the external sort cost.
        int numOfBuffer = BufferManager.getBuffersPerJoin();
        cost += getExternalSortCost(numOfInPages, numOfBuffer);

        return numOfInTuples;
    }

    /**
     * Calculates the cost of performing an external sort.
     *
     * @param numOfPages is the number of input pages.
     * @param numOfBuffer is the number of buffer pages available.
     * @return the cost of this sorting process.
     */
    private int getExternalSortCost(int numOfPages, int numOfBuffer) {
        int numOfSortedRuns = numOfPages / numOfBuffer;
        int numOfPasses = (int) Math.ceil(Math.log(numOfSortedRuns) / Math.log(numOfBuffer - 1)) + 1;
        return 2 * numOfPages * numOfPasses;
    }
}
