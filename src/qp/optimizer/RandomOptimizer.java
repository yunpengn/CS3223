package qp.optimizer;

import java.util.Vector;

import qp.operators.BlockNestedJoin;
import qp.operators.Debug;
import qp.operators.Distinct;
import qp.operators.Groupby;
import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.OpType;
import qp.operators.Operator;
import qp.operators.PageNestedJoin;
import qp.operators.Project;
import qp.operators.Select;
import qp.operators.Sort;
import qp.operators.SortMergeJoin;
import qp.utils.Attribute;
import qp.utils.RandomNum;
import qp.utils.SQLQuery;

/**
 * Performs randomized optimization, and iterative improvement algorithm.
 */
public abstract class RandomOptimizer {
    // Enumeration of different ways to find the neighbor plan.

    // Selects neighbor by changing a method for an operator.
    private static final int METHOD_CHOICE = 0;
    // Selects neighbor by rearranging the operators by commutative rule.
    private static final int COMMUTATIVE = 1;
    // Selects neighbor by rearranging the operators by associative rule.
    private static final int ASSOCIATIVE = 2;
    // Number of alternative methods available for a node as specified above.
    private static final int NUM_OF_CHOICES = 3;

    // Vector of Vectors of Select + From + Where + GroupBy.
    SQLQuery sqlQuery;

    // Number of joins in this query plan.
    int numOfJoin;

    /**
     * Constructor of RandomOptimizer.
     *
     * @param sqlQuery is the SQL query to be optimized.
     */
    public RandomOptimizer(SQLQuery sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    /**
     * Performs a random walk in the search space and gets an optimal (or at least sub-optimal) plan.
     *
     * @return the optimal plan generated.
     */
    public abstract Operator getOptimizedPlan();

    /**
     * Randomly selects a neighbor.
     *
     * @param root is the root of the query plan tree.
     * @return the neighboring plan.
     */
    Operator getNeighbor(Operator root) {
        // Randomly selects a node to be altered to get the neighbour.
        int nodeNum = RandomNum.randInt(0, numOfJoin - 1);
        // Randomly selects type of alteration: Change Method / Associative / Commutative.
        int changeType = RandomNum.randInt(0, NUM_OF_CHOICES - 1);

        switch (changeType) {
            case METHOD_CHOICE:
                // Selects a neighbour by changing the method type.
                return Transformations.neighborMethod(root, nodeNum);
            case COMMUTATIVE:
                return Transformations.neighborCommutativity(root, nodeNum);
            case ASSOCIATIVE:
                return Transformations.neighborAssociativity(root, nodeNum);
            default:
                return root;
        }
    }

    /**
     * Prepares an execution plan by replacing the methods with corresponding
     * join operator implementation, after finding a choice of method for
     * each operator.
     *
     * @param node is the query plan.
     * @return the execution plan.
     */
    public static Operator makeExecPlan(Operator node) {
        int numOfBuff = BufferManager.getBuffersPerJoin();
        if (node.getOpType() == OpType.JOIN) {
            Operator left = makeExecPlan(((Join) node).getLeft());
            Operator right = makeExecPlan(((Join) node).getRight());
            int joinType = ((Join) node).getJoinType();

            switch (joinType) {
                case JoinType.PAGE_NESTED_JOIN:
                    PageNestedJoin pnj = new PageNestedJoin((Join) node);
                    pnj.setLeft(left);
                    pnj.setRight(right);
                    pnj.setNumOfBuffer(numOfBuff);
                    return pnj;

                case JoinType.BLOCK_NESTED_JOIN:
                    BlockNestedJoin bnj = new BlockNestedJoin((Join) node);
                    bnj.setLeft(left);
                    bnj.setRight(right);
                    bnj.setNumOfBuffer(numOfBuff);
                    return bnj;

                case JoinType.SORT_MERGE_JOIN:
                    SortMergeJoin smj = new SortMergeJoin((Join) node);

                    Vector<Attribute> leftAttrs = new Vector<>();
                    leftAttrs.add(smj.getCondition().getLeft());
                    smj.setLeft(new Sort(left, leftAttrs, numOfBuff));

                    Vector<Attribute> rightAttrs = new Vector<>();
                    rightAttrs.add((Attribute) smj.getCondition().getRight());
                    smj.setRight(new Sort(right, rightAttrs, numOfBuff));

                    smj.setNumOfBuffer(numOfBuff);
                    return smj;

                case JoinType.HASH_JOIN:
                    PageNestedJoin hj = new PageNestedJoin((Join) node);
                    // Add other code here.

                    return hj;

                case JoinType.INDEX_NESTED_JOIN:
                    PageNestedJoin inj = new PageNestedJoin((Join) node);
                    // Add other code here.

                    return inj;

                default:
                    return node;
            }
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = makeExecPlan(((Select) node).getBase());
            ((Select) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = makeExecPlan(((Project) node).getBase());
            ((Project) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.DISTINCT) {
            Distinct operator = (Distinct) node;
            operator.setNumOfBuffer(numOfBuff);
            Operator base = makeExecPlan(operator.getBase());
            operator.setBase(base);
            return node;
        } else if (node.getOpType() == OpType.GROUPBY) {
            Groupby operator = (Groupby) node;
            operator.setNumOfBuffer(numOfBuff);
            Operator base = makeExecPlan(operator.getBase());
            operator.setBase(base);
            return node;
        } else {
            return node;
        }
    }

    /**
     * Prints out the information about an execution plan and its cost.
     *
     * @param name is the name of this plan.
     * @param plan is the execution plan.
     * @return the cost of this execution plan.
     */
    int printPlanCostInfo(String name, Operator plan) {
        PlanCost planCost = new PlanCost();
        int cost = planCost.getCost(plan);

        printPlanCostInfo(name, plan, cost);
        return cost;
    }

    /**
     * Prints out the information about an execution plan and its cost.
     *
     * @param name is the name of this plan.
     * @param plan is the execution plan.
     * @param cost is the cost of this execution plan.
     */
    void printPlanCostInfo(String name, Operator plan, int cost) {
        System.out.println("---------------------------" + name + "---------------------------");
        Debug.PPrint(plan);
        System.out.println(" " + cost);
    }
}
