package qp.optimizer;

import java.util.Vector;

import qp.operators.BlockNestedJoin;
import qp.operators.Debug;
import qp.operators.Distinct;
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
import qp.utils.Condition;
import qp.utils.RandomNum;
import qp.utils.SQLQuery;

/**
 * Performs randomized optimization, and iterative improvement algorithm.
 */
public class RandomOptimizer {
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
    private SQLQuery sqlQuery;

    // Number of joins in this query plan.
    private int numOfJoin;

    /**
     * Constructor of RandomOptimizer.
     */
    public RandomOptimizer(SQLQuery sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    /**
     * Randomly selects a neighbor.
     *
     * @param root is the root of the query plan tree.
     * @return the neighboring plan.
     */
    private Operator getNeighbor(Operator root) {
        // Randomly selects a node to be altered to get the neighbour.
        int nodeNum = RandomNum.randInt(0, numOfJoin - 1);

        // Randomly selects type of alteration: Change Method / Associative / Commutative.
        int changeType = RandomNum.randInt(0, NUM_OF_CHOICES - 1);
        Operator neighbor = null;

        switch (changeType) {
            // Selects a neighbour by changing the method type.
            case METHOD_CHOICE:
                neighbor = neighborMethod(root, nodeNum);
                break;
            case COMMUTATIVE:
                neighbor = neighborCommutativity(root, nodeNum);
                break;
            case ASSOCIATIVE:
                neighbor = neighborAssociativity(root, nodeNum);
                break;
        }
        return neighbor;
    }

    /**
     * Implements Iterative Improvement Algorithm or Randomized optimization of Query Plan.
     *
     * @return the optimized plan.
     */
    public Operator getOptimizedPlan() {
        // Gets an initial plan for the given sql query.
        RandomInitialPlan rip = new RandomInitialPlan(sqlQuery);
        numOfJoin = rip.getNumJoins();

        int minCost = Integer.MAX_VALUE;
        Operator finalPlan = null;

        // The number of times of random restart.
        int numOfRestart;
        if (numOfJoin != 0) {
            numOfRestart = 2 * numOfJoin;
        } else {
            numOfRestart = 1;
        }

        // Randomly restarts the gradient descent until the maximum specified number
        // of random restarts (numOfRestart) has satisfied.
        for (int j = 0; j < numOfRestart; j++) {
            Operator initPlan = rip.prepareInitialPlan();
            modifySchema(initPlan);

            System.out.println("-----------initial Plan-------------");
            Debug.PPrint(initPlan);

            PlanCost pc = new PlanCost();
            int initCost = pc.getCost(initPlan);
            System.out.println(initCost);

            boolean flag = true;
            // Just for initialization purpose.
            int minNeighborCost = initCost;
            Operator minNeighbor = initPlan;

            if (numOfJoin != 0) {
                // flag = false when local minimum is reached.
                while (flag) {
                    System.out.println("---------------while--------");

                    Operator initPlanCopy = (Operator) initPlan.clone();
                    minNeighbor = getNeighbor(initPlanCopy);

                    System.out.println("--------------------------neighbor---------------");
                    Debug.PPrint(minNeighbor);

                    pc = new PlanCost();
                    minNeighborCost = pc.getCost(minNeighbor);
                    System.out.println("  " + minNeighborCost);

                    // In this loop we consider from the possible neighbors (randomly selected)
                    // and take the minimum among for next step.
                    for (int i = 1; i < 2 * numOfJoin; i++) {
                        initPlanCopy = (Operator) initPlan.clone();
                        Operator neighbor = getNeighbor(initPlanCopy);

                        System.out.println("------------------neighbor--------------");
                        Debug.PPrint(neighbor);

                        pc = new PlanCost();
                        int neighborCost = pc.getCost(neighbor);
                        System.out.println(neighborCost);

                        if (neighborCost < minNeighborCost) {
                            minNeighbor = neighbor;
                            minNeighborCost = neighborCost;
                        }
                    }
                    if (minNeighborCost < initCost) {
                        initPlan = minNeighbor;
                        initCost = minNeighborCost;
                    } else {
                        minNeighbor = initPlan;
                        minNeighborCost = initCost;

                        // Reaches local minimum.
                        flag = false;
                    }
                }
                System.out.println("------------------local minimum--------------");
                Debug.PPrint(minNeighbor);
                System.out.println(" " + minNeighborCost);
            }
            if (minNeighborCost < minCost) {
                minCost = minNeighborCost;
                finalPlan = minNeighbor;
            }
        }
        System.out.println("\n\n\n");
        System.out.println("---------------------------Final Plan----------------");
        Debug.PPrint(finalPlan);
        System.out.println("  " + minCost);

        return finalPlan;
    }

    /**
     * Selects a random method choice for join with joinNum. i.e., Nested Loop Join,
     * Sort-Merge Join, Hash Join, etc.
     *
     * @param root    is the root of the query plan tree.
     * @param joinNum is the randomly selected node number.
     * @return the modified plan.
     */
    private Operator neighborMethod(Operator root, int joinNum) {
        System.out.println("------------------neighbor by method change----------------");

        int numJoinMethods = JoinType.numJoinTypes();
        if (numJoinMethods > 1) {
            // Finds the node that is to be altered.
            Join node = (Join) findNodeAt(root, joinNum);
            int prevJoinMethod = node.getJoinType();
            int joinMethod = RandomNum.randInt(0, numJoinMethods - 1);
            while (joinMethod == prevJoinMethod) {
                joinMethod = RandomNum.randInt(0, numJoinMethods - 1);
            }
            node.setJoinType(joinMethod);
        }
        return root;
    }

    /**
     * Applies join Commutativity for the join numbered with joinNum.
     * i.e., A X B  is changed as B X A
     *
     * @param root    is the root of the query plan tree.
     * @param joinNum is the randomly selected node number.
     * @return the modified plan.
     */
    private Operator neighborCommutativity(Operator root, int joinNum) {
        System.out.println("------------------neighbor by commutative---------------");

        // Finds the node to be altered.
        Join node = (Join) findNodeAt(root, joinNum);
        Operator left = node.getLeft();
        Operator right = node.getRight();
        node.setLeft(right);
        node.setRight(left);

        // Flips the condition. i.e.,  A X a1b1 B = B X b1a1 A
        node.getCondition().flip();
        // Schema newschem = left.getSchema().joinWith(right.getSchema());
        // node.setSchema(newschem);

        // Modifies the schema before returning the root.
        modifySchema(root);
        return root;
    }

    /**
     * Applies join Associativity for the join numbered with joinNum.
     * i.e., (A X B) X C is changed to A X (B X C)
     *
     * @param root    is the root of the query plan tree.
     * @param joinNum is the randomly selected node number.
     * @return the modified plan.
     */
    private Operator neighborAssociativity(Operator root, int joinNum) {
        // Finds the node to be altered.
        Join op = (Join) findNodeAt(root, joinNum);
        // Join op = (Join) joinOpList.elementAt(joinNum);
        Operator left = op.getLeft();
        Operator right = op.getRight();

        if (left.getOpType() == OpType.JOIN && right.getOpType() != OpType.JOIN) {
            transformLeftToRight(op, (Join) left);
        } else if (left.getOpType() != OpType.JOIN && right.getOpType() == OpType.JOIN) {
            transformRightToLeft(op, (Join) right);
        } else if (left.getOpType() == OpType.JOIN && right.getOpType() == OpType.JOIN) {
            if (RandomNum.flipCoin())
                transformLeftToRight(op, (Join) left);
            else
                transformRightToLeft(op, (Join) right);
        } else {
            // The join is just A X B, therefore Association rule is not applicable.
        }

        // Modifies the schema before returning the root.
        modifySchema(root);
        return root;
    }

    /**
     * This is the given plan (A X B) X C.
     *
     * @param op   is the node to be altered.
     * @param left is the left child of the node op.
     */
    private void transformLeftToRight(Join op, Join left) {
        System.out.println("------------------Left to Right neighbor--------------");

        Operator right = op.getRight();
        Operator leftLeft = left.getLeft();
        Operator leftRight = left.getRight();
        Attribute leftAttr = op.getCondition().getLeft();
        Join temp;

        // CASE 1 : (A X a1b1 B) X b4c4 C = A X a1b1 (B X b4c4 C)
        // a1b1, b4c4 are the join conditions for that join operator.
        if (leftRight.getSchema().contains(leftAttr)) {
            System.out.println("----------------CASE 1-----------------");

            temp = new Join(leftRight, right, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftLeft);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            op.setRight(temp);
            op.setCondition(left.getCondition());
        } else {
            System.out.println("--------------------CASE 2---------------");

            // CASE 2: (A X a1b1 B) X a4c4 C = B X b1a1 (A X a4c4 C)
            // a1b1, a4c4 are the join conditions for that join operator.
            temp = new Join(leftLeft, right, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftRight);
            op.setRight(temp);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            Condition newCondition = left.getCondition();
            newCondition.flip();
            op.setCondition(newCondition);
        }
    }

    /**
     * @param op    is the node to be altered.
     * @param right is the right child of the node op.
     */
    private void transformRightToLeft(Join op, Join right) {
        System.out.println("------------------Right to Left Neighbor------------------");

        Operator left = op.getLeft();
        Operator rightLeft = right.getLeft();
        Operator rightRight = right.getRight();
        Attribute rightAttr = (Attribute) op.getCondition().getRight();
        Join temp;

        // CASE 3 : A X a1b1 (B X b4c4 C) = (A X a1b1 B) X b4c4 C
        // a1b1, b4c4 are the join conditions at that join operator.
        if (rightLeft.getSchema().contains(rightAttr)) {
            System.out.println("----------------------CASE 3-----------------------");

            temp = new Join(left, rightLeft, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(temp);
            op.setRight(rightRight);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            op.setCondition(right.getCondition());
        } else {
            // CASE 4 : A X a1c1 (B X b4c4 C) = (A X a1c1 C) X c4b4 B
            // a1b1,  b4c4 are the join conditions at that join operator.
            System.out.println("-----------------------------CASE 4-----------------");

            temp = new Join(left, rightRight, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());

            op.setLeft(temp);
            op.setRight(rightLeft);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            Condition newCondition = right.getCondition();
            newCondition.flip();
            op.setCondition(newCondition);
        }
    }

    /**
     * Traverses through the query plan and returns the node specified by joinNum.
     *
     * @param node    is the query plan.
     * @param joinNum is the randomly selected node number.
     * @return the node specified by joinNum.
     */
    private Operator findNodeAt(Operator node, int joinNum) {
        if (node.getOpType() == OpType.JOIN) {
            if (((Join) node).getNodeIndex() == joinNum) {
                return node;
            } else {
                Operator temp;
                temp = findNodeAt(((Join) node).getLeft(), joinNum);
                if (temp == null)
                    temp = findNodeAt(((Join) node).getRight(), joinNum);
                return temp;
            }
        } else if (node.getOpType() == OpType.SCAN) {
            return null;
        } else if (node.getOpType() == OpType.SELECT) {
            // If sort / project / select operator.
            return findNodeAt(((Select) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.PROJECT) {
            return findNodeAt(((Project) node).getBase(), joinNum);
        } else {
            return null;
        }
    }

    /**
     * Modifies the schema of operators which are modified due to selecting
     * an alternative neighbor plan.
     *
     * @param node is the query plan.
     */
    private void modifySchema(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            Operator left = ((Join) node).getLeft();
            Operator right = ((Join) node).getRight();

            modifySchema(left);
            modifySchema(right);
            node.setSchema(left.getSchema().joinWith(right.getSchema()));
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = ((Select) node).getBase();

            modifySchema(base);
            node.setSchema(base.getSchema());
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = ((Project) node).getBase();
            Vector attrList = ((Project) node).getProjectAttr();

            modifySchema(base);
            node.setSchema(base.getSchema().subSchema(attrList));
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
        } else {
            return node;
        }
    }
}
