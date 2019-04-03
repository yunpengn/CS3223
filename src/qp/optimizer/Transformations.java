package qp.optimizer;

import java.util.Vector;

import qp.operators.Distinct;
import qp.operators.Groupby;
import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.OpType;
import qp.operators.Operator;
import qp.operators.Project;
import qp.operators.Select;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.RandomNum;

/**
 * Defines all the legal transformation rules.
 */
class Transformations {
    /**
     * Selects a random method choice for join with joinNum. i.e., Nested Loop Join,
     * Sort-Merge Join, Hash Join, etc.
     *
     * @param root    is the root of the query plan tree.
     * @param joinNum is the randomly selected node number.
     * @return the modified plan.
     */
    static Operator neighborMethod(Operator root, int joinNum) {
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
    static Operator neighborCommutativity(Operator root, int joinNum) {
        System.out.println("------------------neighbor by commutative---------------");

        // Finds the node to be altered.
        Join node = (Join) findNodeAt(root, joinNum);
        Operator left = node.getLeft();
        Operator right = node.getRight();
        node.setLeft(right);
        node.setRight(left);

        // Flips the condition. i.e.,  A X a1b1 B = B X b1a1 A
        node.getCondition().flip();

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
    static Operator neighborAssociativity(Operator root, int joinNum) {
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
    private static void transformLeftToRight(Join op, Join left) {
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
    private static void transformRightToLeft(Join op, Join right) {
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
    private static Operator findNodeAt(Operator node, int joinNum) {
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
        } else if (node.getOpType() == OpType.DISTINCT) {
            return findNodeAt(((Distinct) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.GROUPBY) {
            return findNodeAt(((Groupby) node).getBase(), joinNum);
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
    static void modifySchema(Operator node) {
        Operator base;

        switch (node.getOpType()) {
            case OpType.JOIN:
                Operator left = ((Join) node).getLeft();
                Operator right = ((Join) node).getRight();

                modifySchema(left);
                modifySchema(right);
                node.setSchema(left.getSchema().joinWith(right.getSchema()));
                break;
            case OpType.SELECT:
                base = ((Select) node).getBase();
                modifySchema(base);
                node.setSchema(base.getSchema());
                break;
            case OpType.PROJECT:
                base = ((Project) node).getBase();
                Vector attrList = ((Project) node).getProjectAttr();
                modifySchema(base);
                node.setSchema(base.getSchema().subSchema(attrList));
                break;
            case OpType.DISTINCT:
                base = ((Distinct) node).getBase();
                modifySchema(base);
                node.setSchema(base.getSchema());
                break;
            case OpType.GROUPBY:
                base = ((Groupby) node).getBase();
                modifySchema(base);
                node.setSchema(base.getSchema());
                break;
        }
    }
}
