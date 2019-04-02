package qp.utils;

import java.util.Vector;

/**
 * Defines the format of the parsed SQL query.
 */
public class SQLQuery {
    // List of projected attributes in the SELECT clause.
    private Vector projectList;
    // List of tables in the FROM clause.
    private Vector fromList;

    // List of conditions in the WHERE clause.
    private Vector conditionList;
    // List of select predicates only (a subset of conditionList).
    private Vector selectionList;
    // List of join predicates only (a subset of conditionList).
    private Vector joinList;

    // List of attributes in the GROUP_BY clause.
    private Vector groupByList;

    // Indicates whether DISTINCT appears in the SELECT clause.
    private boolean isDistinct = false;

    /**
     * Constructs a new SQL query object. Uses this constructor when there is no GROUP_BY clause.
     *
     * @param projectList is the project list.
     * @param fromList is the from list.
     * @param conditionList is the condition list.
     */
    public SQLQuery(Vector projectList, Vector fromList, Vector conditionList) {
        this.projectList = projectList;
        this.fromList = fromList;
        this.conditionList = conditionList;
        this.groupByList = null;
        splitConditionList(conditionList);
    }

    /**
     * Constructs a new SQL query object. Uses this constructor when there is neither WHERE nor
     * GROUP_BY clause.
     *
     * @param projectList is the project list.
     * @param fromList is the from list.
     */
    public SQLQuery(Vector projectList, Vector fromList) {
        this.projectList = projectList;
        this.fromList = fromList;
        this.conditionList = null;
        this.groupByList = null;
        splitConditionList(new Vector());
    }

    /**
     * Splits the condition list into selection & join predicates.
     *
     * @param tempVector is condition list.
     */
    private void splitConditionList(Vector tempVector) {
        selectionList = new Vector();
        joinList = new Vector();

        for (int i = 0; i < tempVector.size(); i++) {
            Condition cn = (Condition) tempVector.elementAt(i);
            if (cn.getCondType() == Condition.SELECT) {
                selectionList.add(cn);
            } else {
                joinList.add(cn);
            }
        }
    }

    /**
     * Getter for project list.
     *
     * @return the project list.
     */
    public Vector getProjectList() {
        return projectList;
    }

    /**
     * Getter for from list.
     *
     * @return the from list.
     */
    public Vector getFromList() {
        return fromList;
    }

    /**
     * Getter for selection predicates.
     *
     * @return the selection predicates.
     */
    public Vector getSelectionList() {
        return selectionList;
    }

    /**
     * Getter for join predicates.
     *
     * @return the join predicates.
     */
    public Vector getJoinList() {
        return joinList;
    }

    /**
     * Setter for groupBy list.
     *
     * @param list is the groupBy list.
     */
    public void setGroupByList(Vector list) {
        groupByList = list;
    }

    /**
     * Getter for groupBy list.
     *
     * @return the groupBy list.
     */
    public Vector getGroupByList() {
        return groupByList;
    }

    /**
     * @return the number of joins in this SQL query.
     */
    public int getNumJoin() {
        if (joinList == null) {
            return 0;
        }

        return joinList.size();
    }

    /**
     * Setter for isDistinct.
     *
     * @param isDistinct means whether DISTINCT is present in the query.
     */
    public void setIsDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    /**
     * Getter for isDistinct.
     *
     * @return whether DISTINCT is present in the query.
     */
    public boolean getIsDistinct() {
        return this.isDistinct;
    }

    /**
     * @return whether GROUPBY is present in the query.
     */
    public boolean isGroupby() {
        return groupByList != null && !groupByList.isEmpty();
    }
}
