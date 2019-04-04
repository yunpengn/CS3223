package qp.optimizer;

import qp.operators.Operator;
import qp.utils.SQLQuery;

/**
 * Defines a randomized query optimizer using the Iterative Improvement (II) algorithm.
 */
public class RandomII extends RandomOptimizer {
    private static final int MAX_LOCAL_OPTIMIZATIONS = 10;
    /**
     * Constructor of RandomII.
     *
     * @param sqlQuery is the SQL query to be optimized.
     */
    public RandomII(SQLQuery sqlQuery) {
        super(sqlQuery);
    }

    /**
     * Implements an iterative improvement algorithm for query plan.
     *
     * @return the optimized plan.
     */
    public Operator getOptimizedPlan() {
        // Gets an initial plan for the given sql query.
        RandomInitialPlan rip = new RandomInitialPlan(sqlQuery);
        numOfJoin = rip.getNumJoins();

        // The final plan.
        Operator finalPlan = null;
        int finalCost = Integer.MAX_VALUE;

        // Premature exits if there is no join in the query.
        if (numOfJoin == 0) {
            finalPlan = rip.prepareInitialPlan();
            printPlanCostInfo("Final Plan", finalPlan);
            return finalPlan;
        }

        // Number of local optimizations performed.
        int localOptCount = 0;

        // Randomly restarts the gradient descent algorithm for a specified number of times.
        for (int j = 0; j < 3 * numOfJoin && localOptCount < MAX_LOCAL_OPTIMIZATIONS; j++) {
            Operator initPlan = rip.prepareInitialPlan();
            Transformations.modifySchema(initPlan);
            int initCost = printPlanCostInfo("Initial Plan", initPlan);

            // A flag to determine whether we have reached local minimum.
            boolean flag = true;

            while (flag) {
                // Just for initialization purpose.
                Operator minNeighborPlan = initPlan;
                int minNeighborCost = initCost;
                System.out.println("---------------while---------------");

                // In this loop we consider from the possible neighbors (randomly selected)
                // and take the minimum among for next step.
                for (int i = 0; i < 2 * numOfJoin; i++) {
                    Operator initPlanCopy = (Operator) initPlan.clone();
                    Operator neighbor = getNeighbor(initPlanCopy);
                    int neighborCost = printPlanCostInfo("Neighbor", neighbor);

                    if (neighborCost < minNeighborCost) {
                        minNeighborPlan = neighbor;
                        minNeighborCost = neighborCost;
                    }
                }

                if (minNeighborCost < initCost) {
                    initPlan = minNeighborPlan;
                    initCost = minNeighborCost;
                } else {
                    // Reaches local minimum.
                    flag = false;
                }
            }

            printPlanCostInfo("Local Minimum", initPlan, initCost);
            if (initCost < finalCost) {
                finalPlan = initPlan;
                finalCost = initCost;
                localOptCount++;
            }
        }

        printPlanCostInfo("Final Plan", finalPlan, finalCost);
        return finalPlan;
    }
}
