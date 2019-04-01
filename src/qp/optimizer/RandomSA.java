package qp.optimizer;

import qp.operators.Operator;
import qp.utils.SQLQuery;

/**
 * Defines a randomized query optimizer using the Simulated Annealing (SA) algorithm.
 */
public class RandomSA extends RandomOptimizer {
    /**
     * Constructor of RandomOptimizer.
     *
     * @param sqlQuery is the SQL query to be optimized.
     */
    public RandomSA(SQLQuery sqlQuery) {
        super(sqlQuery);
    }

    /**
     * Implements an simulated annealing algorithm for query plan.
     *
     * @return the optimized plan.
     */
    @Override
    public Operator getOptimizedPlan() {
        // Gets an initial plan for the given sql query.
        RandomInitialPlan rip = new RandomInitialPlan(sqlQuery);
        numOfJoin = rip.getNumJoins();

        // The current & final plan.
        Operator minPlan = rip.prepareInitialPlan();
        int minCost = Integer.MAX_VALUE;
        int temperature = numOfJoin * 5;

        // Premature exits if there is no join in the query.
        if (numOfJoin == 0) {
            printPlanCostInfo("Final Plan", minPlan);
            return minPlan;
        }

        while (temperature > 0) {
            temperature--;
        }

        return null;
    }
}
