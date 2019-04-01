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

    @Override
    public Operator getOptimizedPlan() {
        return null;
    }
}
