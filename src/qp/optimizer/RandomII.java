package qp.optimizer;

import qp.operators.Debug;
import qp.operators.Operator;
import qp.utils.SQLQuery;

/**
 * Defines a randomized query optimizer using the Iterative Improvement (II) algorithm.
 */
public class RandomII extends RandomOptimizer {
    /**
     * Constructor of RandomOptimizer.
     *
     * @param sqlQuery is the SQL query to be optimized.
     */
    public RandomII(SQLQuery sqlQuery) {
        super(sqlQuery);
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
        int numOfRestart = Math.max(2 * numOfJoin, 1);

        // Randomly restarts the gradient descent until the maximum specified number
        // of random restarts (numOfRestart) has satisfied.
        for (int j = 0; j < numOfRestart; j++) {
            Operator initPlan = rip.prepareInitialPlan();
            Transformations.modifySchema(initPlan);

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
}
