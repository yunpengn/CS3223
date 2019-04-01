package qp.utils;

/**
 * An utility class for generation of random numbers.
 *
 * @apiNote this class is useful for randomized optimizer.
 */
public class RandomNum {
    /**
     * Generates a random number in a given range.
     *
     * @param a is the lower limit of the range (inclusive).
     * @param b is the upper limit of the range (inclusive).
     * @return a random number in the required range.
     */
    public static int randInt(int a, int b) {
        return ((int) (Math.floor(Math.random() * (b - a + 1)) + a));
    }

    /**
     * Simulates a flip-coin experiment.
     * @return true if the experiment is a coin head.
     */
    public static boolean flipCoin() {
        return Math.random() < 0.5;
    }

    /**
     * Generates a random double-precision floating-point number, in the range of [0, 1).
     *
     * @return the random number generated.
     */
    public static double randDouble() {
        return Math.random();
    }
}
