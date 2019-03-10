package qp.operators;

/**
 * Defines all possible join algorithms. Changes this class depending on the algorithms
 * that are actually implemented.
 */
public class JoinType {
    public static final int NESTED_JOIN = 0;
    public static final int BLOCK_NESTED = 1;
    public static final int SORT_MERGE = 2;
    public static final int HASH_JOIN = 3;
    public static final int INDEX_NESTED = 4;

    /**
     * @return the number of join types.
     */
    public static int numJoinTypes() {
        return 1;
        // return k for k joins
    }
}
