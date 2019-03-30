package qp.optimizer;

/**
 * Defines a simple buffer manager that distributes the buffers equally among all the
 * join operators.
 */
public class BufferManager {
    // The number of buffers for each join operator.
    private static int buffersPerJoin;

    /**
     * Constructor of BufferManager.
     *
     * @param numOfBuffer is the total number of buffers.
     * @param numOfJoin   is the total number of join operators.
     */
    public BufferManager(int numOfBuffer, int numOfJoin) {
        buffersPerJoin = numOfJoin == 0 ? numOfBuffer : numOfBuffer / numOfJoin;
    }

    /**
     * Getter for buffersPerJoin.
     *
     * @return the number of buffers available for each join operator.
     */
    public static int getBuffersPerJoin() {
        return buffersPerJoin;
    }
}
