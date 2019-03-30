package qp.utils;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class RandomNumTest {
    @Test
    public void randInt() {
        for (int i = 0; i < 100; i++) {
            int result = RandomNum.randInt(10, 200);
            assertTrue("result should be not smaller than lower bound", result >= 10);
            assertTrue("result should be not larger than upper bound", result <= 200);
        }
    }
}
