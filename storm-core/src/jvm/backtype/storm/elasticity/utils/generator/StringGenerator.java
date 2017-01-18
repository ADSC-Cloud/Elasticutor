package backtype.storm.elasticity.utils.generator;

import java.util.Random;

/**
 * Created by robert on 1/8/16.
 */
public class StringGenerator {

    int numberOfDistinctValues;

    Random random = new Random();

    public StringGenerator(int numberOfDistinctValues) {
        this.numberOfDistinctValues = numberOfDistinctValues;
    }

    public String nextString() {
        return Integer.toString(random.nextInt(numberOfDistinctValues));
    }
}
