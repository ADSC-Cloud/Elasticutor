package backtype.storm.elasticity.utils.generator;

/**
 * Created by robert on 7/12/16.
 */
public class RoundRobinKeyGenerator implements KeyGenerator {

    int numberOfKeys;
    int seed = 0;

    public RoundRobinKeyGenerator(int numberOfKeys) {
        this.numberOfKeys = numberOfKeys;
    }

    @Override
    public int generate() {
        seed = (seed + 1) % numberOfKeys;
        return seed;
    }
}
