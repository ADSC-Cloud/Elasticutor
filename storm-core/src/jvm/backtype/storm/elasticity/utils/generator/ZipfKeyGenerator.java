package backtype.storm.elasticity.utils.generator;

import org.apache.commons.math3.distribution.ZipfDistribution;

/**
 * Created by robert on 7/12/16.
 */
public class ZipfKeyGenerator implements KeyGenerator {

    ZipfDistribution distribution;

    public ZipfKeyGenerator(int numberOfKeys, double skewness) {
        distribution = new ZipfDistribution(numberOfKeys, skewness);
    }

    @Override
    public int generate() {
        return distribution.sample() - 1;
    }
}
