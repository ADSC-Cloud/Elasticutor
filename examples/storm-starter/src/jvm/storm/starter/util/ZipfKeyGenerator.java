package storm.starter.util;

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
        return distribution.sample() - 1; // minus 1 is to ensure that the key starting from 0 instead of 1.
    }
}
