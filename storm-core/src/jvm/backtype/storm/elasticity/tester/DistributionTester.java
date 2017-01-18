package backtype.storm.elasticity.tester;

import backtype.storm.elasticity.routing.TwoTireRouting;
import backtype.storm.elasticity.utils.generator.RoundRobinKeyGenerator;
import backtype.storm.elasticity.utils.generator.ZipfKeyGenerator;

import javax.crypto.KeyGenerator;

/**
 * Created by robert on 11/1/17.
 */
public class DistributionTester {

    public static void main(String[] args) {
        int numberOfKeys = 256;
        long numberOfTuples = 1024 * 32;
        int numberOfShard = 256;
        int numberOfExecutors = 4;
        double skewness = 1.5;

        for(double s = 0; s <= 1.00001; s+= 0.01) {
           evaluate(numberOfTuples, numberOfKeys, numberOfShard, numberOfExecutors, s);
        }

    }

    static void evaluate(long tuples, int keys, int shards, int executors, double skewness) {
        backtype.storm.elasticity.utils.generator.KeyGenerator generator;
        if (skewness > 0)
            generator = new ZipfKeyGenerator(keys, skewness);
        else
            generator = new RoundRobinKeyGenerator(keys);

        TwoTireRouting routing = new TwoTireRouting(executors, shards);


        long[] executorCounts = new long[executors];
        long[] keyCounts = new long[keys];

        for(long i = 0; i < tuples; i++) {
            final int key = generator.generate();
            keyCounts[key]++;
//            final int executorId = routing.route(key).route;
            final int executorId = hash(key, executors);
            executorCounts[executorId]++;
        }

        double maxExecutorRate = 0;
        for (long count: executorCounts) {
            maxExecutorRate = Math.max(maxExecutorRate, count / (double) tuples);
        }

        double maxKeyRate = 0;
        for (long count: keyCounts) {
            maxKeyRate = Math.max(maxKeyRate, count / (double) tuples);
        }

//        System.out.println(String.format("executors: %d, shards: %d, keys: %d, skewness: %2.2f, max executor rate: %4.4f," +
//                        "max key rate: %4.4f",
//                executors, shards, keys, skewness, maxExecutorRate, maxKeyRate));
        System.out.println(String.format("%2.5f\t%2.5f", maxExecutorRate, maxKeyRate));
    }

    static int hash(int key, int maxValue) {
        return (int)(((long)key + 104183) * 104473 % maxValue);
    }

}
