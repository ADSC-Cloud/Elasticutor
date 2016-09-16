package backtype.storm.elasticity.scheduler.model;

import backtype.storm.elasticity.scheduler.ElasticScheduler;

/**
 * Created by robert on 16-8-10.
 */
public class NaivePredictor implements ExecutorParallelismPredictor {
    final double overProvisioningFactor = 0.5;
    @Override
    public int predict(Double inputRate, int currentDop, Double ratePerTask, long[] routeLoads, long maxShardLoad, boolean isSaturated) {
        Double performanceFactor = ElasticScheduler.getPerformanceFactor(routeLoads);
        return (int)Math.ceil(inputRate / (ratePerTask * performanceFactor) + overProvisioningFactor);
    }
}
