package backtype.storm.elasticity.scheduler.model;

/**
 * Created by robert on 16-8-10.
 */
public interface ExecutorParallelismPredictor {

    int predict(Double inputRate, int currentDop, Double ratePerTask, long[] routeLoads, long maxShardLoad, boolean isSaturated);
}
