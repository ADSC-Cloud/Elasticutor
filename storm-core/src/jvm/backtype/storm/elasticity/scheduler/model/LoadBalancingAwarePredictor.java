package backtype.storm.elasticity.scheduler.model;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.scheduler.ElasticScheduler;

/**
 * This class predicate the desirable parallelism of an elastic executor, with consideration on the workload distributions
 * of the tasks.
 */
public class LoadBalancingAwarePredictor implements ExecutorParallelismPredictor {

    @Override
    public int predict(Double inputRate, int currentDop, Double ratePerTask, long[] routeLoads, long maxShardLoad, boolean isSaturated) {
        final double overProvisionFactor = 1.5;

        final double overProvisionForAGivenDoP = (currentDop + overProvisionFactor) / currentDop;

        long totalWorkload = 0;
        for(long i: routeLoads) {
            totalWorkload += i;
        }

        // The maximum throughput for given workload distribution of the shards.
        final double maxProcessingThroughput = ratePerTask / (maxShardLoad / (double) totalWorkload);

//        Slave.getInstance().sendMessageToMaster(String.format("maxProcessingThroughputCapedByLoadBlanacing: %f"
//                , maxProcessingThroughput));

        int desirableDoP;



        boolean isBoundedBySkewness = false;

        // There are two ways to determine @isBoundedBySkewness

        // 1st way:
//        isBoundedBySkewness = inputRate * overProvisionForAGivenDoP > maxProcessingThroughput;

        // 2nd way:
        long maxRouteLoads = 0;
        for(long load: routeLoads) {
            maxRouteLoads = Math.max(maxRouteLoads, load);
        }
        isBoundedBySkewness = maxRouteLoads * 0.99 < maxShardLoad;


        if(isBoundedBySkewness) {
            Slave.getInstance().sendMessageToMaster("the performance is bounded by the load balancing.");
            desirableDoP = currentDop;
        } else if(isSaturated){
            Slave.getInstance().logOnMaster(String.format("Saturated: inputRate: %.2f, currentDop: %d, ratePerTask: %.2f", inputRate, currentDop, ratePerTask));
            Slave.getInstance().logOnMaster(String.format("MaxRouteLoad: %d, MaxShardLoad: %d", maxRouteLoads, maxShardLoad));
            desirableDoP = currentDop + 1;
        } else {

            Double performanceFactor = ElasticScheduler.getPerformanceFactor(routeLoads);
            Double processCapability = ratePerTask * currentDop * performanceFactor;

            desirableDoP = (int) Math.ceil(inputRate * overProvisionForAGivenDoP / processCapability * currentDop);

            if (desirableDoP > currentDop) {
                //When the shard with the highest workload is assigned to the most overloaded task, neither load balancing can
                // be improved by shard reassignments nor the processing capability can be enhanced by increasing the parallelism.
                // In such case, we should not scale up.
                maxRouteLoads = Long.MIN_VALUE;
                for (long i : routeLoads) {
                    maxRouteLoads = Math.max(i, maxRouteLoads);
                }

                // this means that there exists one extremely overloaded shard, which exceeds the processing capability of a task.
                // In such case, scaling out cannot improve the throughput but waste computation resource.
                if (maxRouteLoads == maxShardLoad)
                    desirableDoP = currentDop;
            }
        }

        if(desirableDoP > currentDop)
            return Math.max(1, currentDop + 1);
        else if (desirableDoP < currentDop)
            return Math.max(1, currentDop - 1);
        else
            return Math.max(1, currentDop);
    }
}
