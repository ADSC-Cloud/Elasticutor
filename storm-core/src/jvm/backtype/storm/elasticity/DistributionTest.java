package backtype.storm.elasticity;


import backtype.storm.elasticity.routing.BalancedHashRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.scheduler.ElasticScheduler;
import backtype.storm.elasticity.scheduler.ShardReassignment;
import backtype.storm.elasticity.scheduler.ShardReassignmentPlan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by robert on 18/9/16.
 */
public class DistributionTest {
    public static void main(String[] args) throws Exception {
        String file = "/home/wangli/data/poc/SB20100913.txt";
        BufferedReader reader = new BufferedReader(new FileReader(file));


        int round = 10000;
        int shards = 512;
        int routes = 64;

        BalancedHashRouting routingTable = new BalancedHashRouting(routes);
        BalancedHashRouting originalRoutingTable = new BalancedHashRouting(routes);
        routingTable.enableRoutingDistributionSampling();
        originalRoutingTable.enableRoutingDistributionSampling();
        while(true) {
            Thread.sleep(1000);
            System.out.println("Random: " + new Random(100).nextInt(10));
            final Map<Integer, Long> distribution = new HashMap<>();
            for(int i = 0; i < round; i++) {
                String line = reader.readLine();
                String[] values = line.split("\\|");
                int secCode = Integer.parseInt(values[11]);
                secCode += (new Random(secCode).nextInt(5)*1000 + new Random().nextInt(5) + 1);
//                int secCode = Integer.parseInt(values[11]);
                routingTable.route(secCode);
                originalRoutingTable.route(secCode);
                if(!distribution.containsKey(secCode)) {
                    distribution.put(secCode, 0L);
                }
                distribution.put(secCode, distribution.get(secCode) + 1);
            }

            System.out.println("Cardinality: " + distribution.size());

            int maxSecCode = -100;
            long max = 0;
            for(int i: distribution.keySet()) {
                if(max < distribution.get(i)) {
                    max = distribution.get(i);
                    maxSecCode = i;
                }
            }
            System.out.println(String.format("Max secCode %d: %d", maxSecCode, max));


//            long[] routeCount = new long[routes];
            List<Long> routeCount =  originalRoutingTable.getRoutingDistribution().histogramsToArrayList();

            for(Integer code: distribution.keySet()) {
                int index = code.hashCode()%shards%routes;
                routeCount.set(index, routeCount.get(index)+ distribution.get(code));
            }

            Collections.sort(routeCount);
            Collections.reverse(routeCount);
//            Arrays.sort(routeCount);

            for(Long l: routeCount) {
                System.out.print(l + " ");
            }

            System.out.println("\nPerformance Factor: " + ElasticScheduler.getPerformanceFactor(originalRoutingTable));

            long maxShardLoad = 0;
            for(long i: routingTable.getBucketsDistribution().histogramsToArrayList()) {
                maxShardLoad = Math.max(i, maxShardLoad);
            }
            System.out.println("Max shard: " + maxShardLoad);

            ShardReassignmentPlan plan = ElasticScheduler.getMinimizedShardToTaskReassignment(0, routingTable.getNumberOfRoutes(), routingTable.getBucketToRouteMapping(), routingTable.getBucketsDistribution());

            System.out.println(String.format("%d reassignments will be performed!", plan.getReassignmentList().size()));

            for(ShardReassignment reassignment: plan.getReassignmentList()) {
                routingTable.reassignBucketToRoute(reassignment.shardId, reassignment.newRoute);
            }
            System.out.println("\n optimized Performance Factor: " + ElasticScheduler.getPerformanceFactor(routingTable));

            System.out.println("=======================");

        }
    }
    static  double getPerformanceFactor(long[] routeLoads) {
        long loadSum = 0;
        long loadMin = Long.MAX_VALUE;
        long loadMax = Long.MIN_VALUE;
        for(Long i: routeLoads) {
            loadSum += i;
        }


        double ratio = 1;

        double standardLoad = loadSum / (double)routeLoads.length;

        for(Long i: routeLoads) {
            if(i != 0)
                ratio = Math.min(ratio, standardLoad / i);
        }

        return ratio;
    }
}
