package backtype.storm.elasticity.routing;

import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.utils.GlobalHashFunction;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.elasticity.utils.SlideWindowKeyBucketSample;
import backtype.storm.utils.RateTracker;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

/**
 * Created by robert on 11/26/15.
 */
public class TwoTireRouting implements RoutingTable {

    private GlobalHashFunction hashFunction = GlobalHashFunction.getInstance();

    private int numberOfRoutes;

    private Map<Integer, Integer> shardToRoute;

    private int numberOfShards;

    private long signature = 0;

    transient SlideWindowKeyBucketSample sample;

    public TwoTireRouting(Map<Integer, Integer> hashValueToPartition, int numberOfRoutes) {
        this.numberOfRoutes = numberOfRoutes;
        shardToRoute = new HashMap<>();
        shardToRoute.putAll(hashValueToPartition);
        numberOfShards = hashValueToPartition.size();
    }

    public TwoTireRouting(int numberOfRoutes) {
        this(numberOfRoutes, Config.NumberOfShard);
    }

    public TwoTireRouting(int numberOfRoutes, int numberOfShards) {
        this.numberOfRoutes = numberOfRoutes;
        this.numberOfShards = numberOfShards;
        shardToRoute = new HashMap<>();
        for(int i = 0; i < numberOfShards; i++) {
            shardToRoute.put(i, i % numberOfRoutes);
        }
        enableSampling();
    }

    public TwoTireRouting(Map<Integer, Integer> hashValueToPartition, int numberOfRoutes, boolean enableSample) {
        this(hashValueToPartition, numberOfRoutes);
        if(enableSample)
            enableSampling();
    }

    public void enableSampling() {
        sample = new SlideWindowKeyBucketSample(numberOfShards);
        sample.enable();
    }

    public int computeShard(Object key) {
        return hashFunction.hash(key) % numberOfShards;
    }

    @Override
    public synchronized Route route(Object key) {
        final int shard = hashFunction.hash(key) % numberOfShards;
        if(sample!=null)
            sample.recordShard(shard);

        final int ret = shardToRoute.get(shard);

        return new Route(ret);
    }

    @Override
    public synchronized int getNumberOfRoutes() {
        return numberOfRoutes;
    }

    @Override
    public synchronized List<Integer> getRoutes() {
        List<Integer> ret = new ArrayList<>();
        for(int i=0; i<numberOfRoutes; i++) {
            ret.add(i);
        }
        return ret;
    }

    @Override
    public Histograms getRoutingDistribution() {
        Histograms bucketHistograms = getBucketsDistribution();
        Histograms histograms = new Histograms();
        histograms.setDefaultValueForAbsentKey(numberOfRoutes);
        for (int i = 0; i < numberOfShards; i++) {
            final long shardLoad = bucketHistograms.histograms.get(i);
            final int targetRoute = shardToRoute.get(i);
            histograms.histograms.put(targetRoute, histograms.histograms.get(targetRoute) + shardLoad);
        }
        return histograms;
    }


    @Override
    public long getSigniture() {
        return signature;
    }

    public Set<Integer> getBucketSet() {
        return shardToRoute.keySet();
    }

    public synchronized void reassignBucketToRoute(int bucketid, int targetRoute) {
        shardToRoute.put(bucketid, targetRoute);
        numberOfRoutes = Math.max(targetRoute + 1, numberOfRoutes);
        signature ++;
    }

    public synchronized String toString() {

        String ret = "Balanced Hash Routing: " + System.identityHashCode(this) + "\n";
        try {
            NumberFormat formatter = new DecimalFormat("#0.0000");

            ret += "number of routes: " + getNumberOfRoutes() + "\n";

            ArrayList<Integer>[] routeToBuckets = new ArrayList[numberOfRoutes];

            for (int i = 0; i < numberOfRoutes; i++) {
                routeToBuckets[i] = new ArrayList<>();
            }

            for (int bucket : shardToRoute.keySet()) {
                routeToBuckets[shardToRoute.get(bucket)].add(bucket);
            }

            for (ArrayList<Integer> list : routeToBuckets) {
                Collections.sort(list);
            }


            ret += "Route Details:\n";

            if (sample != null) {
                Double[] bucketFrequencies = sample.getFrequencies();

                for (int i = 0; i < routeToBuckets.length; i++) {
                    double sum = 0;
                    ret += "Route " + i + ": ";
                    for (Integer bucket : routeToBuckets[i]) {
                        sum += bucketFrequencies[bucket];
                        ret += bucket + " (" + formatter.format(bucketFrequencies[bucket]) + ")  ";
                    }
                    ret += "total = " + formatter.format(sum) + "\n";
                }
            } else {
                for (int i = 0; i < routeToBuckets.length; i++) {
                    ret += "Route " + i + ": ";
                    for (Integer bucket : routeToBuckets[i]) {
                        ret += bucket + "  ";
                    }
                    ret += "\n";
                }
            }


//        ret += hashValueToRoute;
//        ret +="\n";
            return ret;
        }
        catch (Exception e) {
            System.out.println("There is something wrong with the routing table!");
            System.out.println("Number of route: " + numberOfRoutes);
            System.out.println("Shard to Route mapping:");
            for(int shard: shardToRoute.keySet()) {
                System.out.println(shard + "." + shardToRoute.get(shard));
            }
            System.out.println();

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);

            return ret + " routing table cannot convert to String due to " + sw.toString();
        }
    }

    public int getNumberOfBuckets() {
        return numberOfShards;
    }

    public Histograms getBucketsDistribution() {
        Histograms ret = sample.getDistribution();
        ret.setDefaultValueForAbsentKey(numberOfShards);

        return ret;
    }

    public Map<Integer, Integer> getBucketToRouteMapping() {
        return this.shardToRoute;
    }

    public synchronized void setBucketToRouteMapping( Map<Integer, Integer> newMapping) {
        this.shardToRoute.clear();
        this.shardToRoute.putAll(newMapping);
        signature ++;
    }

    public synchronized void update(TwoTireRouting newRoute) {
        this.numberOfRoutes = newRoute.numberOfRoutes;
        this.setBucketToRouteMapping(newRoute.getBucketToRouteMapping());
    }

    /**
     * create a new route (empty route)
     * @return new route id
     */
    @Override
    public synchronized int scalingOut() {
        signature ++;
        numberOfRoutes++;
        return numberOfRoutes - 1;
    }

    @Override
    public synchronized void scalingIn() {
        signature ++;
        int largestSubtaskIndex = numberOfRoutes - 1;
        for(int shard: shardToRoute.keySet()) {
            if(shardToRoute.get(shard) == largestSubtaskIndex)
                throw new RuntimeException("There is at least one shard ("+ shard +") assigned to the Subtask with the largest index ("+ largestSubtaskIndex + "). Scaling in fails!");
        }
        numberOfRoutes--;

    }

    public static void main(String[] args) {
        final int numberOfRoutes = 8;
        final RoutingTable routingTable = new TwoTireRouting(numberOfRoutes);
        final RateTracker rateTracker = new RateTracker(1000, 5);

        new Thread(new Runnable() {
            @Override
            public void run() {
                long key = 0;
                Random random = new Random();
                while(true) {

                    routingTable.route(random.nextLong());
                    rateTracker.notify(1);
                }
            }
        }).start();

        new Timer().scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                System.out.println("Throughput: " + rateTracker.reportRate());
                System.out.println("Route Histogram: " + routingTable.getRoutingDistribution());
            }
        }, 0, 1000);
    }
}
