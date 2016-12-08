package backtype.storm.elasticity.routing;

import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.utils.GlobalHashFunction;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.elasticity.utils.SlideWindowKeyBucketSample;
import backtype.storm.elasticity.utils.SlidingWindowRouteSampler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

/**
 * Created by robert on 11/26/15.
 */
public class BalancedHashRouting implements RoutingTable {

    GlobalHashFunction hashFunction = GlobalHashFunction.getInstance();

    int numberOfRoutes;

    Map<Integer, Integer> hashValueToRoute;

    int numberOfHashValues;

    long signature = 0;

    transient SlideWindowKeyBucketSample sample;
    transient SlidingWindowRouteSampler routeDistributionSampler;

    public BalancedHashRouting(Map<Integer, Integer> hashValueToPartition, int numberOfRoutes) {
        this.numberOfRoutes = numberOfRoutes;
        hashValueToRoute = new HashMap<>();
        hashValueToRoute.putAll(hashValueToPartition);
        numberOfHashValues = hashValueToPartition.size();
    }

    public BalancedHashRouting(int numberOfRoutes) {
        this.numberOfRoutes = numberOfRoutes;
        numberOfHashValues = Config.NumberOfShard;
        hashValueToRoute = new HashMap<>();
        for(int i = 0; i < numberOfHashValues; i++) {
            hashValueToRoute.put(i, i % numberOfRoutes);
        }
        enableSampling();
    }

    public BalancedHashRouting(Map<Integer, Integer> hashValueToPartition, int numberOfRoutes, boolean enableSample) {
        this(hashValueToPartition, numberOfRoutes);
        if(enableSample)
            enableSampling();
    }

    public void enableSampling() {
        sample = new SlideWindowKeyBucketSample(numberOfHashValues);
        sample.enable();
    }

    @Override
    public synchronized int route(Object key) {
        final int shard = hashFunction.hash(key) % numberOfHashValues;
        if(sample!=null)
            sample.record(shard);

        final int ret = hashValueToRoute.get(shard);
        if(routeDistributionSampler != null)
            routeDistributionSampler.record(ret);

        return ret;
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
        return routeDistributionSampler.getDistribution();
    }

    @Override
    public synchronized void enableRoutingDistributionSampling() {
        routeDistributionSampler = new SlidingWindowRouteSampler(numberOfRoutes);
        routeDistributionSampler.enable();
    }

    @Override
    public long getSigniture() {
        return signature;
    }

    public Set<Integer> getBucketSet() {
        return hashValueToRoute.keySet();
    }

    public synchronized void reassignBucketToRoute(int bucketid, int targetRoute) {
        hashValueToRoute.put(bucketid, targetRoute);
        numberOfRoutes = Math.max(targetRoute + 1, numberOfRoutes);
        signature ++;
//        ElasticTaskHolder.instance()._slaveActor.sendMessageToMaster(bucketid + ", " + targetRoute + " is put!");
    }

    public synchronized String toString() {

//        ArrayList<ArrayList<Integer>> routeToBuckets = new ArrayList<ArrayList<Integer>>();
//        for(int i=0; i < numberOfRoutes; i++ ) {
//            routeToBuckets.add(new ArrayList<Integer>());// = new ArrayList<>();
//        }
        String ret = "Balanced Hash Routing: " + System.identityHashCode(this) + "\n";
        try {
            NumberFormat formatter = new DecimalFormat("#0.0000");

            ret += "number of routes: " + getNumberOfRoutes() + "\n";

            ArrayList<Integer>[] routeToBuckets = new ArrayList[numberOfRoutes];

            for (int i = 0; i < numberOfRoutes; i++) {
                routeToBuckets[i] = new ArrayList<>();
            }

            for (int bucket : hashValueToRoute.keySet()) {
                routeToBuckets[hashValueToRoute.get(bucket)].add(bucket);
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
            for(int shard: hashValueToRoute.keySet()) {
                System.out.println(shard + "." + hashValueToRoute.get(shard));
            }
            System.out.println();

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);

            return ret + " routing table cannot convert to String due to " + sw.toString();
        }
    }

    public int getNumberOfBuckets() {
        return numberOfHashValues;
    }

    public Histograms getBucketsDistribution() {
        Histograms ret = sample.getDistribution();
        ret.setDefaultValueForAbsentKey(numberOfHashValues);

        return ret;
    }

    public Map<Integer, Integer> getBucketToRouteMapping() {
        return this.hashValueToRoute;
    }

    public synchronized void setBucketToRouteMapping( Map<Integer, Integer> newMapping) {
        this.hashValueToRoute.clear();
        this.hashValueToRoute.putAll(newMapping);
        signature ++;
    }

    public synchronized void update(BalancedHashRouting newRoute) {
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
        routeDistributionSampler = new SlidingWindowRouteSampler(numberOfRoutes);
        routeDistributionSampler.enable();

//        if(super(PartialHashingRouting))

        return numberOfRoutes - 1;
    }

    @Override
    public synchronized void scalingIn() {
        signature ++;
        int largestSubtaskIndex = numberOfRoutes - 1;
        for(int shard: hashValueToRoute.keySet()) {
            if(hashValueToRoute.get(shard) == largestSubtaskIndex)
                throw new RuntimeException("There is at least one shard ("+ shard +") assigned to the Subtask with the largest index ("+ largestSubtaskIndex + "). Scaling in fails!");
        }
        numberOfRoutes--;
        routeDistributionSampler = new SlidingWindowRouteSampler(numberOfRoutes);
        routeDistributionSampler.enable();

    }
}
