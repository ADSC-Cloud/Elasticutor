package backtype.storm.elasticity.routing;

import backtype.storm.elasticity.utils.GlobalHashFunction;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.elasticity.utils.SlidingWindowRouteSampler;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by Robert on 11/3/15.
 */
public class HashingRouting implements RoutingTable {

    private int numberOfRoutes;

    private long signature = 0;


    transient SlidingWindowRouteSampler sampler;

    GlobalHashFunction hashFunction;


    /**
     *
     * @param nRoutes is the number of routes processed by elastic tasks.
     */
    public HashingRouting(int nRoutes) {
        numberOfRoutes = nRoutes;
        hashFunction = new GlobalHashFunction();
    }

    public HashingRouting(HashingRouting hashingRouting) {
        numberOfRoutes = hashingRouting.numberOfRoutes;
        this.hashFunction = hashingRouting.hashFunction;
    }

    /**
     * TODO: the module hash funcion may result balls skewness. A better hash function is needed here.
     * @param key the key of the input tuple.
     * @return the number of route this key belongs to.
     */
    @Override
    public synchronized int route(Object key) {
//        if(key instanceof String) {
//            final int hashvalue = hashFunction.hashString(key.toString()).asInt();
//            return Math.abs(hashvalue%(numberOfRoutes + 1)) - 1;
//        } else {
//            final int hashValue =hashFunction.hash(key);

//            final int ret = Math.abs((hashValue*1171+5843))%9973%(numberOfRoutes);
        final int ret = hashFunction.hash(key) % numberOfRoutes;
        if(sampler!=null)
            sampler.record(ret);

            return ret;
//        }
    }

    @Override
    public int getNumberOfRoutes() {
        return numberOfRoutes;
    }

    @Override
    public List<Integer> getRoutes() {
        List<Integer> ret = new ArrayList<>();
        for(int i=0;i<numberOfRoutes;i++) {
            ret.add(i);
        }
        return ret;
    }

    @Override
    public Histograms getRoutingDistribution() {
        return sampler.getDistribution();
    }

    @Override
    public synchronized void enableRoutingDistributionSampling() {
        sampler = new SlidingWindowRouteSampler(numberOfRoutes);
        sampler.enable();
    }

    @Override
    public long getSigniture() {
        return signature;
    }

    @Override
    public int scalingOut() {
        signature ++;
        return numberOfRoutes++;
    }

    @Override
    public void scalingIn() {
        signature ++;
        numberOfRoutes--;
    }

    @Override
    public String toString() {
        String str ="";
        str += String.format("Hashing routing with %d routes", numberOfRoutes);
        return str;
    }
}
