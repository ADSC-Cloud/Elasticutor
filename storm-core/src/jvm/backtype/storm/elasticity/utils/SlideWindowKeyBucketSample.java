package backtype.storm.elasticity.utils;

import backtype.storm.utils.RateTracker;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by robert on 12/18/15.
 */
public class SlideWindowKeyBucketSample implements Serializable {

    private int _nBuckets;

    public RateTracker[] buckets;

    private boolean enabled = false;

    static private int sampleLength = 1000;

    static private int numberOfSlides = 5;


    private GlobalHashFunction hashFunction = GlobalHashFunction.getInstance();

    public SlideWindowKeyBucketSample(int nbucks) {
        _nBuckets = nbucks;
        buckets = new RateTracker[_nBuckets];
        for(int i=0; i < nbucks; i++) {
            buckets[i] = new RateTracker(sampleLength, numberOfSlides);
        }
    }

    public synchronized void record(Object key) {
        if(enabled) {
            final int bucket = hashFunction.hash(key)%_nBuckets;
            buckets[bucket].notify(1);
        }
    }

    public synchronized void enable() {
        enabled = true;
    }

    public synchronized void disable() {
        enabled = false;
    }

    public String toString() {
        String ret = "";
        for(RateTracker i: buckets) {
            ret += i.reportRate() + "\n";
        }
        return ret;
    }

    public Double[] getFrequencies() {
        Double[] ret = new Double[_nBuckets];
        for(int i=0; i < _nBuckets; i++ ) {
            ret[i] = buckets[i].reportRate();
        }
        return ret;
    }

    public Histograms getDistribution() {
        ConcurrentHashMap<Integer, Long> distribution = new ConcurrentHashMap<>();
        for(Integer i=0; i < _nBuckets; i++ ) {
            //RateTracker.reportRate() return tuples per second rather than millisecond
            distribution.put(i, (long)(buckets[i].reportRate() * sampleLength/1000));
        }
//        for(int i = 0; i < _nBuckets; ++i){
//            System.out.print(i);
//            System.out.print(" ");
//            System.out.print(distribution.get(i));
//            System.out.println();
//        }
        return new Histograms(distribution);
    }
}
