package backtype.storm.elasticity.utils;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.utils.RateTracker;

import java.util.HashMap;

/**
 * Created by robert on 1/8/16.
 */
public class SlidingWindowRouteSampler {
    private int _nRoutes;

    public RateTracker[] routeCounts;

    private boolean enabled = false;

    static private int sampleLength = 5000;

    static private int numberOfSlides = 5;


    private GlobalHashFunction hashFunction = GlobalHashFunction.getInstance();

    public SlidingWindowRouteSampler(int nRoutes) {
        _nRoutes = nRoutes;
        routeCounts = new RateTracker[_nRoutes];
        for(int i=0; i < nRoutes; i++) {
            routeCounts[i] = new RateTracker(sampleLength, numberOfSlides);
        }
    }

    public synchronized void record(int route) {
        if(enabled) {
            routeCounts[route].notify(1);
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
        for(RateTracker i: routeCounts) {
            ret += i.reportRate() + "\n";
        }
        return ret;
    }

    public Double[] getFrequencies() {
        Double[] ret = new Double[_nRoutes];
        for(int i=0; i < _nRoutes; i++ ) {
            ret[i] = routeCounts[i].reportRate();
        }
        return ret;
    }

    public Histograms getDistribution() {
        HashMap<Integer, Long> distribution = new HashMap<>();
        for(Integer i=0; i < _nRoutes; i++ ) {
            distribution.put(i, (long)(routeCounts[i].reportRate()));
            Slave.getInstance().logOnMaster(String.format("Route %d: %f", i, routeCounts[i].reportRate()));
        }
        Slave.getInstance().logOnMaster(String.format("Histograms: %s", new Histograms(distribution).toString()));
        return new Histograms(distribution);
    }
}
