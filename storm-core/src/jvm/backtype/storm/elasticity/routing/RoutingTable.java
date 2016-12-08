package backtype.storm.elasticity.routing;

import backtype.storm.elasticity.utils.Histograms;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Robert on 11/3/15.
 */
public interface RoutingTable extends Serializable, ScalableRouting {
    public static int origin = 0;
    public static int remote = -2;
    public int route(Object key);
    public int getNumberOfRoutes();
    public List<Integer> getRoutes();
    public Histograms getRoutingDistribution();
    public void enableRoutingDistributionSampling();
    public long getSigniture();
//    public int scalingOut();

    


}
