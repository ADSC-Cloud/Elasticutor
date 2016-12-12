package backtype.storm.elasticity.routing;

import backtype.storm.elasticity.utils.Histograms;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Robert on 11/3/15.
 */
public interface RoutingTable extends Serializable, ScalableRouting {
    static public class Route {
        public int route;
        public int originalRoute;
        public Route(int route, int originalRoute) {
            this.route = route;
            this.originalRoute = originalRoute;
        }

        public Route(int route) {
            this(route, route);
        }
    }
    public static int origin = 0;
    public static int remote = -2;
    public Route route(Object key);
    public int getNumberOfRoutes();
    public List<Integer> getRoutes();
    public Histograms getRoutingDistribution();
    public void enableRoutingDistributionSampling();
    public long getSigniture();
//    public int scalingOut();

    


}
