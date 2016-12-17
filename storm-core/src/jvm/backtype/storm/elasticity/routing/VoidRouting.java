package backtype.storm.elasticity.routing;

import backtype.storm.elasticity.utils.Histograms;

import java.util.List;

/**
 * Created by Robert on 11/4/15.
 */
public class VoidRouting implements RoutingTable {
    @Override
    public Route route(Object key) {
        return new Route(origin);
    }

    @Override
    public int getNumberOfRoutes() {
        return 0;
    }

    @Override
    public List<Integer> getRoutes() {
        return null;
    }

    @Override
    public Histograms getRoutingDistribution() {
        return null;
    }

    @Override
    public long getSigniture() {
        return 0;
    }

    @Override
    public int scalingOut() {
        return 0;
    }

    @Override
    public void scalingIn() {

    }
}
