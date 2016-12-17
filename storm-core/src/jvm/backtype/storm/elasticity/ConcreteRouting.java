package backtype.storm.elasticity;

import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.utils.Histograms;

import java.util.*;

/**
 * Created by Robert on 11/3/15.
 */
public class ConcreteRouting implements RoutingTable {

    private HashMap<Object, Integer> routingTable;

    @Override
    public Route route(Object key) {
        if(routingTable.containsKey(key))
            return new Route(routingTable.get(key));
        return new Route(RoutingTable.origin);
    }

    @Override
    public int getNumberOfRoutes() {
        Set<Object> routes = new HashSet<Object>(routingTable.values());
        return routes.size();
//        for(int i: ) {
//
//        }
    }

    @Override
    public ArrayList<Integer> getRoutes() {
        return new ArrayList<Integer>(routingTable.values());
    }

    @Override
    public Histograms getRoutingDistribution() {
        return null;
    }

    @Override
    public long getSigniture() {
        return 0;
    }

    public void addRounting(Object key, Integer rount) {
        routingTable.put(key, rount);
    }

    public void removeRouting(Object key) {
        routingTable.remove(key);
    }

    @Override
    public int scalingOut() {
        return 0;
    }

    @Override
    public void scalingIn() {

    }
}
