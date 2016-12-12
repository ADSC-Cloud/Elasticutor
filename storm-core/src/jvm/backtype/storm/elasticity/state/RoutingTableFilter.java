package backtype.storm.elasticity.state;

import backtype.storm.elasticity.routing.RoutingTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

/**
 * Created by robert on 12/16/15.
 */
public class RoutingTableFilter implements StateFilter{

    RoutingTable _routingTable;

    HashSet<Integer> _validRoutes = new HashSet<>();

    public RoutingTableFilter(RoutingTable routingTable, Integer validRoute) {
        _routingTable = routingTable;
        _validRoutes.add(validRoute);
    }

    public RoutingTableFilter(RoutingTable routingTable, Collection<Integer> validRoutes) {
        _routingTable = routingTable;
        _validRoutes.addAll(validRoutes);

    }

    @Override
    public Boolean isValid(Object key) {
        return _validRoutes.contains(_routingTable.route(key).originalRoute);
    }
}
