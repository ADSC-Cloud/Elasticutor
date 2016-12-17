package backtype.storm.elasticity.routing;

import java.util.List;

/**
 * Created by robert on 12/18/15.
 */
public class RoutingTableUtils {

    public static TwoTireRouting getBalancecHashRouting(RoutingTable routingTable) {
        if(routingTable instanceof TwoTireRouting) {
            return (TwoTireRouting)routingTable;
        } else if ((routingTable instanceof PartialHashingRouting) && (((PartialHashingRouting) routingTable).getOriginalRoutingTable() instanceof TwoTireRouting)) {
            return (TwoTireRouting)((PartialHashingRouting) routingTable).getOriginalRoutingTable();
        } else
            return null;
    }

    public static List<Integer> getOriginalRoutes(RoutingTable routingTable) {
        if(routingTable instanceof PartialHashingRouting) {
            return ((PartialHashingRouting) routingTable).getOriginalRoutes();
        } else
            return routingTable.getRoutes();
    }
}
