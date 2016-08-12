package backtype.storm.elasticity.metrics;

import backtype.storm.elasticity.actors.Slave;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by robert on 21/6/16.
 */
public class ThroughputForRoutes implements Serializable {
    private Map<Integer, Double> routeToThroughput = new ConcurrentHashMap<>();;
    private Map<Integer, Long> routeToUpdateTime = new HashMap<>();

    public void add(int routeId, double throughput) {
        routeToThroughput.put(routeId, throughput);
        routeToUpdateTime.put(routeId, System.currentTimeMillis());
    }

    public void merge(ThroughputForRoutes throughputForRoutes) {
        routeToThroughput.putAll(throughputForRoutes.routeToThroughput);
        for(Integer route: throughputForRoutes.routeToThroughput.keySet()) {
            routeToUpdateTime.put(route, System.currentTimeMillis());
        }
    }

    public double getThroughput() {
        double throughput = 0;
        for(int route: routeToThroughput.keySet()) {
            throughput += routeToThroughput.get(route);
            Slave.getInstance().sendMessageToMaster(String.format("[%d:] %f", route, routeToThroughput.get(route)));
        }
        return throughput;
    }

    public double getRecentThroughput(long threshold) {
        double throughput = 0;
        for(int route: routeToThroughput.keySet()) {
            if(routeToUpdateTime.get(route)!=null && System.currentTimeMillis() - routeToUpdateTime.get(route) <= threshold) {
                throughput += routeToThroughput.get(route);
            }
        }
        return throughput;
    }
}
