package backtype.storm.elasticity.routing;

import junit.framework.TestSuite;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by Robert on 12/17/16.
 */
public class ElasticityTwoTireRoutingTest extends TestSuite {
    @Test
    public void testRoute() {
        RoutingTable routingTable = new TwoTireRouting(5);
        for (int i = 0; i < 1000; i ++) {
            assertTrue(routingTable.route(i).route <=5 && routingTable.route(i).route >=0);
        }
    }

    @Test
    public void testSignatureUpdate() {
        TwoTireRouting routingTable = new TwoTireRouting(10);
        long signature = routingTable.getSigniture();
        routingTable.scalingIn();
        assertTrue(signature != routingTable.getSigniture());
        signature = routingTable.getSigniture();

        routingTable.scalingOut();
        assertTrue(signature != routingTable.getSigniture());
        signature = routingTable.getSigniture();

        routingTable.reassignBucketToRoute(0, 4);
        routingTable.reassignBucketToRoute(0, 6);
        assertTrue(signature != routingTable.getSigniture());

    }
}
