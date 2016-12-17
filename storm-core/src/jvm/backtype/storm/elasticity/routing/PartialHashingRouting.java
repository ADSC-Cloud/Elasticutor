package backtype.storm.elasticity.routing;

import backtype.storm.elasticity.utils.Histograms;

import java.util.*;

/**
 * Created by Robert on 11/12/15.
 */
public class PartialHashingRouting implements RoutingTable {


    /* the set of valid routes balls this routing table */
    Set<Integer> _validRoutes = new HashSet<>();

    RoutingTable _routingTable;

    long signature = 0;

//    /**
//     * @param nRoutes is the number of routes processed by elastic tasks.
//     */
//    public PartialHashingRouting(int nRoutes) {
//        super(nRoutes);
//        for(int i = 0; i < nRoutes; i++) {
//            _validRoutes.add(i);
//        }
//    }

    public PartialHashingRouting(RoutingTable hashingRouting) {
        _routingTable = hashingRouting;
//        super(hashingRouting);
        _validRoutes.addAll(_routingTable.getRoutes());
    }

    public PartialHashingRouting setExceptionRoutes(ArrayList<Integer> exceptionRoutes) {
        _validRoutes.addAll(getRoutes());
        _validRoutes.removeAll(exceptionRoutes);
        signature ++;
        return this;
    }

    public PartialHashingRouting addExceptionRoutes(ArrayList<Integer> exceptionRoutes) {
        _validRoutes.removeAll(exceptionRoutes);
        signature ++;
        return this;
    }

    public PartialHashingRouting addExceptionRoute(Integer exception) {
        _validRoutes.remove(exception);
        signature ++;
        return this;
    }

    @Override
    public int getNumberOfRoutes() {
        return _validRoutes.size();
    }


    @Override
    public ArrayList<Integer> getRoutes() {
        return new ArrayList<>(_validRoutes);
    }

    @Override
    public Histograms getRoutingDistribution() {
        return _routingTable.getRoutingDistribution();
    }

    @Override
    public long getSigniture() {
        return signature + _routingTable.getSigniture();
    }

    @Override
    public synchronized int scalingOut() {
        int newRouteIndex = -1;
//        if(_routingTable instanceof TwoTireRouting) {
            newRouteIndex = _routingTable.scalingOut();
//        }
        _validRoutes.add(newRouteIndex);
        return newRouteIndex;
    }

    @Override
    public synchronized void scalingIn() {
        _routingTable.scalingIn();
        _validRoutes.remove(_routingTable.getNumberOfRoutes());
    }

    @Override
    public synchronized Route route(Object key) {
        Route route = _routingTable.route(key);
        if (_validRoutes.contains(route.route))
            return route;
        else
            return new Route(RoutingTable.remote, route.route);
    }

    public List<Integer> getOriginalRoutes() {
        return _routingTable.getRoutes();
    }

    public List<Integer> getExceptionRoutes() {
        List<Integer> ret = getOriginalRoutes();
        ret.removeAll(getRoutes());
        return ret;
    }

    public int getOrignalRoute(Object key) {
        final int ret = _routingTable.route(key).originalRoute;
        if(ret < 0) throw new RuntimeException(String.format("key: %s RoutingTable: %s", key.toString(), this.toString()));
        return ret;
    }

    public PartialHashingRouting createComplementRouting() {
        PartialHashingRouting ret = new PartialHashingRouting(_routingTable);
        ret._validRoutes.removeAll(this._validRoutes);
//        signature ++;
        return ret;
    }

    public void invalidAllRoutes() {
        _validRoutes.clear();
    }

    public void addValidRoute(int route) {
        ArrayList<Integer> list = new ArrayList<>();
        list.add(route);
        addValidRoutes(list);
    }

    public synchronized void addValidRoutes(List<Integer> routes) {
        signature ++;
        for(int i: routes) {
            if(_routingTable.getRoutes().contains(i)) {
                _validRoutes.add(i);
            } else {
                System.out.println("Cannot added routes "+i+", because it is not a valid route");
            }
        }
    }

    public synchronized void setValidRoutes(List<Integer> routes) {
        signature ++;
        _validRoutes.clear();
        addValidRoutes(routes);
    }

    public String toString() {
        String ret = "PartialHashRouting: \n";
        ret += "number of original routes: " + getOriginalRoutes() + "\n";
        ret += "number of valid routes: " + getNumberOfRoutes() + "\n";
        ret += "valid routes: " + _validRoutes + "\n";
        ret += this._routingTable.toString();
        return ret;
    }

    public RoutingTable getOriginalRoutingTable() {
        return _routingTable;
    }

    public static void main(String[] args) {

        HashingRouting routing = new HashingRouting(3);

        PartialHashingRouting partialHashingRouting = new PartialHashingRouting(routing);

        PartialHashingRouting complement = partialHashingRouting.addExceptionRoute(2).createComplementRouting();

        System.out.println("Complement:"+complement.getRoutes());


        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        map.put(0,0);
        map.put(1,0);
        map.put(2,1);
        map.put(3,1);
        TwoTireRouting twoTireRouting = new TwoTireRouting(map,2);

        PartialHashingRouting partialHashingRouting2 = new PartialHashingRouting(twoTireRouting);

        System.out.println("2-->" + partialHashingRouting2.route(1).route);

        PartialHashingRouting complement2 = partialHashingRouting2.addExceptionRoute(1);

        System.out.println("2-->" + partialHashingRouting2.route(1).route);

        System.out.println("Complement:"+complement2.getRoutes());





    }


}
