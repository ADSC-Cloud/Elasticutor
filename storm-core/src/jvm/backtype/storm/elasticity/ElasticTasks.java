package backtype.storm.elasticity;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.exceptions.InvalidRouteException;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.metrics.ExecutionLatencyForRoutes;
import backtype.storm.elasticity.message.taksmessage.ITaskMessage;
import backtype.storm.elasticity.message.taksmessage.RemoteTuple;
import backtype.storm.elasticity.metrics.ThroughputForRoutes;
import backtype.storm.elasticity.routing.*;
import backtype.storm.elasticity.utils.GlobalHashFunction;
import backtype.storm.elasticity.utils.KeyFrequencySampler;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import backtype.storm.elasticity.state.*;
import backtype.storm.utils.Utils;

/**
 * Created by Robert on 11/3/15.
 */
public class ElasticTasks implements Serializable {

    private RoutingTable _routingTable;

    private BaseElasticBolt _bolt;

    private int _taskID;
    private transient HashMap<Integer, ArrayBlockingQueue<Tuple>> _queues;

    private transient HashMap<Integer, Thread> _queryThreads;

    private transient HashMap<Integer, QueryRunnable> _queryRunnables;

    private transient ElasticOutputCollector _elasticOutputCollector;

    private transient ArrayBlockingQueue<ITaskMessage> _remoteTupleQueue;

    private transient ElasticTaskHolder _taskHolder;

    public transient KeyFrequencySampler _sample;

    private boolean remote = false;

    private boolean reportDispatchUtilizationNotificatoin = false;
    long lastCpuTime = 0;


    public ElasticTasks(BaseElasticBolt bolt, Integer taskID) {
        _bolt = bolt;
        _taskID = taskID;
        _routingTable = new VoidRouting();
    }

    public void setRemoteElasticTasks() {
        remote = true;
    }

//    public ElasticTasks(ElasticTasks instance) {
//        _bolt = instance._bolt;
//        _taskID = instance._taskID;
//        _routingTable = new VoidRouting();
//    }

//    private ElasticTasks(BaseElasticBolt bolt, RoutingTable routing, ElasticOutputCollector elasticOutputCollector) {
//        _bolt = bolt;
//        _routingTable = routing;
//        _elasticOutputCollector = elasticOutputCollector;
//        ArrayList<Integer> routes = _routingTable.getRoutes();
//        for(Integer i: routes) {
//            _queues.put(i, new LinkedBlockingQueue<Tuple>());
//        }
//    }

    public void prepare(ElasticOutputCollector elasticOutputCollector) {
        _queues = new HashMap<>();
        _queryThreads = new HashMap<>();
        _queryRunnables = new HashMap<>();
        _elasticOutputCollector = elasticOutputCollector;
        _sample = new KeyFrequencySampler(Config.RoutingSamplingRate);
        _taskHolder=ElasticTaskHolder.instance();
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                while(true) {
//                    Utils.sleep(5000);
//                    reportDispatchUtilizationNotificatoin = true;
//                }
//            }
//        }).start();
    }

    public void prepare(ElasticOutputCollector elasticOutputCollector, KeyValueState state) {
        _bolt.setState(state);
//        for(Object key: state.getState().keySet()) {
////            System.out.println("State <"+key+", "+state.getValueByKey(key)+"> has been restored!");
//        }
        System.out.println(state.getState().size() + " states have been migrated!");
        _queues = new HashMap<>();
        _queryThreads = new HashMap<>();
        _queryRunnables = new HashMap<>();
        _elasticOutputCollector = elasticOutputCollector;
        _sample = new KeyFrequencySampler(Config.RoutingSamplingRate);
        _taskHolder=ElasticTaskHolder.instance();
    }

    public RoutingTable get_routingTable() {
        return _routingTable;
    }

    public void set_routingTable(RoutingTable routingTable) {
        _routingTable = routingTable;
    }

    public synchronized boolean tryHandleTuple(Tuple tuple, Object key) {

        if(reportDispatchUtilizationNotificatoin) {
            reportDispatchUtilizationNotificatoin = false;
            ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();

            long cpuTime = tmxb.getThreadUserTime(Thread.currentThread().getId());
            double utilization = (cpuTime - lastCpuTime) / 5E9;
            lastCpuTime = cpuTime;
            Slave.getInstance().sendMessageToMaster(String.format("Dispatch: %f", utilization));
        }

        final long signature = _routingTable.getSigniture();
        int route = _routingTable.route(key);
        int originalRoute = route;
        if(signature != _routingTable.getSigniture() && route == RoutingTable.remote)
            originalRoute = ((PartialHashingRouting)_routingTable).getOrignalRoute(key);

        final boolean paused = _taskHolder.waitIfStreamToTargetSubtaskIsPaused(_taskID, route);
        synchronized (_taskHolder._taskIdToRouteToSendingWaitingSemaphore.get(_taskID)) {

            // The routing table may be updated during the pausing phase, so we should recompute the route.
            if (paused && signature != _routingTable.getSigniture()) {
                route = _routingTable.route(key);
                if (route == RoutingTable.remote)
                    originalRoute = ((PartialHashingRouting) _routingTable).getOrignalRoute(key);
            }

            if (route == RoutingTable.remote) {
                if (remote) {
                    String str = String.format("A tuple [key = %s]is routed to remote on a remote ElasticTasks!\n", key);
                    str += "target route is " + originalRoute + "\n";
                    str += "target shard is " + GlobalHashFunction.getInstance().hash(key) % Config.NumberOfShard + "\n";
                    str += _routingTable.toString();
                    Slave.getInstance().sendMessageToMaster(str);
                    return false;
                }

                if(new Random().nextInt(5000)==0)
                    System.out.println(String.format("%s(shard = %d) is routed to %d [remote]!", key.toString(), GlobalHashFunction.getInstance().hash(key) % Config.NumberOfShard, originalRoute));

                RemoteTuple remoteTuple = new RemoteTuple(_taskID, originalRoute, tuple);

                try {
                    _remoteTupleQueue.put(remoteTuple);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return true;
            } else {
                try {
                    if(new Random().nextInt(5000)==0)
                        System.out.println("A tuple is route to "+route+ " by the routing table!");
                    _queues.get(route).put(tuple);
//                System.out.println("A tuple is inserted into the processing queue!");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return true;
            }
        }
    }


//    public boolean handleTuple(Tuple tuple, Object key){
//        int route = _routingTable.route(key);
//        if(route < 0)
//            return false;
//        try {
//            _queues.get(route).put(tuple);
//            return true;
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            return true;
//        }
//
//    }


    public static ElasticTasks createHashRouting(int numberOfRoutes, BaseElasticBolt bolt, int taskID, ElasticOutputCollector collector) {
     //   RoutingTable routingTable = new HashingRouting(numberOfRoutes);
     //========>below are my version
        RoutingTable routingTable = new BalancedHashRouting(numberOfRoutes);
        ElasticTasks ret = new ElasticTasks(bolt, taskID);
        ret._routingTable = routingTable;
        ret.prepare(collector);
        ret.createAndLaunchElasticTasks();
////        ElasticTasks ret = new ElasticTasks(bolt, routingTable, collector);
//        for(int i : routingTable.getRoutes())
////        for(int i = 0; i < numberOfRoutes; i++)
//        {
//            LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<>();
//            ret._queues.put(i, inputQueue);
//
//            QueryRunnable query = new QueryRunnable(bolt, inputQueue, collector);
//            ret._queryRunnables.add(query);
//            Thread newThread = new Thread(query);
//            newThread.start();
//            ret._queryThreads.add(newThread);
//        }
        return ret;
    }

    public static ElasticTasks createVoidRouting(BaseElasticBolt bolt, int taskID,ElasticOutputCollector collector ) {
        ElasticTasks ret = new ElasticTasks(bolt, taskID);
        ret.prepare(collector);
        return ret;
    }

    public void createAndLaunchElasticTasks() {
        for(int i : get_routingTable().getRoutes())
        {
//            LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<>();
//            _queues.put(i, inputQueue);
//
//            QueryRunnable query = new QueryRunnable(_bolt, inputQueue, _elasticOutputCollector);
//            _queryRunnables.put(i, query);
//            Thread newThread = new Thread(query);
//            newThread.start();
//            _queryThreads.put(i, newThread);
//            System.out.println("created elastic worker threads for route "+i);
//            createAndLaunchElasticTasksForGivenRoute(i);
            createElasticTasksForGivenRoute(i);
            launchElasticTasksForGivenRoute(i);
        }
        System.out.println("##########after launch tasks!");

    }

    public void createElasticTasksForGivenRoute(int i) {
        if(!_routingTable.getRoutes().contains(i)) {
            System.out.println("Cannot create tasks for route "+i+", because it is not valid!");
            return;
        }
        ArrayBlockingQueue<Tuple> inputQueue = new ArrayBlockingQueue<>(Config.SubtaskInputQueueCapacity);
        _queues.put(i, inputQueue);
    }

    public void launchElasticTasksForGivenRoute(int i) {
        ArrayBlockingQueue<Tuple> inputQueue = _queues.get(i);
        QueryRunnable query = new QueryRunnable(_bolt, inputQueue, _elasticOutputCollector, i);
        _queryRunnables.put(i, query);
        Thread newThread = new Thread(query);
        newThread.start();
        _queryThreads.put(i, newThread);
//        System.out.println("created elastic worker threads for route "+i);
        System.out.println(String.format("Task %d created elastic worker thread (%xd) for route %d (%s)", _taskID, newThread.getId(), i, _elasticOutputCollector));
//        ElasticTaskHolder.instance().sendMessageToMaster("created elastic worker threads for route "+i);
        ElasticTaskHolder.instance()._slaveActor.registerRoutesOnMaster(_taskID, i);
    }

    public void createAndLaunchElasticTasksForGivenRoute(int i) {
        if(!_routingTable.getRoutes().contains(i)) {
            System.out.println("Cannot create tasks for route "+i+", because it is not valid!");
            return;
        }
        ArrayBlockingQueue<Tuple> inputQueue = new ArrayBlockingQueue<>(Config.SubtaskInputQueueCapacity);
        _queues.put(i, inputQueue);

        QueryRunnable query = new QueryRunnable(_bolt, inputQueue, _elasticOutputCollector, i);
        _queryRunnables.put(i, query);
        Thread newThread = new Thread(query);
        newThread.start();
        _queryThreads.put(i, newThread);
        System.out.println(String.format("Task %d created elastic worker thread (%xd) for route %d (%s)", _taskID, newThread.getId(), i, _elasticOutputCollector));
//        System.out.println("created elastic worker threads for route "+i);
        ElasticTaskHolder.instance()._slaveActor.registerRoutesOnMaster(_taskID, i);
    }

    /**
     *
     * @param list list of exception routes
     * @return a PartialHashRouting that routes the excepted routes
     */
    public synchronized PartialHashingRouting addExceptionForHashRouting(ArrayList<Integer> list, ArrayBlockingQueue<ITaskMessage> exceptedRoutingQueue) throws InvalidRouteException, RoutingTypeNotSupportedException {
        if((!(_routingTable instanceof HashingRouting))&&(!(_routingTable instanceof BalancedHashRouting))&&(!(_routingTable instanceof PartialHashingRouting))) {
            throw new RoutingTypeNotSupportedException("cannot set Exception for non-hash routing: " + _routingTable.getClass().toString());
//            System.err.println("cannot set Exception for non-hash routing");
//            return null;
        }
        for(int i: list) {
            if(!_routingTable.getRoutes().contains(i)) {
                throw new InvalidRouteException("input route " + i + "is invalid");
//                System.err.println("input route " + i + "is invalid");
//                return null;
            }
        }
        _remoteTupleQueue = exceptedRoutingQueue;

//        HashingRouting routing = (HashingRouting)_routingTable;
        if(!(_routingTable instanceof PartialHashingRouting)) {
            _routingTable = new PartialHashingRouting(_routingTable);
        }
//        System.out.println("Original Routing (Before adding exception): getRoutes:" +get_routingTable().getRoutes());
        ((PartialHashingRouting)_routingTable).addExceptionRoutes(list);
        for(int i: list) {
//            System.out.println("Terminating the thread for route " + i);
            terminateGivenQuery(i);
        }
//        System.out.println("Original Routing (After adding exception): getRoutes:" +get_routingTable().getRoutes());
        PartialHashingRouting ret = ((PartialHashingRouting)_routingTable).createComplementRouting();
//        System.out.println("Complement Routing: getRoutes:" + ret.getRoutes());
        ret.invalidAllRoutes();
        ret.addValidRoutes(list);
//        System.out.println("Complement Routing: getRoutes:" +ret.getRoutes());

//        System.out.println("routing table: " + _routingTable.getClass() + " " + _routingTable + " valid routing: " + _routingTable.getRoutes());
        return ret;
    }

    public PartialHashingRouting addExceptionForHashRouting(int r, ArrayBlockingQueue<ITaskMessage> exceptedRoutingQueue) throws InvalidRouteException, RoutingTypeNotSupportedException {
        ArrayList<Integer> list = new ArrayList<>();
        list.add(r);
        return addExceptionForHashRouting(list, exceptedRoutingQueue);
    }

    /**
     * add a valid route to the routing table, but does not create the processing thread.
     * In fact, the processing thread can only be created when the remote state is merged with
     * local state, which should be handled by the callee.
     * @param route route to be added
     */
    public synchronized void addValidRoute(int route) throws RoutingTypeNotSupportedException {
        if(!(_routingTable instanceof PartialHashingRouting)) {
            System.err.println("can only add valid route for PartialHashRouting");
            throw new RoutingTypeNotSupportedException("can only add valid route for PartialHashRouting!");
        }

        PartialHashingRouting partialHashingRouting = (PartialHashingRouting) _routingTable;

        partialHashingRouting.addValidRoute(route);

        createElasticTasksForGivenRoute(route);

    }

    public synchronized void setHashBalancedRouting(int numberOfRoutes, Map<Integer, Integer> hashValueToPartition) {
        if (numberOfRoutes < 0)
            throw new IllegalArgumentException("number of routes should be positive!");
        withDrawCurrentRouting();
        _routingTable = new BalancedHashRouting(hashValueToPartition, numberOfRoutes, true);


        createAndLaunchElasticTasks();


    }

    public synchronized void setHashRouting(int numberOfRoutes) throws IllegalArgumentException {
        long start = System.nanoTime();
        if(numberOfRoutes < 0)
            throw new IllegalArgumentException("number of routes should be positive");

//        System.out.println("##########before termination!");
//        if((_routingTable instanceof HashingRouting)) {
//
//            terminateQueries();
//            if(_routingTable instanceof PartialHashingRouting) {
//                PartialHashingRouting partialHashingRouting = (PartialHashingRouting) _routingTable;
//                for(int i: partialHashingRouting.getExceptionRoutes()) {
//                    try {
//                        ElasticTaskHolder.instance().withdrawRemoteElasticTasks(get_taskID(), i);
//                    } catch(Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }
//        System.out.println("##########after termination!");
//        clearDistributionSample();

        withDrawCurrentRouting();
        long withdrawTime = System.nanoTime() - start;



        _routingTable = new HashingRouting(numberOfRoutes);
        createAndLaunchElasticTasks();

        long totalTime = System.nanoTime() - start;

        Slave.getInstance().sendMessageToMaster("Terminate: " + withdrawTime / 1000 + " us\tLaunch: " + (totalTime - withdrawTime) / 1000 + "us\t total: " + totalTime / 1000);

//        for(int i: _routingTable.getRoutes())
////        for(int i = 0; i < numberOfRoutes; i++)
//        {
//            LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<>();
//            _queues.put(i, inputQueue);
//
//            QueryRunnable query = new QueryRunnable(_bolt, inputQueue, _elasticOutputCollector);
//            _queryRunnables.add(query);
//            Thread newThread = new Thread(query);
//            newThread.start();
//            _queryThreads.add(newThread);
//        }
    }

    public void setVoidRounting() {
        if(!(_routingTable instanceof VoidRouting)) {
            terminateQueries();
            _routingTable = new VoidRouting();
        }
    }

    public int get_taskID() {
        return _taskID;
    }

    public BaseElasticBolt get_bolt() {
        return _bolt;
    }


    public void terminateGivenQuery(int route) {
//        ElasticTaskHolder.instance().sendMessageToMaster("Terminating "+_taskID+"."+route + " (" + _queues.get(route).size() + " pending elements)"+" ...");
        _queryRunnables.get(route).terminate();
        try {
            _queryThreads.get(route).join();
            System.out.println("Query thread for "+_taskID+"."+route + " is terminated!");
//            ElasticTaskHolder.instance().sendMessageToMaster("Query thread for "+_taskID+"."+route + " is terminated!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        _queryRunnables.remove(route);
        _queryThreads.remove(route);
        _queues.remove(route);
        ElasticTaskHolder.instance()._slaveActor.unregisterRoutesOnMaster(_taskID, route);

    }

    private void withDrawCurrentRouting() {
        System.out.println("##########before termination!");
//        if((_routingTable instanceof RoutingTable)) {

            if(_routingTable instanceof PartialHashingRouting) {
                PartialHashingRouting partialHashingRouting = (PartialHashingRouting) _routingTable;
                for(int i: partialHashingRouting.getExceptionRoutes()) {
                    try {
                        ElasticTaskHolder.instance().withdrawRemoteElasticTasks(get_taskID(), i);
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            terminateQueries();
//        }
        System.out.println("##########after termination!");
        clearDistributionSample();
    }

    private void terminateQueries() {

        for(int i: _routingTable.getRoutes())
            terminateGivenQuery(i);
//
//        for(QueryRunnable query: _queryRunnables.values()) {
//            query.terminate();
//        }
//        _queryRunnables.clear();
//        for(Thread thread: _queryThreads.values()) {
//            try {
//                thread.join();
//            } catch (InterruptedException e) {
//
//            }
//        }
//        _queryThreads.clear();
//        _queues.clear();
    }
//
//    public void terminateAGivenQuery(int route) throws InvalidRouteException {
//        if(!_queryThreads.containsKey(route)&&!_queryRunnables.containsKey(route)||!_queues.containsKey(route)) {
//            throw new InvalidRouteException("ElasticTasks does not have route " + route + ".");
//        }
//        _queryRunnables.get(route).terminate();
//        try {
//            _queryThreads.get(route).join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        _queryRunnables.remove(route);
//        _queryThreads.remove(route);
//        _queues.remove(route);
//    }

    private void clearDistributionSample() {
        if(_sample!=null) {
            _sample.clear();
        }
    }

    public ExecutionLatencyForRoutes getExecutionLatencyForRoutes() {
        ExecutionLatencyForRoutes latencyForRoutes = new ExecutionLatencyForRoutes();
        for(Integer routeId: _queryRunnables.keySet()) {
//            if(_queryRunnables.containsKey(routeId)) {
                Long averageExecutionLatency = _queryRunnables.get(routeId).getAverageExecutionLatency();
                if(averageExecutionLatency != null)
                    latencyForRoutes.add(routeId, averageExecutionLatency);
//            }
        }
        return latencyForRoutes;
    }

    public ThroughputForRoutes getThroughputForRoutes() {
        ThroughputForRoutes throughputForRoutes = new ThroughputForRoutes();
        for(int route: _queryRunnables.keySet()) {
            double throughput = _queryRunnables.get(route).getThroughput();
            throughputForRoutes.add(route, throughput);
        }
        return throughputForRoutes;
    }

    public void makesSureNoPendingTuples(int routeId) {
        if(!_queues.containsKey(routeId)) {
            System.err.println(String.format("RouteId %d cannot be found in makesSureNoPendingTuples!", routeId));
            Slave.getInstance().logOnMaster(String.format("RouteId %d cannot be found in makesSureNoPendingTuples!", routeId));
            return;
        }
//        Slave.getInstance().sendMessageToMaster("Cleaning...." + this._taskID + "." + routeId);
        Long startTime = null;
        while(!_queues.get(routeId).isEmpty()) {
            Utils.sleep(1);
            if(startTime == null) {
                startTime = System.currentTimeMillis();
            }

            if(System.currentTimeMillis() - startTime > 1000) {
                Slave.getInstance().sendMessageToMaster(_queues.get(routeId).size() + "  tuples remaining in " + this._taskID + "." + routeId);
                startTime = System.currentTimeMillis();
            }
        }

    }

}
