package backtype.storm.elasticity;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.exceptions.InvalidRouteException;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.message.LabelingTuple;
import backtype.storm.elasticity.metrics.ExecutionLatencyForRoutes;
import backtype.storm.elasticity.message.taksmessage.ITaskMessage;
import backtype.storm.elasticity.message.taksmessage.RemoteTuple;
import backtype.storm.elasticity.metrics.ThroughputForRoutes;
import backtype.storm.elasticity.routing.*;
import backtype.storm.elasticity.utils.GlobalHashFunction;
import backtype.storm.elasticity.utils.MonitorUtils;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import backtype.storm.elasticity.state.*;

/**
 * An ElasticExecutor is a self-contained, light-weight, distributed subsystem that is able to run a dynamic number of
 * local tasks and remote tasks.
 */
public class ElasticExecutor implements Serializable {


    private BaseElasticBolt _bolt;

    private int _id; // The id of this ElasticExecutor.


    private transient HashMap<Integer, Thread> _taskIdToThread;

    private transient HashMap<Integer, QueryRunnable> _taskIdToQueryRunnable;

    private transient ElasticOutputCollector _elasticOutputCollector;

    // Pending queues for the local tasks.
    private transient HashMap<Integer, ArrayBlockingQueue<Tuple>> _localTaskIdToInputQueue;

    // A Shared pending queue for all remote tasks.
    private transient ArrayBlockingQueue<ITaskMessage> _reroutingTupleSendingQueue;

    private RoutingTable _routingTable; // a routing table that partitions the input stream into the tasks.

    private transient ElasticTaskHolder _taskHolder;

    private transient ProtocolAgent _protocolAgent;

    Dispatcher _dispatcher;

    private boolean remote = false;

    private Random _random;

    class Dispatcher implements Serializable{

        Map<Integer, Boolean> routeToWaitingFlag =  new ConcurrentHashMap<>();

        synchronized boolean dispatch(Tuple tuple, Object key) {
            return dispatch(tuple, key, null);
        }

        /**
         *
         * @param tuple tuple to be dispatched
         * @param key the key of the tuple
         * @param dispatchThreadDebugInfo debug info output
         * @return true, if the tuple is successfully dispatched; false, if the dispatching is aborted.
         */
        synchronized boolean dispatch(Tuple tuple, Object key,
                                      BaseElasticBoltExecutor.DispatchThreadDebugInfo dispatchThreadDebugInfo) {
            RoutingTable.Route route = _routingTable.route(key);

            if (routeToWaitingFlag.containsKey(route.originalRoute)) {
                    return false;
            }

            try {
                if (route.route != RoutingTable.REMOTE) {
                    if (!_localTaskIdToInputQueue.get(route.route).offer(tuple, 1, TimeUnit.MILLISECONDS)) {
                        return false;
                    }
                } else {
                    if (remote) {
                        String str = String.format("A tuple [key = %s]is routed to remote on a remote ElasticExecutor!\n", key);

                        str += "target route is " + route.originalRoute + "\n";
                        str += "target shard is " + GlobalHashFunction.getInstance().hash(key) % Config.NumberOfShard
                                + "\n";
                        str += _routingTable.toString();
                        Slave.getInstance().sendMessageToMaster(str);
                        return false;
                    }
                    RemoteTuple remoteTuple = new RemoteTuple(_id, route.originalRoute, tuple);
                    if (!_reroutingTupleSendingQueue.offer(remoteTuple, 1, TimeUnit.MILLISECONDS)) {
                        return false;
                    }
                }
                return true;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }

        synchronized void pauseSendingToARoute(int route) {
            routeToWaitingFlag.put(route, true);
        }

        synchronized void resumeSendingToARoute(int route) {
            routeToWaitingFlag.remove(route);
        }

        synchronized void dispatchTupleToLocalRoute(int route, Tuple tuple) throws InterruptedException {
            if (_routingTable.getRoutes().contains(route)) {
                _localTaskIdToInputQueue.get(route).put(tuple);
            }
        }

        synchronized void dispatchTupleToRemoteRoute(int route, ITaskMessage tuple) throws InterruptedException {
            if(_routingTable.getRoutes().contains(route)) {
                System.err.println("dispatch a remote tuple to a local route!");
                return;
            }
            _reroutingTupleSendingQueue.put(tuple);
        }

    }

    class ProtocolAgent {

        private transient Map<Integer, Semaphore> _routeIdToCleaningTupleSemaphore;

        private ProtocolAgent() {
            this._routeIdToCleaningTupleSemaphore = new ConcurrentHashMap<>();
        }

        void initializeCleaningTupleProtocol(int route) {
            final Semaphore semaphore = new Semaphore(0);
            _routeIdToCleaningTupleSemaphore.put(route, semaphore);
        }

        void markPendingTuplesCleaned(int route) {
            _routeIdToCleaningTupleSemaphore.get(route).release();
        }

        boolean waitUntilTuplesCleaned(int route, int waitTime, TimeUnit timeUnit) throws InterruptedException {
            boolean finished = _routeIdToCleaningTupleSemaphore.get(route).tryAcquire(waitTime, timeUnit);
            if(finished) {
                _routeIdToCleaningTupleSemaphore.remove(route);
            }
            return finished;
        }


    }

    public ElasticExecutor(BaseElasticBolt bolt, Integer taskID, RoutingTable routingTable) {
        _bolt = bolt;
        _id = taskID;
        _dispatcher = new Dispatcher();
        _routingTable = routingTable;
    }

    public static ElasticExecutor createHashRouting(int numberOfRoutes, BaseElasticBolt bolt, int taskID, ElasticOutputCollector collector) {
        RoutingTable routingTable = new TwoTireRouting(numberOfRoutes);
        ElasticExecutor ret = new ElasticExecutor(bolt, taskID, routingTable);
        ret.prepare(collector);
        ret.createAndLaunchLocalTasks();
        return ret;
    }

    public static ElasticExecutor createVoidRouting(BaseElasticBolt bolt, int taskID, ElasticOutputCollector collector) {
        ElasticExecutor ret = new ElasticExecutor(bolt, taskID, new VoidRouting());
        ret.prepare(collector);
        return ret;
    }

    public void setRemoteElasticTasks() {
        remote = true;
    }

    public void prepare(ElasticOutputCollector elasticOutputCollector) {
        _dispatcher = new Dispatcher();
        _localTaskIdToInputQueue = new HashMap<>();
        _taskIdToThread = new HashMap<>();
        _taskIdToQueryRunnable = new HashMap<>();
        _elasticOutputCollector = elasticOutputCollector;
        _taskHolder = ElasticTaskHolder.instance();
        _random = new Random();
        _protocolAgent = new ProtocolAgent();
    }

    public void prepare(ElasticOutputCollector elasticOutputCollector, KeyValueState state) {
        _bolt.setState(state);
        prepare(elasticOutputCollector);
    }

    public void set_reroutingTupleSendingQueue(ArrayBlockingQueue<ITaskMessage> reroutingTupleSendingQueue) {
        _reroutingTupleSendingQueue = reroutingTupleSendingQueue;
    }

    public RoutingTable get_routingTable() {
        return _routingTable;
    }

    public synchronized boolean dispatch(Tuple tuple, Object key) {
        return _dispatcher.dispatch(tuple, key);
    }

    public synchronized boolean dispatch(Tuple tuple, Object key, BaseElasticBoltExecutor.DispatchThreadDebugInfo dispatchThreadDebugInfo) {
        return _dispatcher.dispatch(tuple, key, dispatchThreadDebugInfo);
    }

    public void createAndLaunchLocalTasks() {
        for (int i : get_routingTable().getRoutes()) {
            createElasticTasksForGivenRoute(i);
            launchElasticTasksForGivenRoute(i);
        }
    }

    public void createElasticTasksForGivenRoute(int i) {
        if (!_routingTable.getRoutes().contains(i)) {
            System.out.println("Cannot create tasks for route " + i + ", because it is not valid!");
            return;
        }
        ArrayBlockingQueue<Tuple> inputQueue = new ArrayBlockingQueue<>(Config.SubtaskInputQueueCapacity);
        _localTaskIdToInputQueue.put(i, inputQueue);
    }

    public void launchElasticTasksForGivenRoute(int i) {
        ArrayBlockingQueue<Tuple> inputQueue = _localTaskIdToInputQueue.get(i);
        QueryRunnable query = new QueryRunnable(_bolt, inputQueue, _elasticOutputCollector, i, _protocolAgent);
        _taskIdToQueryRunnable.put(i, query);
        Thread newThread = new Thread(query);
        newThread.start();
        _taskIdToThread.put(i, newThread);
//        MonitorUtils.instance().registerThreadMonitor(newThread.getId(), "query route " + i, -1, 5);
//        System.out.println("created elastic worker threads for route "+i);
        System.out.println(String.format("Task %d created elastic worker thread (%xd) for route %d (%s)", _id,
                newThread.getId(), i, _elasticOutputCollector));
        ElasticTaskHolder.instance().sendMessageToMaster("created elastic worker threads for route " + i);
        ElasticTaskHolder.instance()._slaveActor.registerRoutesOnMaster(_id, i);
    }

    public void createAndLaunchElasticTasksForGivenRoute(int i) {
        createElasticTasksForGivenRoute(i);
        launchElasticTasksForGivenRoute(i);
    }

    /**
     * @param list list of exception routes
     * @return a PartialHashRouting that routes the excepted routes
     */
    private synchronized PartialHashingRouting addExceptionForHashRouting(ArrayList<Integer> list)
            throws InvalidRouteException, RoutingTypeNotSupportedException {
        if ((!(_routingTable instanceof HashingRouting)) && (!(_routingTable instanceof TwoTireRouting))
                && (!(_routingTable instanceof PartialHashingRouting))) {
            throw new RoutingTypeNotSupportedException("cannot set Exception for non-hash routing: "
                    + _routingTable.getClass().toString());
        }
        for (int i : list) {
            if (!_routingTable.getRoutes().contains(i)) {
                throw new InvalidRouteException("input route " + i + "is invalid");
            }
        }

        if (!(_routingTable instanceof PartialHashingRouting)) {
            _routingTable = new PartialHashingRouting(_routingTable);
        }

        ((PartialHashingRouting) _routingTable).addExceptionRoutes(list);
        for (int i : list) {
            terminateGivenQuery(i);
        }

        PartialHashingRouting ret = ((PartialHashingRouting) _routingTable).createComplementRouting();

//        ret.invalidAllRoutes();
//        ret.addValidRoutes(list);
        ret.setValidRoutes(list);
        return ret;
    }

    PartialHashingRouting addExceptionForHashRouting(int r)
            throws InvalidRouteException, RoutingTypeNotSupportedException {
        ArrayList<Integer> list = new ArrayList<>();
        list.add(r);
        return addExceptionForHashRouting(list);
    }

    /**
     * add a valid route to the routing table, but does not create the processing thread.
     * In fact, the processing thread can only be created when the remote state is merged with
     * local state, which should be handled by the callee.
     *
     * @param route route to be added
     */
    synchronized void addValidRoute(int route) throws RoutingTypeNotSupportedException {
        if (!(_routingTable instanceof PartialHashingRouting)) {
            System.err.println("can only add valid route for PartialHashRouting");
            throw new RoutingTypeNotSupportedException("can only add valid route for PartialHashRouting!");
        }

        PartialHashingRouting partialHashingRouting = (PartialHashingRouting) _routingTable;

        partialHashingRouting.addValidRoute(route);

        createElasticTasksForGivenRoute(route);

    }

    synchronized void setHashBalancedRouting(int numberOfRoutes, Map<Integer, Integer> hashValueToPartition) {
        if (numberOfRoutes < 0)
            throw new IllegalArgumentException("number of routes should be positive!");
        withdrawRoutes();
        _routingTable = new TwoTireRouting(hashValueToPartition, numberOfRoutes, true);

        createAndLaunchLocalTasks();
    }

    synchronized void setHashRouting(int numberOfRoutes) throws IllegalArgumentException {
        long start = System.nanoTime();
        if (numberOfRoutes < 0)
            throw new IllegalArgumentException("number of routes should be positive");
        withdrawRoutes();
        long withdrawTime = System.nanoTime() - start;

        _routingTable = new HashingRouting(numberOfRoutes);
        createAndLaunchLocalTasks();

        long totalTime = System.nanoTime() - start;

        Slave.getInstance().sendMessageToMaster("Terminate: " + withdrawTime / 1000 + " us\tLaunch: " +
                (totalTime - withdrawTime) / 1000 + "us\t total: " + totalTime / 1000);

    }

    public void setVoidRounting() {
        if (!(_routingTable instanceof VoidRouting)) {
            terminateQueries();
            _routingTable = new VoidRouting();
        }
    }

    public int get_id() {
        return _id;
    }

    public BaseElasticBolt get_bolt() {
        return _bolt;
    }


    public void terminateGivenQuery(int route) {
//        ElasticTaskHolder.instance().sendMessageToMaster("Terminating "+_taskID+"."+route +
// " (" + _queues.get(route).size() + " pending elements)"+" ...");
        _taskIdToQueryRunnable.get(route).terminate();
        try {
            _taskIdToThread.get(route).join();
            System.out.println("Query thread for " + _id + "." + route + " is terminated!");
//            ElasticTaskHolder.instance().sendMessageToMaster("Query thread for "+_taskID+"."+route + " is terminated!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        MonitorUtils.instance().unregister(_taskIdToThread.get(route).getId());

        _taskIdToQueryRunnable.remove(route);
        _taskIdToThread.remove(route);
        _localTaskIdToInputQueue.remove(route);
        ElasticTaskHolder.instance()._slaveActor.unregisterRoutesOnMaster(_id, route);

    }

    private void withdrawRoutes() {
        System.out.println("##########before termination!");

        // withdraw remote tasks.
        if (_routingTable instanceof PartialHashingRouting) {
            PartialHashingRouting partialHashingRouting = (PartialHashingRouting) _routingTable;
            for (int i : partialHashingRouting.getExceptionRoutes()) {
                try {
                    ElasticTaskHolder.instance().withdrawRemoteElasticTasks(get_id(), i);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // withdraw local tasks.
        terminateQueries();
        System.out.println("##########after termination!");
    }

    private void terminateQueries() {
        for (int i : _routingTable.getRoutes())
            terminateGivenQuery(i);
    }

    public ExecutionLatencyForRoutes getExecutionLatencyForRoutes() {
        ExecutionLatencyForRoutes latencyForRoutes = new ExecutionLatencyForRoutes();
        for (Integer routeId : _taskIdToQueryRunnable.keySet()) {
            Long averageExecutionLatency = _taskIdToQueryRunnable.get(routeId).getAverageExecutionLatency();
            if (averageExecutionLatency != null)
                latencyForRoutes.add(routeId, averageExecutionLatency);
        }
        return latencyForRoutes;
    }

    public ThroughputForRoutes getThroughputForRoutes() {
        ThroughputForRoutes throughputForRoutes = new ThroughputForRoutes();
        for (int route : _taskIdToQueryRunnable.keySet()) {
            double throughput = _taskIdToQueryRunnable.get(route).getThroughput();
            throughputForRoutes.add(route, throughput);
        }
        return throughputForRoutes;
    }

    public void makesSureNoPendingTuples(int routeId) {
        synchronized (_taskHolder._taskIdToRouteToSendingWaitingSemaphore.get(_id)) {
            _protocolAgent.initializeCleaningTupleProtocol(routeId);
            try {
//                _localTaskIdToInputQueue.get(routeId).put(new LabelingTuple());
                _dispatcher.dispatchTupleToLocalRoute(routeId, new LabelingTuple());
                while (!_protocolAgent.waitUntilTuplesCleaned(routeId, 1, TimeUnit.SECONDS)) {
                    Slave.getInstance().sendMessageToMaster(_localTaskIdToInputQueue.get(routeId).size() +
                            "  tuples remaining in " + this._id + "." + routeId);
                }
                System.out.println(String.format("Route %d is cleaned!", routeId));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


//        if (!_localTaskIdToInputQueue.containsKey(routeId)) {
//            System.err.println(String.format("RouteId %d cannot be found in makesSureNoPendingTuples!", routeId));
//            Slave.getInstance().logOnMaster(String.format("RouteId %d cannot be found in makesSureNoPendingTuples!",
//                    routeId));
//            return;
//        }
////        Slave.getInstance().sendMessageToMaster("Cleaning...." + this._taskID + "." + routeId);
//        System.out.print(String.format("Begin to clean local pending tuples for route %d", routeId));
//        synchronized (_taskHolder._taskIdToRouteToSendingWaitingSemaphore.get(_id)) {
//            Long startTime = null;
//            while (!_localTaskIdToInputQueue.get(routeId).isEmpty()) {
//                Utils.sleep(1);
//                if (startTime == null) {
//                    startTime = System.currentTimeMillis();
//                }
//
//                if (System.currentTimeMillis() - startTime > 1000) {
//                    Slave.getInstance().sendMessageToMaster(_localTaskIdToInputQueue.get(routeId).size() +
//                            "  tuples remaining in " + this._id + "." + routeId);
//                    startTime = System.currentTimeMillis();
//                }
//            }
//        }
//        System.out.print(String.format("Cleaned local pending tuples for route %d", routeId));
    }
}
