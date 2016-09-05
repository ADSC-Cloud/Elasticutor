package backtype.storm.elasticity;

import backtype.storm.elasticity.common.RouteId;
import backtype.storm.elasticity.common.ShardWorkload;
import backtype.storm.elasticity.common.SubTaskWorkload;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.exceptions.BucketNotExistingException;
import backtype.storm.elasticity.message.actormessage.*;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.exceptions.InvalidRouteException;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.exceptions.TaskNotExistingException;
import backtype.storm.elasticity.message.taksmessage.*;
import backtype.storm.elasticity.metrics.ExecutionLatencyForRoutes;
import backtype.storm.elasticity.metrics.ThroughputForRoutes;
import backtype.storm.elasticity.resource.ResourceMonitor;
import backtype.storm.elasticity.routing.BalancedHashRouting;
import backtype.storm.elasticity.routing.PartialHashingRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.routing.RoutingTableUtils;
import backtype.storm.elasticity.scheduler.ShardReassignment;
import backtype.storm.elasticity.scheduler.ShardReassignmentPlan;
import backtype.storm.elasticity.state.*;
import backtype.storm.elasticity.utils.FirstFitDoubleDecreasing;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.elasticity.utils.serialize.RemoteTupleExecuteResultDeserializer;
import backtype.storm.elasticity.utils.serialize.RemoteTupleExecuteResultSerializer;
import backtype.storm.elasticity.utils.timer.SmartTimer;
import backtype.storm.elasticity.utils.timer.SubtaskWithdrawTimer;
import backtype.storm.generated.HostNotExistException;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.messaging.netty.Client;
import backtype.storm.messaging.netty.Context;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.Utils;
import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Created by Robert on 11/3/15.
 */
public class ElasticTaskHolder {

    public static Logger LOG = LoggerFactory.getLogger(ElasticTaskHolder.class);

    private static ElasticTaskHolder _instance;

    private IContext _context;

    private IConnection _inputConnection;

    private IConnection _priorityInputConnection;

    private IConnection _remoteExecutionResultInputConnection;

    private String _workerId;

    private int _port;

    public Slave _slaveActor;

    private WorkerTopologyContext _workerTopologyContext;

    private Map stormConf;

    private KryoTupleDeserializer tupleDeserializer;

    private KryoTupleSerializer tupleSerializer;

    private RemoteTupleExecuteResultSerializer remoteTupleExecuteResultSerializer;

    private RemoteTupleExecuteResultDeserializer remoteTupleExecuteResultDeserializer;


    Map<Integer, BaseElasticBoltExecutor> _bolts = new HashMap<>();

    Map<Integer, ElasticRemoteTaskExecutor> _originalTaskIdToRemoteTaskExecutor = new HashMap<>();

    Map<Integer, IConnection> _originalTaskIdToConnection = new ConcurrentHashMap<>();

    Map<Integer, IConnection> _originalTaskIdToExecutorResultConnection = new ConcurrentHashMap<>();

    Map<Integer, IConnection> _originalTaskIdToPriorityConnection = new ConcurrentHashMap<>();

    LinkedBlockingQueue<ITaskMessage> _sendingQueue = new LinkedBlockingQueue<>(Config.ElasticTaskHolderOutputQueueCapacity);

    Map<String, Semaphore> _taskidRouteToStateWaitingSemaphore = new ConcurrentHashMap<>();

//    Map<String, Semaphore> _taskIdRouteToSendingWaitingSemaphore = new ConcurrentHashMap<>();

    Map<Integer, Map<Integer, Semaphore>> _taskIdToRouteToSendingWaitingSemaphore = new ConcurrentHashMap<>();

    Map<String, Semaphore> _taskIdRouteToCleanPendingTupleSemaphore = new ConcurrentHashMap<>();

    private Map<String, IConnection> _taskidRouteToConnection = new ConcurrentHashMap<>();

    ResourceMonitor resourceMonitor;

    Map<RouteId, String> routeIdToRemoteHost = new HashMap<>();

    static public ElasticTaskHolder instance() {
        return _instance;
    }

    static public ElasticTaskHolder createAndGetInstance(Map stormConf, String workerId, int port) {
        if(_instance==null) {
            _instance = new ElasticTaskHolder(stormConf, workerId, port);
        }
        return _instance;
    }

    private ElasticTaskHolder(Map stormConf, String workerId, int port) {
        System.out.println("creating ElasticTaskHolder");
        Config.overrideFromStormConf(stormConf);
        this.stormConf = stormConf;
        _context = new Context();
        _port = port + 10000;
        _context.prepare(stormConf);
        _inputConnection = _context.bind(workerId,_port);
        _priorityInputConnection = _context.bind(workerId, _port + 5);
        _remoteExecutionResultInputConnection = _context.bind(workerId, _port + 10);
        _workerId = workerId;
        _slaveActor = Slave.createActor(_workerId,Integer.toString(port));
        if(_slaveActor == null)
            System.out.println("NOTE: _slaveActor is null!!***************\n");
        createExecuteResultReceivingThread();
        createExecuteResultSendingThread();
//        createExecuteResultSendingThread();
        createPriorityReceivingThread();
        createRemoteExecutorResultReceivingThread();
        LOG.info("ElasticTaskHolder is launched.");
        LOG.info("storm id:" + workerId + " port:" + port);
        Utils.sleep(2000);
        _slaveActor.sendMessageToMaster("My pid is: " + ManagementFactory.getRuntimeMXBean().getName());
        resourceMonitor = new ResourceMonitor();
        createMetricsReportThread();
        createParallelismPredicationThread();
    }

    public void registerElasticBolt(BaseElasticBoltExecutor bolt, int taskId) {
        _bolts.put(taskId, bolt);
        _taskIdToRouteToSendingWaitingSemaphore.put(taskId, new ConcurrentHashMap<Integer, Semaphore>());
        _slaveActor.registerOriginalElasticTaskToMaster(taskId);
        createQueueUtilizationMonitoringThread(_sendingQueue, "Sending Queue", Config.ElasticTaskHolderOutputQueueCapacity, 0.9, null);
        LOG.info("A new ElasticTask is registered." + taskId);
    }

    public void sendMessageToMaster(String message) {
        _slaveActor.sendMessageToMaster(message);
    }


    public ElasticTaskMigrationMessage generateRemoteElasticTasks(int taskid, int route) throws RoutingTypeNotSupportedException, InvalidRouteException {
        if(!_bolts.containsKey(taskid)){
            System.err.println("task "+taskid+" does not exist! Remember to use withdraw command if you want to move a remote subtask to the original host!");
            throw new RuntimeException("task "+taskid+" does not exist! Remember to use withdraw command if you want to move a remote subtask to the original host!");
        }

        try {
//            System.out.println("Add exceptions to the routing table...");
            /* set exceptions for existing routing table and get the complement routing table */
            PartialHashingRouting complementHashingRouting = _bolts.get(taskid).get_elasticTasks().addExceptionForHashRouting(route, _sendingQueue);
            SmartTimer.getInstance().stop("SubtaskMigrate", "rerouting 1");
            SmartTimer.getInstance().start("SubtaskMigrate", "state construction");
    //        if(complementHashingRouting==null) {
    //            return null;
    //        }

            System.out.println("Constructing the instance of ElasticTask for remote execution...");
            /* construct the instance of ElasticTasks to be executed remotely */
            ElasticTasks existingElasticTasks = _bolts.get(taskid).get_elasticTasks();
            ElasticTasks remoteElasticTasks = new ElasticTasks(existingElasticTasks.get_bolt(),existingElasticTasks.get_taskID());
            remoteElasticTasks.setRemoteElasticTasks();
            remoteElasticTasks.set_routingTable(complementHashingRouting);

//            System.out.println("cleaning pending tuples for state consistency!");
//            existingElasticTasks.makesSureNoPendingTuples(route);

            System.out.println("Packing the involved state...");
            long start = System.currentTimeMillis();
            KeyValueState existingState = existingElasticTasks.get_bolt().getState();


            KeyValueState state = new KeyValueState();

            if(existingState != null) {
                for(Serializable key: existingState.getState().keySet()) {
    //                System.out.println("---->");
                    if(complementHashingRouting.route(key)>=0) {
                        state.setValueByKey(key, existingState.getValueByKey(key));
    //                    System.out.println("State <"+key+","+existingState.getValueByKey(key)+"> will be migrated!");
                    } else {
    //                    System.out.println("State <"+key+","+existingState.getValueByKey(key)+"> will be ignored!");
                    }
    //                System.out.println("<----");
                }
            } else {
                System.out.println("It's a stateless operator!");
            }

//            if(!state.getState().containsKey("payload")) {
//                final int stateSize = 1024;
//                Slave.getInstance().logOnMaster(String.format("%d bytes have been added to the state!", stateSize));
//                state.getState().put("payload", new byte[stateSize]);
//            }

//            sendMessageToMaster((System.currentTimeMillis() - start) + "ms to prepare the state to migrate!");
            System.out.println("State for migration is ready!");
//            sendMessageToMaster("State is ready for migration!");
            return new ElasticTaskMigrationMessage(remoteElasticTasks, _port, state);

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }


    }

    public ElasticTaskMigrationConfirmMessage handleGuestElasticTasks(ElasticTaskMigrationMessage message) {
        try {
//        sendMessageToMaster("ElasticTaskMigrationMessage: "+ message.getString());
        System.out.println("ElasticTaskMigrationMessage: "+ message.getString());
        System.out.println("#. of routes"+message._elasticTask.get_routingTable().getRoutes().size());
//        _remoteTasks.put(message._elasticTask.get_taskID(), message._elasticTask);


        if(!_originalTaskIdToRemoteTaskExecutor.containsKey(message._elasticTask.get_taskID())) {
            //This is the first RemoteTasks assigned to this host.
//            if(!_originalTaskIdToConnection.containsKey(message._elasticTask.get_taskID())) {
                Client iConnection = (Client)_context.connect(message._ip + ":" + message._port + "-" + message._elasticTask.get_taskID(), message._ip, message._port);
                while(iConnection.status()!=Client.Status.Ready)
                    Utils.sleep(1);
                _originalTaskIdToConnection.put(message._elasticTask.get_taskID(), iConnection);
//            }

//            if(!_originalTaskIdToPriorityConnection.containsKey(message._elasticTask.get_taskID())) {
                Client prioritizedConnection = (Client)_context.connect(message._ip + ":" + (message._port + 5) + "-" + message._elasticTask.get_taskID(), message._ip, message._port + 5);
                while(prioritizedConnection.status()!=Client.Status.Ready)
                    Utils.sleep(1);
                _originalTaskIdToPriorityConnection.put(message._elasticTask.get_taskID(), prioritizedConnection);
//            }

//            if(!_originalTaskIdToExecutorResultConnection.containsKey(message._elasticTask.get_taskID())) {
                Client remoteExecutionResultConnection = (Client) _context.connect(message._ip + ":" + (message._port + 10) + "-" + message._elasticTask.get_taskID(), message._ip, message._port + 10);
                while(remoteExecutionResultConnection.status()!=Client.Status.Ready)
                    Utils.sleep(1);
                _originalTaskIdToExecutorResultConnection.put(message._elasticTask.get_taskID(), remoteExecutionResultConnection);
//            }

            System.out.println("Connected with original Task Holders");
//            sendMessageToMaster("Connected with original Task holders!");
            
            System.out.println("create new remote task executor!");

            Slave.getInstance().logOnMaster("A new Remote Task Executor is created!##");

            ElasticRemoteTaskExecutor remoteTaskExecutor = new ElasticRemoteTaskExecutor(message._elasticTask, _sendingQueue, message._elasticTask.get_bolt());

            System.out.println("ElasticRemoteTaskExecutor is created!");

            _originalTaskIdToRemoteTaskExecutor.put(message._elasticTask.get_taskID(), remoteTaskExecutor);

            _taskIdToRouteToSendingWaitingSemaphore.put(message._elasticTask.get_taskID(), new ConcurrentHashMap<Integer, Semaphore>());

            System.out.println("ElasticRemoteTaskExecutor is added to the map!");
            remoteTaskExecutor.prepare(message.state);
            System.out.println("ElasticRemoteTaskExecutor is prepared!");
//            remoteTaskExecutor.createProcessingThread();
            Slave.getInstance().logOnMaster(remoteTaskExecutor._elasticTasks.get_routingTable().toString());

            System.out.println("Remote Task Executor is launched");
        } else {
            //There is already a RemoteTasks for that tasks on this host, so we just need to update the routing table
            //and create processing thread accordingly.
            System.out.println("integrate new subtask into existing remote task executor!");

            ElasticRemoteTaskExecutor remoteTaskExecutor = _originalTaskIdToRemoteTaskExecutor.get(message._elasticTask.get_taskID());
            remoteTaskExecutor._elasticTasks.get_bolt().getState().update(message.state);
//            for(Object key: message.state.getState().keySet()) {
//                System.out.println("State <"+key+", "+ message.state.getValueByKey(key)+"> has been restored!");
//            }
            System.out.println("Received original state!");
            remoteTaskExecutor.mergeRoutingTableAndCreateCreateWorkerThreads(message._elasticTask.get_routingTable());


        }

        ElasticTaskMigrationConfirmMessage confirmMessage = new ElasticTaskMigrationConfirmMessage(message._elasticTask.get_taskID(), _slaveActor.getIp() , _port, message._elasticTask.get_routingTable().getRoutes() );
        return confirmMessage;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    private void insertToConnectionToTaskMessageArray(Map<String, ArrayList<TaskMessage>> map, Map<String, IConnection> connectionNameToIConnection, IConnection connection, TaskMessage message) {
        String connectionName = connection.toString();
        if(!map.containsKey(connectionName)) {
            map.put(connectionName, new ArrayList<TaskMessage>());
            connectionNameToIConnection.put(connectionName, connection);
        }
        map.get(connectionName).add(message);
    }

    private void createExecuteResultSendingThread() {

        final Thread sendingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
//                        System.out.println("fetching...");

                        Map<String, ArrayList<TaskMessage>> iConnectionNameToTaskMessageArray = new HashMap<>();
                        Map<String, IConnection> connectionNameToIConnection = new HashMap<>();

                        ArrayList<ITaskMessage> drainer = new ArrayList<>();
                        ITaskMessage firstMessage = _sendingQueue.take();
                        drainer.add(firstMessage);
//                        _sendingQueue.drainTo(drainer, 2048);
                        _sendingQueue.drainTo(drainer);

                        for(ITaskMessage message: drainer) {
    //                        System.out.println("sending...");
//                            LOG.debug("An element is taken from the sendingQueue");
                            if(message instanceof RemoteTupleExecuteResult) {
//                                LOG.debug("The element is RemoteTupleExecuteResult");

                                RemoteTupleExecuteResult remoteTupleExecuteResult = (RemoteTupleExecuteResult)message;
//                                remoteTupleExecuteResult.spaceEfficientSerialize(remoteTupleExecuteResultSerializer);
                                if (_originalTaskIdToExecutorResultConnection.containsKey(remoteTupleExecuteResult._originalTaskID)) {
//                                    byte[] bytes = SerializationUtils.serialize(remoteTupleExecuteResult);
//                                    _originalTaskIdToExecutorResultConnection.get(remoteTupleExecuteResult._originalTaskID).send(remoteTupleExecuteResult._originalTaskID, bytes);
                                    byte[] bytes = remoteTupleExecuteResultSerializer.serialize(remoteTupleExecuteResult);
//                                    RemoteTupleExecuteResult recovered = remoteTupleExecuteResultDeserializer.deserializeToTuple(bytes);
//
//                                    if(recovered._originalTaskID!=remoteTupleExecuteResult._originalTaskID) {
//                                        System.out.println(String.format("OriginalTaskId is wrong. %d --> %d", remoteTupleExecuteResult._originalTaskID, recovered._originalTaskID));
//                                        System.out.println("\n\n\nBefore: " + remoteTupleExecuteResult.toString());
//                                        System.out.println("After: " + recovered.toString());
//                                    }

                                    TaskMessage taskMessage = new TaskMessage(remoteTupleExecuteResult._originalTaskID, bytes);
                                    insertToConnectionToTaskMessageArray(iConnectionNameToTaskMessageArray, connectionNameToIConnection, _originalTaskIdToExecutorResultConnection.get(remoteTupleExecuteResult._originalTaskID), taskMessage);

//                                    LOG.debug("RemoteTupleExecutorResult is send back!");
                                } else {
    //                                System.err.println("RemoteTupleExecuteResult will be ignored, because we cannot find the connection for tasks " + remoteTupleExecuteResult._originalTaskID);
                                }
                            } else if (message instanceof RemoteTuple) {

    //                            LOG.debug("The element is RemoteTuple");
                                RemoteTuple remoteTuple = (RemoteTuple) message;


                                final String key = remoteTuple.taskIdAndRoutePair();
    //                            LOG.debug("Key :"+key);
                                long startTime = -1;
                                while(!_taskidRouteToConnection.containsKey(key)) {
                                    Utils.sleep(1);
                                    if(startTime==-1)
                                        startTime = System.currentTimeMillis();

                                    if(System.currentTimeMillis() - startTime > 1000) {
                                        System.out.println("Cannot send tuples to " + remoteTuple._taskId +"." +remoteTuple._route + ", as it is not ready now!");
                                        sendMessageToMaster("Cannot send tuples to " + remoteTuple._taskId +"." +remoteTuple._route + ", as it is not ready now!");
                                        startTime = System.currentTimeMillis();
                                    }
                                }
                                byte[] bytes = tupleSerializer.serialize(remoteTuple._tuple);

                                TaskMessage taskMessage = new TaskMessage(remoteTuple._taskId + 10000, bytes);
                                taskMessage.setRemoteTuple();
                                insertToConnectionToTaskMessageArray(iConnectionNameToTaskMessageArray, connectionNameToIConnection, _taskidRouteToConnection.get(key), taskMessage);
//                                System.out.println(String.format("%s is sent to %s.", remoteTuple.toString(), _taskidRouteToConnection.get(key)));

                            } else if (message instanceof RemoteSubtaskTerminationToken) {
                                RemoteSubtaskTerminationToken remoteSubtaskTerminationToken = (RemoteSubtaskTerminationToken) message;
                                final String key = remoteSubtaskTerminationToken.taskid + "." + remoteSubtaskTerminationToken.route;
                                if(_taskidRouteToConnection.containsKey(key)) {
                                    final byte[] bytes = SerializationUtils.serialize(remoteSubtaskTerminationToken);
//                                    _taskidRouteToConnection.get(key).send(remoteSubtaskTerminationToken.taskid, bytes);
                                    TaskMessage taskMessage = new TaskMessage(remoteSubtaskTerminationToken.taskid, bytes);
                                    insertToConnectionToTaskMessageArray(iConnectionNameToTaskMessageArray, connectionNameToIConnection, _taskidRouteToConnection.get(key), taskMessage);
                                    System.out.println("RemoteSubtaskTerminationToken is sent to " +  _taskidRouteToConnection.get(key));
                                } else {
    //                                System.err.println("RemoteSubtaskTerminationToken does not have a valid taskid and route: " +key);
                                }

                            } else if (message instanceof RemoteState) {
                                RemoteState remoteState = (RemoteState) message;
                                byte[] bytes = SerializationUtils.serialize(remoteState);
                                IConnection connection = _originalTaskIdToConnection.get(remoteState._taskId);
                                if(connection != null) {
                                    if(!remoteState.finalized&& !_originalTaskIdToRemoteTaskExecutor.containsKey(remoteState._taskId)) {
                                        System.out.println("Remote state is ignored to send, as the state is not finalized ans the original RemoteTaskExecutor does not exist!");
                                        continue;
                                    }
//                                    connection.send(remoteState._taskId, bytes);
                                    TaskMessage taskMessage = new TaskMessage(remoteState._taskId, bytes);
                                    insertToConnectionToTaskMessageArray(iConnectionNameToTaskMessageArray, connectionNameToIConnection, connection, taskMessage);

                                    System.out.println("RemoteState is sent back!");
                                } else {
                                    System.err.println("Cannot find the connection for task " + remoteState._state);
                                    System.out.println("TaskId: " + remoteState._taskId);
                                    System.out.println("Connections: " + _originalTaskIdToConnection );
                                }
                            } else if (message instanceof MetricsForRoutesMessage) {
                                MetricsForRoutesMessage latencyForRoutes = (MetricsForRoutesMessage)message;
                                byte[] bytes = SerializationUtils.serialize(latencyForRoutes);
                                IConnection connection = _originalTaskIdToConnection.get(latencyForRoutes.taskId);
                                if(connection != null) {
                                    TaskMessage taskMessage = new TaskMessage(latencyForRoutes.taskId, bytes);
                                    insertToConnectionToTaskMessageArray(iConnectionNameToTaskMessageArray, connectionNameToIConnection, connection, taskMessage);
//                                    System.out.println("MetricsForRoutesMessage is sent!");
                                } else {
                                    System.err.println("Cannot find the connection for task " + latencyForRoutes.toString());
                                }



                            } else if (message instanceof CleanPendingTupleToken) {
//                                sendMessageToMaster("CleanPendingTuplesToken will be sent!");
                                CleanPendingTupleToken cleanPendingTupleToken = (CleanPendingTupleToken) message;
                                byte[] bytes = SerializationUtils.serialize(cleanPendingTupleToken);
                                IConnection connection = _taskidRouteToConnection.get(cleanPendingTupleToken.taskId+"."+cleanPendingTupleToken.routeId);
                                if(connection!=null) {
                                    TaskMessage taskMessage = new TaskMessage(cleanPendingTupleToken.taskId, bytes);
                                    insertToConnectionToTaskMessageArray(iConnectionNameToTaskMessageArray, connectionNameToIConnection, connection, taskMessage);

//                                    System.out.println("CleanPendingTuplesToken is sent to " + connection.toString());
//                                    sendMessageToMaster("CleanPendingTuplesToken is sent!");
                                } else {
                                    sendMessageToMaster("CleanPendingTupleToken cannot be sent, as the connection to the target is not found!!!");
                                    System.out.println("\"CleanPendingTupleToken canot be sent, as the connection to the target is not found!!!");
                                }
                            }

                            else {
                                System.err.print("Unknown element from the sending queue");
                            }
    //                        System.out.println("sent...");
                        }
                        for(String connectionName: iConnectionNameToTaskMessageArray.keySet()) {
                            if(!iConnectionNameToTaskMessageArray.get(connectionName).isEmpty()) {
                                connectionNameToIConnection.get(connectionName).send(iConnectionNameToTaskMessageArray.get(connectionName).iterator());
                            }
                        }
                        drainer.clear();

                    } catch (SerializationException ex) {
                        System.err.println("Serialization Error!");
                        ex.printStackTrace();
                    } catch (Exception eee) {
                        eee.printStackTrace();
                    }
                }
            }
        });
        sendingThread.start();
        System.out.println("sending thread is created!");

        createThreadUtilizationMonitoringThread(sendingThread.getId(), "Sending Thread", 0.7);
//        createThreadUtilizationMonitoringThread(sendingThread.getId(), "Sending Thread", -1);

    }

    public void createThreadUtilizationMonitoringThread(final long threadId, final String threadName, final double reportThreshold) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();
                long lastCpuTime = 0;
                try {
                    while(true) {
                        Thread.sleep(5000);
                        long cpuTime = tmxb.getThreadUserTime(threadId);
                        double utilization = (cpuTime - lastCpuTime) / 5E9;
                        lastCpuTime = cpuTime;
                        if(utilization > reportThreshold) {
                            sendMessageToMaster("cpu utilization of " + threadName + " reaches " + utilization);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public void createQueueUtilizationMonitoringThread(final LinkedBlockingQueue queue, final String queueName,final long capacity, final Double highWatermark, final Double lowWatermark) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();
                try {
                    while(true) {
                        Thread.sleep(1000);
                        int size = queue.size();
                        double utilization = (double)size/capacity;
                        if(highWatermark!=null&&utilization>highWatermark) {
                            sendMessageToMaster("The utilization of " + queueName + " reaches " + utilization);
                        }
                        if(lowWatermark!=null && utilization < lowWatermark) {
                            sendMessageToMaster("The utilization of " + queueName + "reaches " + utilization);
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }


    private void createPriorityReceivingThread() {
        Thread receivingThread =
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Iterator<TaskMessage> messageIterator = _priorityInputConnection.recv(0, 0);
                        while(messageIterator.hasNext()) {
                            TaskMessage message = messageIterator.next();
                            Object object = SerializationUtils.deserialize(message.message());
                            if(object instanceof RemoteState) {
                                RemoteState remoteState = (RemoteState) object;
                                handleRemoteState(remoteState);
                            } else if (object instanceof PendingTupleCleanedMessage) {
                                PendingTupleCleanedMessage cleanedMessage = (PendingTupleCleanedMessage) object;
                                handlePendingTupleCleanedMessage(cleanedMessage);
                            } else if (object instanceof MetricsForRoutesMessage) {
//                                System.out.println("MetricsForRoutesMessage");
                                MetricsForRoutesMessage latencyForRoutesMessage = (MetricsForRoutesMessage)object;
                                int taskId = latencyForRoutesMessage.taskId;
//                            sendMessageToMaster(String.format("Latency data at %s is received!", latencyForRoutesMessage.timeStamp));
                                _bolts.get(taskId).updateLatencyMetrics(latencyForRoutesMessage.latencyForRoutes);
                                _bolts.get(taskId).updateThroughputMetrics(latencyForRoutesMessage.throughputForRoutes);

                            } else {
                                System.err.println(" -_- Priority input connection receives unexpected object: " + object);
                                _slaveActor.sendMessageToMaster("-_- Priority input connection receives unexpected object: " + object);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        receivingThread.start();
        createThreadUtilizationMonitoringThread(receivingThread.getId(), "Priority Receiving Thread", 0.7);
    }

    private void createRemoteExecutorResultReceivingThread() {
        Thread receivingThread =
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Iterator<TaskMessage> messageIterator = _remoteExecutionResultInputConnection.recv(0, 0);
                        while(messageIterator.hasNext()) {
                            TaskMessage message = messageIterator.next();
//                            Object object = SerializationUtils.deserialize(message.message());
                            Object object = remoteTupleExecuteResultDeserializer.deserializeToTuple(message.message());
//                            Slave.getInstance().sendMessageToMaster("createRemoteExecutorResultReceivingThread got something: " + object);
                            if(object instanceof RemoteTupleExecuteResult) {
                                RemoteTupleExecuteResult result = (RemoteTupleExecuteResult)object;
//                                result.spaceEfficientDeserialize(remoteTupleExecuteResultDeserializer);
                                if(result._inputTuple != null)
                                    ((TupleImpl)result._inputTuple).setContext(_workerTopologyContext);
//                                System.out.println("A query result is received. originalTaskId =  "+result._originalTaskID + " TaskID =  " + result._taskId);
                                _bolts.get(result._originalTaskID).insertToResultQueue(result);
//                                System.out.println("a query result tuple is added into the input queue");
                            } else {
                                System.err.println("Remote Execution Result input connection receives unexpected object: " + object);
                                _slaveActor.sendMessageToMaster("Remote Execution Result input connection receives unexpected object: " + object);
                            }
                        }
//                        Utils.sleep(1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        receivingThread.start();
//        createThreadUtilizationMonitoringThread(receivingThread.getId(), "RemoteExecutorResult Receiving Thread", -1);
        createThreadUtilizationMonitoringThread(receivingThread.getId(), "RemoteExecutorResult Receiving Thread", 0.7);
    }


    private void createExecuteResultReceivingThread() {
        Thread receivingThread =
        new Thread(new Runnable() {
            @Override
            public void run() {
                int count = 0;
                while (true) {
                try {
//                    Utils.sleep(1);
                    Iterator<TaskMessage> messageIterator = _inputConnection.recv(0, 0);

//                    if(messageIterator!=null)
//                        continue;
                    while(messageIterator.hasNext()) {
                        TaskMessage message = messageIterator.next();
                        int targetTaskId = message.task();
//                        System.out.println("Handling a new TaskMessage!");
                        if(message.task() >= 10000) {
//                            System.out.println("Received a strange task with taskId + " + message.task());
                            int taskid = message.task() - 10000;
                            Tuple remoteTuple = tupleDeserializer.deserialize(message.message());
                            ElasticRemoteTaskExecutor elasticRemoteTaskExecutor = _originalTaskIdToRemoteTaskExecutor.get(taskid);
                            LinkedBlockingQueue<Object> queue = elasticRemoteTaskExecutor.get_inputQueue();
                            queue.put(remoteTuple);
                        } else {
                        Object object = SerializationUtils.deserialize(message.message());
//                        System.out.println(String.format("Received %s!", object));
                        if(object instanceof RemoteTupleExecuteResult) {
//                            System.out.println("RemoteTupleExecuteResult");
                            RemoteTupleExecuteResult result = (RemoteTupleExecuteResult)object;
                            result.spaceEfficientDeserialize(remoteTupleExecuteResultDeserializer);
                            ((TupleImpl)result._inputTuple).setContext(_workerTopologyContext);
//                            LOG.debug("A query result is received for "+result._originalTaskID);
                            _bolts.get(targetTaskId).insertToResultQueue(result);
//                            LOG.debug("a query result tuple is added into the input queue");
                        } else if (object instanceof RemoteTuple) {
//                            System.out.println("RemoteTuple");
                            RemoteTuple remoteTuple = (RemoteTuple) object;
                            try {
//                                System.out.println("A remote tuple " + remoteTuple._taskId + "." + remoteTuple._route + " (sid = " + remoteTuple.sid + ") is received!\n");
                                ((TupleImpl)remoteTuple._tuple).setContext(_workerTopologyContext);
                                ElasticRemoteTaskExecutor elasticRemoteTaskExecutor = _originalTaskIdToRemoteTaskExecutor.get(message.task());
                                LinkedBlockingQueue<Object> queue = elasticRemoteTaskExecutor.get_inputQueue();
                                queue.put(remoteTuple._tuple);
//                                System.out.println("handled!");
//                                LOG.debug("A remote tuple is added to the queue!\n");

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        } else if (object instanceof RemoteState) {
                            System.out.println("Received RemoteState!");
                            RemoteState remoteState = (RemoteState) object;
                            handleRemoteState(remoteState);

                        } else if (object instanceof RemoteSubtaskTerminationToken) {
                            System.out.print("Received a RemoteSubtaskTerminationToken!");
                            sendMessageToMaster("Received a RemoteSubtaskTerminationToken!");
                            RemoteSubtaskTerminationToken remoteSubtaskTerminationToken = (RemoteSubtaskTerminationToken) object;
                            terminateRemoteRoute(remoteSubtaskTerminationToken.taskid, remoteSubtaskTerminationToken.route);
                        } else if (object instanceof BucketToRouteReassignment) {
//                            sendMessageToMaster("Received BucketToRouteReassignment");
                            System.out.println("Received BucketToRouteReassignment");
                            BucketToRouteReassignment reassignment = (BucketToRouteReassignment)object;
                            handleBucketToRouteReassignment(reassignment);
                        } else if (object instanceof StateFlushToken) {
                            System.out.println("Received StateFlushToken!");
//                            sendMessageToMaster("Received StateFlushToken!");
                            StateFlushToken token = (StateFlushToken) object;

                            handleStateFlushToken(token);

                        } else if (object instanceof MetricsForRoutesMessage) {
                            System.out.println("MetricsForRoutesMessage");
                            MetricsForRoutesMessage latencyForRoutesMessage = (MetricsForRoutesMessage)object;
                            int taskId = latencyForRoutesMessage.taskId;
//                            sendMessageToMaster(String.format("Latency data at %s is received!", latencyForRoutesMessage.timeStamp));
                            _bolts.get(taskId).updateLatencyMetrics(latencyForRoutesMessage.latencyForRoutes);
                            _bolts.get(taskId).updateThroughputMetrics(latencyForRoutesMessage.throughputForRoutes);

                        } else if (object instanceof CleanPendingTupleToken) {
                            System.out.println("CleanPendingTupleToken");
                            CleanPendingTupleToken cleanPendingTupleToken = (CleanPendingTupleToken) object;
                            final int taskId = cleanPendingTupleToken.taskId;
                            _originalTaskIdToRemoteTaskExecutor.get(taskId).get_inputQueue().put(cleanPendingTupleToken);
//                            sendMessageToMaster("Received CleanPendingTupleToken");

//                            handleCleanPendingTupleToken(cleanPendingTupleToken);
                        }

                        else if (object instanceof String) {
                            System.out.println("Received Message: " + object);
                            sendMessageToMaster("Received Message: " + object);
                        } else {
                            System.err.println("Unexpected Object: " + object);
                        }
                        }
//                        System.out.println("Handled!");
                    }

                    } catch (Exception e ) {
                        e.printStackTrace();
                    }
                }
            }
        });
        receivingThread.start();
        createThreadUtilizationMonitoringThread(receivingThread.getId(), "Receiving Thread", 0.7);
//        createThreadUtilizationMonitoringThread(receivingThread.getId(), "Receiving Thread", -1);
    }

    private void handleRemoteState(RemoteState remoteState) {
        if(_bolts.containsKey(remoteState._taskId)) {
            if(getState(remoteState._taskId) == null)
                sendMessageToMaster("getState(remoteState._taskId) is null!");
            if(remoteState._state == null) {
                sendMessageToMaster("remoteState._state is null!");
            }
            getState(remoteState._taskId).update(remoteState._state);
            System.out.println("State ("+remoteState._state.size()+ " elements) has been updated!");
            if(remoteState.finalized) {
                System.out.println("Its finalized!");
                for(int route: remoteState._routes) {
                    if(_taskidRouteToStateWaitingSemaphore.containsKey(remoteState._taskId + "." + route)) {
                        _taskidRouteToStateWaitingSemaphore.get(remoteState._taskId+"."+route).release();
                        System.out.println("Semaphore for " + remoteState._taskId + "." + route + "has been released");
                    }
                }
            } else {
                System.out.println("It's now finalized!");
            }

        } else if (_originalTaskIdToRemoteTaskExecutor.containsKey(remoteState._taskId)) {
            getState(remoteState._taskId).update(remoteState._state);
            System.out.println("State ("+remoteState._state.size()+ " elements) has been updated!");

//            if(_taskidRouteToConnection.containsKey(remoteState._taskId+"."+remoteState._routes.get(0))) {
//                _taskidRouteToConnection.get(remoteState._taskId+"."+remoteState._routes.get(0)).send(remoteState._taskId, SerializationUtils.serialize(remoteState));
//            } else {
//                System.err.println("Cannot find remote connection for  ["+remoteState._taskId+"."+remoteState._routes.get(0));
//            }


        }
    }

    private void makeSureTargetRouteNoPendingTuples(int taskId, int routeId) {
        try {
        if(_bolts.containsKey(taskId)) {
            if(_bolts.get(taskId).get_elasticTasks().get_routingTable().getRoutes().contains(routeId)) {
//                sendMessageToMaster("Waiting for pending tuples to be cleaned!");
                System.out.println("Waiting for pending tuples to be cleaned!");
                _bolts.get(taskId).get_elasticTasks().makesSureNoPendingTuples(routeId);
                System.out.println("Waiting tuples are cleaned!");
//                sendMessageToMaster("Waiting tuples are cleaned!");
            } else {
                _taskIdRouteToCleanPendingTupleSemaphore.put(taskId + "." + routeId, new Semaphore(0));
                _sendingQueue.put(new CleanPendingTupleToken(taskId, routeId));
                System.out.println("Waiting for pending tuples to be cleaned [Remote]!");
//                sendMessageToMaster("Waiting for pending tuples to be cleaned [Remote]!");
                _taskIdRouteToCleanPendingTupleSemaphore.get(taskId+"."+routeId).acquire();
                _taskIdRouteToCleanPendingTupleSemaphore.remove(taskId + "." + routeId);
                System.out.println("Waiting tuples are cleaned [Remote]!");
//                sendMessageToMaster("Waiting tuples are cleaned [Remote]!");
            }
        }
        } catch (Exception e) {
            e.printStackTrace();
            sendMessageToMaster(e.getMessage());
        }

//        _bolts.get(taskId).get_elasticTasks().get_routingTable().getRoutes().contains(routeId)
//
//        if(_originalTaskIdToConnection.containsKey(taskId)) {
//        }

    }

    private void handlePendingTupleCleanedMessage(PendingTupleCleanedMessage message) {
        if(!_taskIdRouteToCleanPendingTupleSemaphore.containsKey(message.taskId+"."+message.routeId)) {
            System.out.println("PendingTupleCleanedMessage is not correct!");
        }
        _taskIdRouteToCleanPendingTupleSemaphore.get(message.taskId+"."+message.routeId).release();
        System.out.println("PendingTupleCleanedMessage is received for " + message.taskId+"."+message.routeId );
//        sendMessageToMaster("PendingTupleCleanedMessage is received for " + message.taskId+"."+message.routeId);
    }

    private void handleStateFlushToken(StateFlushToken token) {

        //TODO: there should be some mechanism to guarantee that the state is flushed until all the tuple has been processed
        KeyValueState partialState = getState(token._taskId).getValidState(token._filter);
        RemoteState remoteState = new RemoteState(token._taskId, partialState.getState(), token._targetRoute);

//        final int stateSize = 1024;
//        Slave.getInstance().logOnMaster(String.format("%d bytes have been added to the state!", stateSize));
//        remoteState._state.put("payload", new byte[stateSize]);

        remoteState.markAsFinalized();
        if(_originalTaskIdToPriorityConnection.containsKey(token._taskId)) {
            final byte[] bytes = SerializationUtils.serialize(remoteState);
            Slave.getInstance().reportStateMigrationToMaster(bytes.length);
            _originalTaskIdToPriorityConnection.get(token._taskId).send(token._taskId, bytes);
//            sendMessageToMaster("Remote state is send back to the original elastic holder!");
            System.out.print("Remote state is send back to the original elastic holder!");
        } else {

            System.out.print("Remote state does not need to be sent, as the remote state is already in the original holder!");
//            sendMessageToMaster("Remote state does not need to be sent, as the remote state is already in the original holder!");
//            handleRemoteState(remoteState); //@Li: This line is commented, as it seems that the state should not be migrate if the target subtask and the original subtask are in the same node.
        }
    }

    private void handleCleanPendingTupleToken(CleanPendingTupleToken token) {
        if(!_originalTaskIdToRemoteTaskExecutor.containsKey(token.taskId)) {
            sendMessageToMaster("Task " + token.taskId + " does not exist!");
        }
        System.out.println("to handle handleCleanPendingTupleToken");
        _originalTaskIdToRemoteTaskExecutor.get(token.taskId)._elasticTasks.makesSureNoPendingTuples(token.routeId);
        System.out.println(String.format("Pending tuple for %s.%s is cleaned!", token.taskId, token.routeId));
        PendingTupleCleanedMessage message = new PendingTupleCleanedMessage(token.taskId, token.routeId);
        _originalTaskIdToPriorityConnection.get(token.taskId).send(token.taskId, SerializationUtils.serialize(message));
//        sendMessageToMaster("PendingTupleCleanedMessage is sent back!");
    }

    private KeyValueState getState(int taskId) {
        KeyValueState ret = null;
        if(_bolts.containsKey(taskId)) {
            ret = _bolts.get(taskId).get_elasticTasks().get_bolt().getState();
            if(ret == null)
                sendMessageToMaster("_bolts.get(taskId).get_elasticTasks().get_bolt().getState() is null!");
        }
        else if (_originalTaskIdToRemoteTaskExecutor.containsKey(taskId)) {
            ret = _originalTaskIdToRemoteTaskExecutor.get(taskId)._bolt.getState();
            if(ret == null) {
                sendMessageToMaster("_originalTaskIdToRemoteTaskExecutor.get(taskId)._bolt.getState() is null!");
            }
        } else {
            sendMessageToMaster("State is not found for Task " + taskId);
        }
        return ret;
    }

    public KryoTupleSerializer getTupleSerializer() {
        return tupleSerializer;
    }

    private BalancedHashRouting getBalancedHashRoutingFromOriginalBolt(int taskid) {
        if(_bolts.containsKey(taskid)) {

                RoutingTable routingTable = _bolts.get(taskid).get_elasticTasks().get_routingTable();
                if(routingTable instanceof BalancedHashRouting) {
                    return (BalancedHashRouting)routingTable;
                } else if ((routingTable instanceof PartialHashingRouting) && (((PartialHashingRouting) routingTable).getOriginalRoutingTable() instanceof BalancedHashRouting)) {
                    return (BalancedHashRouting)((PartialHashingRouting) routingTable).getOriginalRoutingTable();
                }


            }
        return null;
    }

    private BalancedHashRouting getBalancedHashRoutingFromRemoteBolt(int taskid) {
        if(_originalTaskIdToRemoteTaskExecutor.containsKey(taskid)) {

            RoutingTable routingTable = _originalTaskIdToRemoteTaskExecutor.get(taskid)._elasticTasks.get_routingTable();
            if(routingTable instanceof BalancedHashRouting) {
                return (BalancedHashRouting)routingTable;
            } else if ((routingTable instanceof PartialHashingRouting) && (((PartialHashingRouting) routingTable).getOriginalRoutingTable() instanceof BalancedHashRouting)) {
                return (BalancedHashRouting)((PartialHashingRouting) routingTable).getOriginalRoutingTable();
            }
        }
        return null;
    }


    private void handleBucketToRouteReassignment(BucketToRouteReassignment reassignment) {
        if(_bolts.containsKey(reassignment.taskid)) {
            BalancedHashRouting balancedHashRouting = getBalancedHashRoutingFromOriginalBolt(reassignment.taskid);
            if(balancedHashRouting == null)
                throw new RuntimeException("Balanced Hash Routing is null!");
            for(int bucket: reassignment.reassignment.keySet()) {
                balancedHashRouting.reassignBucketToRoute(bucket, reassignment.reassignment.get(bucket));
//                sendMessageToMaster(bucket + " is reassigned to "+ reassignment.reassignment.get(bucket) + " in the original elastic task");
                System.out.println(bucket + " is reassigned to "+ reassignment.reassignment.get(bucket) + " in the original elastic task");
            }
        }
        if(_originalTaskIdToRemoteTaskExecutor.containsKey(reassignment.taskid)) {
            BalancedHashRouting balancedHashRouting = getBalancedHashRoutingFromRemoteBolt(reassignment.taskid);
            for(int bucket: reassignment.reassignment.keySet()) {
                balancedHashRouting.reassignBucketToRoute(bucket, reassignment.reassignment.get(bucket));
//                sendMessageToMaster(bucket + " is reassigned to "+ reassignment.reassignment.get(bucket) + " in the remote elastic task");
//                sendMessageToMaster(balancedHashRouting.toString());
                System.out.println(bucket + " is reassigned to "+ reassignment.reassignment.get(bucket) + " in the remote elastic task");
            }
        }
    }

    public void establishConnectionToRemoteTaskHolder(int taksId, int route, String remoteIp, int remotePort) {
        Client connection = (Client)_context.connect("",remoteIp,remotePort);
        while(connection.status()!= Client.Status.Ready)
            Utils.sleep(1);
        _taskidRouteToConnection.put(taksId+"."+route, connection);
//        connection.send(0, SerializationUtils.serialize("Hello!"));
        System.out.println("Established connection with remote task holder for " + taksId + "." + route);
//        sendMessageToMaster("Established connection with remote task holder for " + taksId + "." + route + " on " + remoteIp + ":" + remotePort);
    }

    public void createRouting(int taskid, int numberOfRouting, String type) throws TaskNotExistingException,RoutingTypeNotSupportedException {
        if(!_bolts.containsKey(taskid)) {
            throw new TaskNotExistingException(taskid);
        }

        if(type.equals("balanced_hash")) {
            createBalancedHashRouting(taskid, numberOfRouting);
        } else {

            if(!type.equals("hash"))
                throw new RoutingTypeNotSupportedException("Only support hash routing now!");
            _bolts.get(taskid).get_elasticTasks().setHashRouting(numberOfRouting);
        }
        _bolts.get(taskid).get_elasticTasks().get_routingTable().enableRoutingDistributionSampling();
        _slaveActor.sendMessageToMaster("New RoutingTable has been created!");
        System.out.println("RoutingTable has been created");


    }

    public void createBalancedHashRouting(int taskid, int numberOfRouting) throws TaskNotExistingException {
        if(!_bolts.containsKey(taskid)) {
            throw new TaskNotExistingException(taskid);
        }

        _bolts.get(taskid)._keyBucketSampler.clear();
        _bolts.get(taskid)._keyBucketSampler.enable();
//        _slaveActor.sendMessageToMaster("It will take " + Config.CreateBalancedHashRoutingSamplingTimeInSecs + "seconds to sample the distribution of the input tuples on the key domain.");
        Utils.sleep(Config.CreateBalancedHashRoutingSamplingTimeInSecs * 1000);
        _bolts.get(taskid)._keyBucketSampler.disable();
//        _slaveActor.sendMessageToMaster("Sampling completes");

        FirstFitDoubleDecreasing firstFitDoubleDecreasing = new FirstFitDoubleDecreasing(Arrays.asList(_bolts.get(taskid)._keyBucketSampler.buckets),numberOfRouting);

        final int result = firstFitDoubleDecreasing.getResult();
        if(result == numberOfRouting) {
//            _slaveActor.sendMessageToMaster(firstFitDoubleDecreasing.toString());
            _bolts.get(taskid).get_elasticTasks().setHashBalancedRouting(numberOfRouting, firstFitDoubleDecreasing.getBucketToPartitionMap());



        } else {
            _slaveActor.sendMessageToMaster("Failed to partition the buckets!");
        }


    }

    public void withdrawRemoteElasticTasks(int taskid, int route) throws TaskNotExistingException, RoutingTypeNotSupportedException, InvalidRouteException, HostNotExistException {
        SubtaskWithdrawTimer.getInstance().start();
        if(!_bolts.containsKey(taskid)) {
            throw new TaskNotExistingException(taskid);
        }
        RoutingTable routingTable = _bolts.get(taskid).get_elasticTasks().get_routingTable();
        if(!(routingTable instanceof PartialHashingRouting)) {
            throw new RoutingTypeNotSupportedException("Can only withdraw remote tasks for PartialHashingRouting!");
        }
        PartialHashingRouting partialHashingRouting = (PartialHashingRouting) routingTable;
        if(!partialHashingRouting.getOriginalRoutes().contains(route)) {
            throw new InvalidRouteException("Route " + route + " is not valid");
        }
        if(partialHashingRouting.getRoutes().contains(route)) {
            throw new InvalidRouteException("Route " + route + " is not in exception list");
        }

        _bolts.get(taskid).get_elasticTasks().addValidRoute(route);
        System.out.println("Route " + route + " has been added into the routing table!");
        sendFinalTuple(taskid, route);

        SubtaskWithdrawTimer.getInstance().prepared();

        System.out.println("RemoteSubtaskTerminationToken has been sent!");


        _taskidRouteToStateWaitingSemaphore.put(taskid+ "."+route,new Semaphore(0));
        try {
            System.out.println("Waiting for the remote state");
            _taskidRouteToStateWaitingSemaphore.get(taskid + "." + route).acquire();

            System.out.println("Remote state arrives!");

            System.out.println("launch the thread for "+taskid+"."+route+".");
            _bolts.get(taskid).get_elasticTasks().launchElasticTasksForGivenRoute(route);
            System.out.println("Remote " + taskid + "." + route + "has been withdrawn!");
            _slaveActor.sendMessageToMaster("Remote " + taskid + "." + route + "has been withdrawn!");
            SubtaskWithdrawTimer.getInstance().terminated();
//            _slaveActor.sendMessageToMaster(SubtaskWithdrawTimer.getInstance().toString());
            String originalHost = routeIdToRemoteHost.get(new RouteId(taskid, route));
            routeIdToRemoteHost.remove(new RouteId(taskid, route));
            _taskidRouteToConnection.remove(taskid + "." + route);
            _slaveActor.sendMessageToNode(originalHost, new TestAliveMessage("Alive after withdraw!"));

        } catch (InterruptedException e ) {
            e.printStackTrace();
        }
    }

    private void sendFinalTuple(int taskid, int route) {

        RemoteSubtaskTerminationToken remoteSubtaskTerminationToken = new RemoteSubtaskTerminationToken(taskid, route);
        try {
            _sendingQueue.put(remoteSubtaskTerminationToken);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * First terminate the query thread and clear the associated resources until all the local tuples for the route has been processed
     * Then get the state for the route and send the state back to the original ElasticTasks.
     * Finally, remove the route balls the routingTable.
     * @param taskid the taskid
     * @param route the route to remove
     */
    private void terminateRemoteRoute(int taskid, int route) {
        //terminate the thread and cleanup the resources.
        ElasticRemoteTaskExecutor remoteTaskExecutor = _originalTaskIdToRemoteTaskExecutor.get(taskid);
        remoteTaskExecutor._elasticTasks.terminateGivenQuery(route);

        /**
         * Ideally, remote task executor should be removed when it does not have any task.
         * The removal operation is pending now, as there is a well hidden bug caused by the removal.
         */
        if(remoteTaskExecutor._elasticTasks.get_routingTable().getRoutes().size()==0) {
            System.out.println("Removing the elastic task...");
            removeEmptyRemoteTaskExecutor(taskid);
            System.out.println("Removed the elastic task...");
        }

        try {
            //get state and send back
            RemoteState state = remoteTaskExecutor.getStateForRoutes(route);
            state.markAsFinalized();
            _sendingQueue.put(state);
            System.out.println("Final State for " + taskid +"." + route + " has been sent back");


        } catch (InterruptedException e ) {
            e.printStackTrace();
        }

        ((PartialHashingRouting)remoteTaskExecutor._elasticTasks.get_routingTable()).addExceptionRoute(route);
        System.out.println("Route "+ route+ " has been removed from the routing table");






    }

    private void removeEmptyRemoteTaskExecutor(int taskid) {
        ElasticRemoteTaskExecutor remoteTaskExecutor = _originalTaskIdToRemoteTaskExecutor.get(taskid);
        remoteTaskExecutor.close();
        _originalTaskIdToRemoteTaskExecutor.remove(taskid);

        System.out.println("RemoteTaskExecutor " + taskid + " is interrupted!");
    }

    public void setworkerTopologyContext(WorkerTopologyContext context) {
        _workerTopologyContext = context;
        tupleDeserializer = new KryoTupleDeserializer(stormConf, context);
        tupleSerializer = new KryoTupleSerializer(stormConf, context);

        remoteTupleExecuteResultDeserializer = new RemoteTupleExecuteResultDeserializer(stormConf, context, tupleDeserializer);
        remoteTupleExecuteResultSerializer = new RemoteTupleExecuteResultSerializer(stormConf, context, tupleSerializer);



    }

    public WorkerTopologyContext getWorkerTopologyContext() {
        return _workerTopologyContext;
    }

    public double getThroughput(int taskid) {
        if(!_bolts.containsKey(taskid))
            return -1;
        return _bolts.get(taskid).getMetrics().getRecentThroughput(5000);
//        return _bolts.get(taskid).getInputRate();
    }

    public Histograms getDistribution(int taskid) throws TaskNotExistingException {
        if(!_bolts.containsKey(taskid)) {
            throw new TaskNotExistingException("Task " + taskid + " does not exist!");
        } else {
//            return _bolts.get(taskid).get_elasticTasks()._sample.getDistribution();
            return _bolts.get(taskid).get_elasticTasks().get_routingTable().getRoutingDistribution();
        }
    }

    public RoutingTable getOriginalRoutingTable(int taskid) throws TaskNotExistingException {
        if(!_bolts.containsKey(taskid)) {
            throw new TaskNotExistingException("task " + taskid + "does not exist!");
        } else {
            return _bolts.get(taskid).get_elasticTasks().get_routingTable();
        }
    }

    public RoutingTable getRoutingTable(int taskid) throws TaskNotExistingException {
        if(!_bolts.containsKey(taskid)) {
            throw new TaskNotExistingException("task " + taskid + "does not exist!");
        } else {
            return _bolts.get(taskid).getCompleteRoutingTable();
        }
    }

    public void reassignHashBucketToRoute(int taskid, int bucketId, int orignalRoute, int targetRoute) throws TaskNotExistingException, RoutingTypeNotSupportedException, InvalidRouteException, BucketNotExistingException {
        SmartTimer.getInstance().start("ShardReassignment","total");
        SmartTimer.getInstance().start("ShardReassignment","prepare");


        System.out.println("===Start Shard Reassignment " + bucketId + " " + taskid + "." + orignalRoute + "---->" + taskid + "." + targetRoute);

        if(!_bolts.containsKey(taskid)) {
            throw new TaskNotExistingException("Task " + taskid + " does not exist balls the ElasticHolder!");
        }
        if(getBalancedHashRoutingFromOriginalBolt(taskid)==null) {
            throw new RoutingTypeNotSupportedException("ReassignHashBucketToRoute only applies on BalancedHashRouting or PartialHashRouting with a internal BalancedHashRouting");
        }

        BalancedHashRouting balancedHashRouting;// = (BalancedHashRouting)_bolts.get(taskid).get_elasticTasks().get_routingT  
        if(_bolts.get(taskid).get_elasticTasks().get_routingTable() instanceof BalancedHashRouting) {
            balancedHashRouting = (BalancedHashRouting)_bolts.get(taskid).get_elasticTasks().get_routingTable();
        } else {
            balancedHashRouting = (BalancedHashRouting)((PartialHashingRouting)_bolts.get(taskid).get_elasticTasks().get_routingTable()).getOriginalRoutingTable();
        }

        if(!(balancedHashRouting.getRoutes().contains(orignalRoute))) {
            throw new InvalidRouteException("Original Route " + orignalRoute + " does not exist!");
        }

        if(!(balancedHashRouting.getRoutes().contains(targetRoute))) {
            throw new InvalidRouteException("Target Route " + targetRoute + " does not exist!");
        }


        if(!balancedHashRouting.getBucketSet().contains(bucketId)) {
            throw new BucketNotExistingException("Bucket " + bucketId + " does not exist balls the balanced hash routing table!");
        }

        BucketToRouteReassignment reassignment = new BucketToRouteReassignment(taskid, bucketId, targetRoute);

        String targetHost, originalHost;
        if(routeIdToRemoteHost.containsKey(new RouteId(taskid, orignalRoute)))
            originalHost = routeIdToRemoteHost.get(new RouteId(taskid, orignalRoute));
        else
            originalHost = "local";

        if(routeIdToRemoteHost.containsKey(new RouteId(taskid, targetRoute)))
            targetHost = routeIdToRemoteHost.get(new RouteId(taskid, targetRoute));
        else
            targetHost = "local";

//        sendMessageToMaster("From " + originalHost + " to " + targetHost);
        System.out.println("From " + originalHost + " to " + targetHost);

        SmartTimer.getInstance().stop("ShardReassignment", "prepare");

        // Update the routing table on the target subtask
        if(_taskidRouteToConnection.containsKey(taskid+"."+targetRoute)){
            _taskidRouteToConnection.get(taskid+"."+targetRoute).send(taskid, SerializationUtils.serialize(reassignment));
        }
//        else {
//            handleBucketToRouteReassignment(reassignment);
//        }


        SmartTimer.getInstance().start("ShardReassignment","rerouting");
        // Pause sending RemoteTuples to the target subtask
        pauseSendingToTargetSubtask(taskid, targetRoute);
        pauseSendingToTargetSubtask(taskid, orignalRoute);

        // Update the routing table on original ElasticTaskHolder
        handleBucketToRouteReassignment(reassignment);

//        // Update the routing table on the source
//        if(_taskidRouteToConnection.containsKey(taskid+"."+orignalRoute)){
////            sendMessageToMaster("BucketToRouteReassignment is sent to the original Host ");
//            _taskidRouteToConnection.get(taskid+"."+orignalRoute).send(taskid, SerializationUtils.serialize(reassignment));
//        }
        SmartTimer.getInstance().stop("ShardReassignment", "rerouting");
        SmartTimer.getInstance().start("ShardReassignment","state migration");
//        sendMessageToMaster("Begin state migration session!");

        // 3. handle state for that shard, if necessary

        // before state migration, we should make sure there is no pending tuples for consistency!
        makeSureTargetRouteNoPendingTuples(taskid, orignalRoute);
        // Update the routing table on the source
        if(_taskidRouteToConnection.containsKey(taskid+"."+orignalRoute)){
//            sendMessageToMaster("BucketToRouteReassignment is sent to the original Host ");
            _taskidRouteToConnection.get(taskid+"."+orignalRoute).send(taskid, SerializationUtils.serialize(reassignment));
        }
//         Update the routing table on the target subtask
//        if(_taskidRouteToConnection.containsKey(taskid+"."+targetRoute)){
//            _taskidRouteToConnection.get(taskid+"."+targetRoute).send(taskid, SerializationUtils.serialize(reassignment));
//        }

        if(!targetHost.equals(originalHost)) {
                HashBucketFilter filter = new HashBucketFilter(balancedHashRouting.getNumberOfBuckets(), bucketId);
            if(!originalHost.equals("local")) {
                StateFlushToken stateFlushToken = new StateFlushToken(taskid, orignalRoute, filter);
                _taskidRouteToConnection.get(taskid+ "." + orignalRoute).send(taskid, SerializationUtils.serialize(stateFlushToken));
//                _slaveActor.sendMessageToMaster("State Flush Token has been sent to " + originalHost);
                _taskidRouteToStateWaitingSemaphore.put(taskid+ "." + orignalRoute, new Semaphore(0));
                try {
//                    _slaveActor.sendMessageToMaster("Waiting for remote state!");
                    _taskidRouteToStateWaitingSemaphore.get(taskid+ "." + orignalRoute).acquire();
//                    _slaveActor.sendMessageToMaster("Remote state arrived!");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
//                _slaveActor.sendMessageToMaster("State for the shard does not need to be flushed, as the source subtask is run on the original host!");
            }
            if (!targetHost.equals("local")) {
                KeyValueState partialState = getState(taskid).getValidState(filter);

                if(!partialState.getState().containsKey("payload")) {
//                    final int stateSize = 1024 * 1024 * 32;
//                    Slave.getInstance().logOnMaster(String.format("%d bytes have been added to the state!", stateSize));
//                    partialState.getState().put("payload", new byte[stateSize]);
                }

                RemoteState remoteState = new RemoteState(taskid, partialState.getState(), targetRoute);
                final byte[] bytes = SerializationUtils.serialize(remoteState);
                _taskidRouteToConnection.get(taskid + "." + targetRoute).send(taskid, bytes);
                Slave.getInstance().reportStateMigrationToMaster(bytes.length);
//                sendMessageToMaster("State has been sent to " + targetHost);
            } else {
//                _slaveActor.sendMessageToMaster("State for the shard does not need to migrate, as the target subtask is run on the original host!");
            }
        } else {
//            _slaveActor.sendMessageToMaster("State movement is not necessary, as the shard is moved within a host!");
        }
        SmartTimer.getInstance().stop("ShardReassignment", "state migration");

        // 5. resume sending RemoteTuples to the target subtask
        System.out.println("Begin to resume!");
        resumeSendingToTargetSubtask(taskid, targetRoute);
        resumeSendingToTargetSubtask(taskid, orignalRoute);
        System.out.println("Resumed!");
        SmartTimer.getInstance().stop("ShardReassignment", "total");
//        sendMessageToMaster("Reassignment completes!");
//        _slaveActor.sendMessageToMaster(SmartTimer.getInstance().getTimerString("ShardReassignment"));
        System.out.println("===End Shard Reassignment " + bucketId + " " + taskid + "." + orignalRoute + "---->" + taskid + "." + targetRoute);

    }

    public boolean waitIfStreamToTargetSubtaskIsPaused(int targetTask, int route) {
//        System.out.println("waitIfStreamToTargetSubtaskIsPaused!");
        if(_taskIdToRouteToSendingWaitingSemaphore.get(targetTask).containsKey(route)) {
            final String key = targetTask+"."+route;
            try {
                System.out.println("Sending stream to " + targetTask + "." + route + " is paused. Waiting for resumption!");
//                _taskIdRouteToSendingWaitingSemaphore.get(key).acquire();
                Semaphore semaphore = _taskIdToRouteToSendingWaitingSemaphore.get(targetTask).get(route);
                semaphore.acquire();
//                synchronized (_taskIdRouteToSendingWaitingSemaphore) {
                    if(_taskIdToRouteToSendingWaitingSemaphore.get(targetTask).containsKey(route) && _taskIdToRouteToSendingWaitingSemaphore.get(targetTask).get(route).equals(semaphore)) {
                        _taskIdToRouteToSendingWaitingSemaphore.get(targetTask).remove(route);
                        System.out.println("Semaphore for " + key + " is removed.");
                    }
                    else {
                        System.out.println("Semaphore for " + key + " is not removed, because the semaphore is not the original one!");
                    }
//                    _taskIdRouteToSendingWaitingSemaphore.remove(key);
//                }
                System.out.println( key+" is resumed!!!!!");
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    void pauseSendingToTargetSubtask(int targetTask, int route) {
        synchronized (_taskIdToRouteToSendingWaitingSemaphore.get(targetTask)) {
            _taskIdToRouteToSendingWaitingSemaphore.get(targetTask).put(route, new Semaphore(0));
        }
        final String key = targetTask+"."+route;
        System.out.println("Sending to " + key + " is paused!");

    }

    void resumeSendingToTargetSubtask(int targetTask, int route) {
        String key = targetTask + "." + route;
        synchronized (_taskIdToRouteToSendingWaitingSemaphore.get(targetTask)) {
            if (!_taskIdToRouteToSendingWaitingSemaphore.get(targetTask).containsKey(route)) {
                System.out.println("cannot resume " + key + " because the semaphore does not exist!");
                return;
            }
            _taskIdToRouteToSendingWaitingSemaphore.get(targetTask).get(route).release();
        }
        System.out.println("Sending to " + key + "is resumed!");

    }

    public Histograms getBucketDistributionForBalancedRoutingTable(int taskId) {
        if(!_bolts.containsKey(taskId)) {
            System.out.println("Task " + taskId + "does not exist!");
            return null;
        }
        RoutingTable routingTable = _bolts.get(taskId).get_elasticTasks().get_routingTable();
        return ((BalancedHashRouting)RoutingTableUtils.getBalancecHashRouting(routingTable)).getBucketsDistribution();
    }

    public void migrateSubtask(String targetHost, int taskId, int routeId)  throws InvalidRouteException, RoutingTypeNotSupportedException, HostNotExistException, TaskNotExistingException {
        String workerLogicalName = _slaveActor.getLogicalName();
        String workerName = _slaveActor.getName();

        String routeName = taskId + "." + routeId;

        if(!_bolts.containsKey(taskId))
            throw new TaskNotExistingException(String.format("Task %d does not exist or is not running on %s", taskId, workerName));

        if(!RoutingTableUtils.getOriginalRoutes(_bolts.get(taskId).get_elasticTasks().get_routingTable()).contains(routeId))
            throw new InvalidRouteException(routeId);

        if(targetHost.equals(routeIdToRemoteHost.get(new RouteId(taskId, routeId))))
            throw new RuntimeException("Cannot migrate " + taskId + "." + routeId + ", because the task is already running on the host ");


        if(_bolts.containsKey(taskId) && _bolts.get(taskId).get_elasticTasks().get_routingTable().getRoutes().contains(routeId)) {
            if(!workerLogicalName.equals(targetHost)) {
                _slaveActor.sendMessageToMaster("========== Migration from local to remote ========= " + routeName);
                migrateSubtaskToRemoteHost(targetHost, taskId, routeId);
                _slaveActor.sendMessageToMaster("====================== E N D ====================== " + routeName);

            }
            else
                throw new RuntimeException("Cannot migrate " + taskId + "." + routeId + " on " + targetHost + ", because the subtask is already running on the host!");
        } else {
            if(workerName.equals(targetHost)) {
                _slaveActor.sendMessageToMaster("========== Migration from remote to local! ========== " + routeName);
                withdrawRemoteElasticTasks(taskId,routeId);
                _slaveActor.sendMessageToMaster("====================== E N D ====================== " + routeName);

            } else {
                _slaveActor.sendMessageToMaster("========== Migration from remote to remote! ==========  " + routeName);
                System.out.println("===Withdraw Start===");
                withdrawRemoteElasticTasks(taskId, routeId);
                System.out.println("===Withdraw End===");
                _slaveActor.sendMessageToMaster("========== Remote->local is done ==========  " + routeName);
                _slaveActor.sendMessageToMaster("========== local->remote is starting ==========  " + routeName);
                System.out.println("===Migrate Start===");
                migrateSubtaskToRemoteHost(targetHost, taskId, routeId);
                System.out.println("===Migrate End===");
                migrateSubtaskToRemoteHost(targetHost, taskId, routeId);
                _slaveActor.sendMessageToMaster("====================== E N D ====================== " + routeName);
            }
        }

    }
      
    public void migrateSubtaskToRemoteHost(String targetHost, int taskId, int routeId) throws InvalidRouteException, RoutingTypeNotSupportedException, HostNotExistException {
//        _slaveActor.sendMessageToNode(targetHost, new TestAliveMessage("at the beginning of migrateSubtaskToRemoteHost. "));
        SmartTimer.getInstance().start("SubtaskMigrate", "rerouting 1");
        ElasticTaskMigrationMessage migrationMessage = ElasticTaskHolder.instance().generateRemoteElasticTasks(taskId, routeId);
        SmartTimer.getInstance().stop("SubtaskMigrate", "state construction");
        SmartTimer.getInstance().start("SubtaskMigrate", "state migration");
//        migrationMessage.state = new HashMap<>();
        routeIdToRemoteHost.put(new RouteId(taskId, routeId), targetHost);
        System.out.println("Sending the ElasticTaskMigrationMessage to " + targetHost);
//        sendMessageToMaster("State contains " + migrationMessage.state.keySet().size() + " elements.");
//        sendMessageToMaster("Serialization size: " + SerializationUtils.serialize(migrationMessage).length);
        final RemoteState remoteState = new RemoteState(-1, migrationMessage.state, -1);
        Slave.getInstance().reportStateMigrationToMaster(SerializationUtils.serialize(remoteState).length);
//        _slaveActor.sendMessageToNode(targetHost, new TestAliveMessage("Before sending..."));
//        Utils.sleep(10);
//        migrationMessage.id = new Random().nextInt();
//        sendMessageToMaster("Migration Message Id = " + migrationMessage.id);
        ElasticTaskMigrationConfirmMessage confirmMessage = (ElasticTaskMigrationConfirmMessage) _slaveActor.sendMessageToNodeAndWaitForResponse(targetHost, migrationMessage);
        SmartTimer.getInstance().stop("SubtaskMigrate", "state migration");
        SmartTimer.getInstance().start("SubtaskMigrate", "reconnect");
        System.out.println("Received ElasticTaskMigrationConfirmMessage!");
        if(confirmMessage != null)
            handleElasticTaskMigrationConfirmMessage(confirmMessage);
        else {
            sendMessageToMaster("Waiting for confirm message time out!");
        }
//        RemoteState remoteState = new RemoteState(taskId, state, routeId);
        SmartTimer.getInstance().stop("SubtaskMigrate", "reconnect");
//        sendMessageToMaster(SmartTimer.getInstance().getTimerString("SubtaskMigrate"));
//        _slaveActor.sendMessageToNode(targetHost, new TestAliveMessage("local to remote migration completes!"));
//        _taskidRouteToConnection.get(taskId + "." + routeId).send(taskId, SerializationUtils.serialize(remoteState));
    }

    private void handleElasticTaskMigrationConfirmMessage(ElasticTaskMigrationConfirmMessage confirmMessage) {

        String ip = confirmMessage._ip;
        int port = confirmMessage._port;
        int taskId = confirmMessage._taskId;

        System.out.print("Received ElasticTaskMigrationConfirmMessage #. routes: "+confirmMessage._routes.size());
        for(int i: confirmMessage._routes) {
            establishConnectionToRemoteTaskHolder(taskId, i, ip, port);
        }
        sendMessageToMaster("Task Migration completes!");
    }

    public String handleSubtaskLevelLoadBalancingCommand(int taskId) {
        try {
            if(!_bolts.containsKey(taskId)) {
                throw new TaskNotExistingException(taskId);
            }

            SmartTimer.getInstance().start("Subtask Level Load Balancing", "Prepare");

            RoutingTable routingTable = _bolts.get(taskId).get_elasticTasks().get_routingTable();

            BalancedHashRouting balancedHashRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);

            if(balancedHashRouting == null) {
                throw new RoutingTypeNotSupportedException("Only support balanced hash routing for scaling out now!");
            }

            int numberOfSubtasks = balancedHashRouting.getNumberOfRoutes();
            // collect the necessary metrics first.
            Histograms histograms = balancedHashRouting.getBucketsDistribution();
            Map<Integer, Integer> shardToRoutingMapping = balancedHashRouting.getBucketToRouteMapping();

            ArrayList<SubTaskWorkload> subTaskWorkloads = new ArrayList<>();
            for(int i = 0; i < numberOfSubtasks; i++ ) {
                subTaskWorkloads.add(new SubTaskWorkload(i));
            }
            for(int shardId: histograms.histograms.keySet()) {
                subTaskWorkloads.get(shardToRoutingMapping.get(shardId)).increaseOrDecraeseWorkload(histograms.histograms.get(shardId));
            }

            Map<Integer, Set<ShardWorkload>> subtaskToShards = new HashMap<>();
            for(int i = 0; i < numberOfSubtasks; i++) {
                subtaskToShards.put(i, new HashSet<ShardWorkload>());
            }


            for(int shardId: shardToRoutingMapping.keySet()) {
                int subtask = shardToRoutingMapping.get(shardId);
                final Set<ShardWorkload> shardWorkloads = subtaskToShards.get(subtask);
                final long workload = histograms.histograms.get(shardId);
                shardWorkloads.add(new ShardWorkload(shardId, workload));
            }

            SmartTimer.getInstance().stop("Subtask Level Load Balancing", "Prepare");
            SmartTimer.getInstance().start("Subtask Level Load Balancing", "Algorithm");
            Comparator<ShardWorkload> shardComparator = ShardWorkload.createReverseComparator();
            Comparator<SubTaskWorkload> subTaskComparator = SubTaskWorkload.createReverseComparator();
            ShardReassignmentPlan plan = new ShardReassignmentPlan();

            boolean moved = true;

            while(moved) {
                moved = false;
                Collections.sort(subTaskWorkloads, subTaskComparator);
                for(int i = subTaskWorkloads.size() - 1 ; i > 0; i --) {
                    SubTaskWorkload subtaskWorloadToMoveIn = subTaskWorkloads.get(i);
                    SubTaskWorkload subtaskWorloadToMoveFrom = subTaskWorkloads.get(0);
                    Set<ShardWorkload> shardWorkloadsInTheSubtaskWithLargestWorkload = subtaskToShards.get(subtaskWorloadToMoveFrom.subtaskId);
                    List<ShardWorkload> sortedShardWorkloadsInTheSubtaskWithLargestWorkload = new ArrayList<>(shardWorkloadsInTheSubtaskWithLargestWorkload);
                    Collections.sort(sortedShardWorkloadsInTheSubtaskWithLargestWorkload, shardComparator);
                    boolean localMoved = false;
                    for(ShardWorkload shardWorkload: sortedShardWorkloadsInTheSubtaskWithLargestWorkload) {
                        if(shardWorkload.workload + subtaskWorloadToMoveIn.workload < subtaskWorloadToMoveFrom.workload) {
                            plan.addReassignment(taskId, shardWorkload.shardId, subtaskWorloadToMoveFrom.subtaskId, subtaskWorloadToMoveIn.subtaskId);
                            localMoved = true;
                            moved = true;
                            shardWorkloadsInTheSubtaskWithLargestWorkload.remove(shardWorkload);
                            subtaskWorloadToMoveFrom.increaseOrDecraeseWorkload(-shardWorkload.workload);
                            subtaskToShards.get(subtaskWorloadToMoveIn.subtaskId).add(shardWorkload);
                            subtaskWorloadToMoveIn.increaseOrDecraeseWorkload(shardWorkload.workload);
                        }
                    }
                    if(localMoved)
                        break;

                }
            }
            SmartTimer.getInstance().stop("Subtask Level Load Balancing", "Algorithm");
            sendMessageToMaster(plan.toString());
            SmartTimer.getInstance().start("Subtask Level Load Balancing", "Deploy");
            for(ShardReassignment reassignment: plan.getReassignmentList()) {
//                sendMessageToMaster("=============== START ===============");
                reassignHashBucketToRoute(taskId, reassignment.shardId, reassignment.originalRoute, reassignment.newRoute);
//                sendMessageToMaster("=============== END ===============");
            }
            SmartTimer.getInstance().stop("Subtask Level Load Balancing", "Deploy");
            sendMessageToMaster(SmartTimer.getInstance().getTimerString("Subtask Level Load Balancing"));
            sendMessageToMaster("Subtask Level Load Balancing finishes with " + plan.getReassignmentList().size() + " movements!");


        } catch (Exception e) {
            e.printStackTrace();
            sendMessageToMaster(e.getMessage());
        }
        return "Succeed!";
    }

    public Status handleScalingInSubtaskCommand(int taskId) {
        /**
         * Subtask with the largest index will be removed to achieve scaling in.
         * Shard already assigned to that subtask will be moved to other existing subtask in a load balance manner.
         *
         */

        try {
            System.out.println("begin to handle scaling in subtask command...");
            SmartTimer.getInstance().start("Scaling In Subtask", "prepare");
            if(!_bolts.containsKey(taskId)) {
                throw new TaskNotExistingException(taskId);
            }
            RoutingTable routingTable = _bolts.get(taskId).get_elasticTasks().get_routingTable();

            BalancedHashRouting balancedHashRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);

            if(balancedHashRouting == null) {
                throw new RoutingTypeNotSupportedException("Only support balanced hash routing for scaling in now!");
            }

            int targetSubtaskId = balancedHashRouting.getNumberOfRoutes() -1;
            int numberOfSubtasks = balancedHashRouting.getNumberOfRoutes();
            // collect the necessary metrics first.
            System.out.println("begin to collect the metrics...");
            Histograms histograms = balancedHashRouting.getBucketsDistribution();
            Map<Integer, Integer> shardToRoutingMapping = balancedHashRouting.getBucketToRouteMapping();

            ArrayList<SubTaskWorkload> subTaskWorkloads = new ArrayList<>();
            for(int i = 0; i < numberOfSubtasks; i++) {
                subTaskWorkloads.add(new SubTaskWorkload(i));
            }
            for(int shardId: histograms.histograms.keySet()) {
                subTaskWorkloads.get(shardToRoutingMapping.get(shardId)).increaseOrDecraeseWorkload(histograms.histograms.get(shardId));
            }

            Map<Integer, Set<ShardWorkload>> subtaskToShards = new HashMap<>();
            for(int i = 0; i < numberOfSubtasks; i++) {
                subtaskToShards.put(i, new HashSet<ShardWorkload>());
            }
            for(int shardId: shardToRoutingMapping.keySet()) {
                int subtask = shardToRoutingMapping.get(shardId);
                final Set<ShardWorkload> shardWorkloads = subtaskToShards.get(subtask);
                final long workload = histograms.histograms.get(shardId);
                shardWorkloads.add(new ShardWorkload(shardId, workload));
            }
            SmartTimer.getInstance().stop("Scaling In Subtask", "prepare");
            SmartTimer.getInstance().start("Scaling In Subtask", "algorithm");
            System.out.println("begin to compute the shard reassignments...");
            Set<ShardWorkload> shardsForTargetSubtask = subtaskToShards.get(targetSubtaskId);
            List<ShardWorkload> sortedShards = new ArrayList<>(shardsForTargetSubtask);
            Collections.sort(sortedShards, ShardWorkload.createReverseComparator());

            ShardReassignmentPlan plan = new ShardReassignmentPlan();

            Comparator<SubTaskWorkload> subTaskComparator = SubTaskWorkload.createReverseComparator();
            subTaskWorkloads.remove(subTaskWorkloads.size() - 1);
            for(ShardWorkload shardWorkload: sortedShards) {
                Collections.sort(subTaskWorkloads, subTaskComparator);
                SubTaskWorkload subtaskWorkloadToMoveIn = subTaskWorkloads.get(0);
                plan.addReassignment(taskId, shardWorkload.shardId, targetSubtaskId, subtaskWorkloadToMoveIn.subtaskId);
                subtaskWorkloadToMoveIn.increaseOrDecraeseWorkload(shardWorkload.workload);
            }
            SmartTimer.getInstance().stop("Scaling In Subtask", "algorithm");
            System.out.println(plan.toString());
            SmartTimer.getInstance().start("Scaling In Subtask", "deploy");
            System.out.println("begin to conduct shard reassignments...");
            sendMessageToMaster(String.format("Scaling in will conduct %d reassignments!", plan.getReassignmentList().size()));
            int count = 0;
            for(ShardReassignment reassignment: plan.getReassignmentList()) {
//                sendMessageToMaster("=============== START ===============");
                reassignHashBucketToRoute(taskId, reassignment.shardId, reassignment.originalRoute, reassignment.newRoute);
                count ++;
                if(count % 10 == 0) {
                    System.out.println(String.format("%d reassignments have been conducted!", count));
                }
//                sendMessageToMaster("=============== END ===============");
            }
            SmartTimer.getInstance().stop("Scaling In Subtask", "deploy");

            SmartTimer.getInstance().start("Scaling In Subtask", "Termination");
            if(_taskidRouteToConnection.containsKey(taskId + "." + targetSubtaskId)){
                withdrawRemoteElasticTasks(taskId, targetSubtaskId);
            }
            _bolts.get(taskId).get_elasticTasks().terminateGivenQuery(targetSubtaskId);
            SmartTimer.getInstance().stop("Scaling In Subtask", "Termination");

            SmartTimer.getInstance().start("Scaling In Subtask", "update routing table");
            balancedHashRouting.scalingIn();
            SmartTimer.getInstance().stop("Scaling In Subtask", "update routing table");


//            sendMessageToMaster(SmartTimer.getInstance().getTimerString("Scaling In Subtask"));

            sendMessageToMaster("Scaling in completes with " + plan.getReassignmentList().size() + " movements.");

            String string = "";
            for(ShardReassignment reassignment: plan.getReassignmentList()) {
                string += reassignment.shardId + " ";
            }
            sendMessageToMaster(string);

            sendMessageToMaster("Current DOP: " + balancedHashRouting.getRoutes().size());
            System.out.println("scaling in subtask command is done, current Dop = " + balancedHashRouting.getRoutes().size());

        } catch (Exception e) {
            e.printStackTrace();
            return Status.Error(e.getMessage());
        }
        return Status.OK();
    } 
    public Status handleScalingOutSubtaskCommand(int taskId){
        try {
            System.out.println("Begin to handle scaling out subtask command!");
            SmartTimer.getInstance().start("ScalingOut", "Create Empty subtask");
            if(!_bolts.containsKey(taskId)) {
                throw new TaskNotExistingException(taskId);
            }
            RoutingTable routingTable = _bolts.get(taskId).get_elasticTasks().get_routingTable();

            BalancedHashRouting balancedHashRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);

            if(balancedHashRouting == null) {
                throw new RoutingTypeNotSupportedException("Only support balanced hash routing for scaling out now!");
            }


            // collect necessary statistics first, otherwise those data might not be available after scaling out of the routing table.
            Histograms histograms = balancedHashRouting.getBucketsDistribution();
            Map<Integer, Integer> shardToRoutingMapping = balancedHashRouting.getBucketToRouteMapping();

            int newSubtaskId = routingTable.scalingOut();

            _bolts.get(taskId).get_elasticTasks().createAndLaunchElasticTasksForGivenRoute(newSubtaskId);

            // so far, a new, empty subtask is create. The next step is to move some shards from existing subtasks.


            SmartTimer.getInstance().stop("ScalingOut", "Create Empty subtask");
            SmartTimer.getInstance().start("ScalingOut", "Algorithm");
            System.out.println("Begin to compute shard reassignments...");
            ArrayList<SubTaskWorkload> subTaskWorkloads = new ArrayList<>();
            for(int i = 0; i < newSubtaskId; i++ ) {
                subTaskWorkloads.add(new SubTaskWorkload(i));
            }
            for(int shardId: histograms.histograms.keySet()) {
                subTaskWorkloads.get(shardToRoutingMapping.get(shardId)).increaseOrDecraeseWorkload(histograms.histograms.get(shardId));
            }

            Map<Integer, Set<ShardWorkload>> subtaskToShards = new HashMap<>();
            for(int i = 0; i < newSubtaskId; i++) {
                subtaskToShards.put(i, new HashSet<ShardWorkload>());
            }


            for(int shardId: shardToRoutingMapping.keySet()) {
                int subtask = shardToRoutingMapping.get(shardId);
                final Set<ShardWorkload> shardWorkloads = subtaskToShards.get(subtask);
                final long workload = histograms.histograms.get(shardId);
                shardWorkloads.add(new ShardWorkload(shardId, workload));
            }

            long targetSubtaskWorkload = 0;
            Comparator<ShardWorkload> shardComparator = ShardWorkload.createReverseComparator();
            Comparator<SubTaskWorkload> subTaskReverseComparator = SubTaskWorkload.createReverseComparator();
            ShardReassignmentPlan plan = new ShardReassignmentPlan();
            boolean moved = true;
            while(moved) {
                moved = false;
                Collections.sort(subTaskWorkloads, subTaskReverseComparator);
                for(SubTaskWorkload subTaskWorkload: subTaskWorkloads) {
                    int subtask = subTaskWorkload.subtaskId;
                    List<ShardWorkload> shardWorkloads = new ArrayList<>(subtaskToShards.get(subtask));
                    Collections.sort(shardWorkloads, shardComparator);
                    boolean localMoved = false;
                    for(ShardWorkload shardWorkload: shardWorkloads) {
                        if(targetSubtaskWorkload + shardWorkload.workload < subTaskWorkload.workload) {
                            plan.addReassignment(taskId, shardWorkload.shardId, subTaskWorkload.subtaskId, newSubtaskId);
                            subtaskToShards.get(subTaskWorkload.subtaskId).remove(new ShardWorkload(shardWorkload.shardId));
                            targetSubtaskWorkload += shardWorkload.workload;
                            subTaskWorkload.increaseOrDecraeseWorkload(-shardWorkload.workload);
                            localMoved = true;
                            moved = true;
//                            System.out.println("Move " + shardWorkload.shardId + " from " + subTaskWorkload.subtaskId + " to " + newSubtaskId);
                            break;
                        }
                    }
                    if(localMoved) {
                        break;
                    }

                }
            }
            SmartTimer.getInstance().stop("ScalingOut", "Algorithm");

//            for(int i = 0; i < newSubtaskId; i++) {
//                sendMessageToMaster("Subtask " + i + ": " + subTaskWorkloads.get(i).workload);
//            }
//            sendMessageToMaster("new subtask: " + targetSubtaskWorkload);

            SmartTimer.getInstance().start("ScalingOut", "Conduct");
            System.out.println("Begin to conduct shard reassignments...");
            sendMessageToMaster(String.format("Scaling out will conduct %d reassignments!", plan.getReassignmentList().size()));
            for(ShardReassignment reassignment: plan.getReassignmentList()) {
//                sendMessageToMaster("=============== START ===============");
                reassignHashBucketToRoute(taskId, reassignment.shardId, reassignment.originalRoute, reassignment.newRoute);
//                sendMessageToMaster("=============== END ===============");
            }
            SmartTimer.getInstance().stop("ScalingOut", "Conduct");
            sendMessageToMaster(SmartTimer.getInstance().getTimerString("ScalingOut"));
            sendMessageToMaster("Scaling out succeeds with " + plan.getReassignmentList().size() + " movements!");
            String shardMovements = "";
            for(ShardReassignment reassignment: plan.getReassignmentList()) {
                shardMovements += reassignment.shardId + " ";
            }
            sendMessageToMaster(shardMovements);
            sendMessageToMaster("Current DOP: " + balancedHashRouting.getRoutes().size());
            System.out.println("Scaling out command is handed. The current Dop is " + balancedHashRouting.getRoutes().size());
            return Status.OK();

        } catch (Exception e) {
            e.printStackTrace();
            sendMessageToMaster(e.getMessage());
            return Status.Error(e.getMessage());
        }

    }

    private void createMetricsReportThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                    while(true) {
                try {
                        Thread.sleep(1000);

                        for(int remoteTaskId: _originalTaskIdToRemoteTaskExecutor.keySet()) {
                            ExecutionLatencyForRoutes latencyForRoutes = _originalTaskIdToRemoteTaskExecutor.get(remoteTaskId)._elasticTasks.getExecutionLatencyForRoutes();
                            ThroughputForRoutes throughputForRoutes = _originalTaskIdToRemoteTaskExecutor.get(remoteTaskId)._elasticTasks.getThroughputForRoutes();
                            MetricsForRoutesMessage message = new MetricsForRoutesMessage(remoteTaskId, latencyForRoutes, throughputForRoutes);

                            message.setTimeStamp(Calendar.getInstance().getTime().toString());
                            _originalTaskIdToPriorityConnection.get(remoteTaskId).send(remoteTaskId, SerializationUtils.serialize(message));
//                            _sendingQueue.put(message);
                        }
                        for(int taskId: _bolts.keySet()) {
                            ExecutionLatencyForRoutes latencyForRoutes = _bolts.get(taskId).get_elasticTasks().getExecutionLatencyForRoutes();
                            ThroughputForRoutes throughputForRoutes = _bolts.get(taskId).get_elasticTasks().getThroughputForRoutes();
                            _bolts.get(taskId).getMetrics().updateLatency(latencyForRoutes);
                            _bolts.get(taskId).getMetrics().updateThroughput(throughputForRoutes);

//                            System.out.println("Latency is added to the local metrics!");
//                            System.out.println(latencyForRoutes);

                            // get state size;
                            long stateSize = 0;
                            KeyValueState state = _bolts.get(taskId).get_elasticTasks().get_bolt().getState();
                            if(state != null)
                                stateSize = state.getStateSize();

                            // get data transfer rate
                            final long dataTransferRate = _bolts.get(taskId).getDataTransferRateInBytesPerSecond();

                            // emit the metric message to master.
                            _slaveActor.sendMessageObjectToMaster(new ElasticExecutorMetricsReportMessage(taskId, stateSize, dataTransferRate));
                        }
                } catch (InterruptedException e) {
                    System.out.println("Metrics report thread is terminated!");
                } catch (Exception e) {
                    e.printStackTrace();
                    sendMessageToMaster(e.getMessage());
                }
                    }
            }
        }).start();

        sendMessageToMaster("Metrics report thread is created!");
    }

    private void createParallelismPredicationThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        Thread.sleep(1000);
                        for(int taskId: _bolts.keySet()) {
                            int currentParallelism = _bolts.get(taskId).getCurrentParallelism();
                            int desirableParallelism = _bolts.get(taskId).getDesirableParallelism();
//                            sendMessageToMaster("Task: " + taskId + " average latency: " + _bolts.get(taskId).getMetrics().getAverageLatency());
//                            sendMessageToMaster("Task: " + taskId + " rate: " + _bolts.get(taskId).getInputRate());
//                            sendMessageToMaster("Task " + taskId + ":  " + currentParallelism + "---->" + desirableParallelism);
                            if(currentParallelism < desirableParallelism) {
                                ExecutorScalingOutRequestMessage requestMessage = new ExecutorScalingOutRequestMessage(taskId);
                                _slaveActor.sendMessageObjectToMaster(requestMessage);
                            } else if (currentParallelism > desirableParallelism) {
                                ExecutorScalingInRequestMessage requestMessage = new ExecutorScalingInRequestMessage(taskId);
                                _slaveActor.sendMessageObjectToMaster(requestMessage);
                            }

                            _slaveActor.sendMessageObjectToMaster(new DesirableParallelismMessage(taskId, desirableParallelism));

                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    sendMessageToMaster("Error happens on createParallelismPredicationThread on " + _slaveActor.getIp() +e.getMessage());
                }
            }
        }).start();

        sendMessageToMaster("Parallelism Predication thread is created on" + _slaveActor.getIp());
    }

}
