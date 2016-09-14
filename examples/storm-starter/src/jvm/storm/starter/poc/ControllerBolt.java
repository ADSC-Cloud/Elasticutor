package storm.starter.poc;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.routing.BalancedHashRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.scheduler.ElasticScheduler;
import backtype.storm.elasticity.scheduler.ShardReassignment;
import backtype.storm.elasticity.scheduler.ShardReassignmentPlan;
import backtype.storm.elasticity.state.KeyValueState;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.elasticity.utils.timer.SmartTimer;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import storm.starter.generated.ResourceCentricControllerService;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Created by Robert on 9/13/16.
 */
public class ControllerBolt extends BaseRichBolt implements ResourceCentricControllerService.Iface {

    OutputCollector collector;

    Map<Integer, Histograms> taskToHistogram;

    List<Integer> upstreamTaskIds, downStreamTaskIds;

    BalancedHashRouting routingTable;

    Map<Integer, Semaphore> sourceTaskIdToPendingTupleCleanedSemaphore = new ConcurrentHashMap<>();
    Map<Integer, Semaphore> targetTaskIdToWaitingStateMigrationSemaphore = new ConcurrentHashMap<>();
    Map<Integer, Semaphore> sourceTaskIndexToResumingWaitingSemaphore = new ConcurrentHashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, final OutputCollector collector) {
        this.collector = collector;

        taskToHistogram = new HashMap<>();

        upstreamTaskIds = context.getComponentTasks(PocTopology.ForwardBolt);
        downStreamTaskIds = context.getComponentTasks(PocTopology.TransactionBolt);

        routingTable = new BalancedHashRouting(downStreamTaskIds.size());

        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(1000);
                        collector.emit(PocTopology.UPSTREAM_COMMAND_STREAM, new Values("getHistograms", 0, 0, 0));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

        createThriftThread(this);

//        createLoadBalancingThread();

    }

    private void createThriftThread(final ControllerBolt bolt) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ResourceCentricControllerService.Processor processor = new ResourceCentricControllerService.Processor(bolt);
//                    MasterService.Processor processor = new MasterService.Processor(_instance);
                    TServerTransport serverTransport = new TServerSocket(29090);
                    TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

                    Slave.getInstance().logOnMaster("Controller daemon is started on " + InetAddress.getLocalHost().getHostAddress());
                    server.serve();
                } catch (Exception e) {
                    e.printStackTrace();
                    Slave.getInstance().logOnMaster(e.getMessage());
                }

            }
        }).start();
    }

    @Override
    public void execute(Tuple input) {
        String streamId = input.getSourceStreamId();
        if(streamId.equals(PocTopology.STATISTICS_STREAM)) {
            int sourceTaskId = input.getInteger(0);
            Histograms histograms = (Histograms)input.getValue(1);
            taskToHistogram.put(sourceTaskId, histograms);
        } else if(streamId.equals(PocTopology.STATE_MIGRATION_STREAM)) {
            int sourceTaskOffset = input.getInteger(0);
            int targetTaskOffset = input.getInteger(1);
            int shardId = input.getInteger(2);
            KeyValueState state = (KeyValueState) input.getValue(3);
            sourceTaskIdToPendingTupleCleanedSemaphore.get(sourceTaskOffset).release();
            System.out.println("New State is forwarded!");
            collector.emitDirect(downStreamTaskIds.get(targetTaskOffset), PocTopology.STATE_UPDATE_STREAM, new Values(targetTaskOffset, state));
        } else if (streamId.equals(PocTopology.STATE_READY_STREAM)) {
            System.out.println("STATE_READY_STREAM is received!");
            int targetTaskOffset = input.getInteger(0);
            targetTaskIdToWaitingStateMigrationSemaphore.get(targetTaskOffset).release();
            System.out.println("targetTaskIdToWaitingStateMigrationSemaphore is released.");
        } else if (streamId.equals(PocTopology.FEEDBACK_STREAM)) {
            String command = input.getString(0);
            if(command.equals("resumed")) {
                int sourceTaskIndex = input.getInteger(1);
                sourceTaskIndexToResumingWaitingSemaphore.get(sourceTaskIndex).release();
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(PocTopology.UPSTREAM_COMMAND_STREAM, new Fields("Command", "arg1", "arg2", "arg3"));
        declarer.declareStream(PocTopology.STATE_UPDATE_STREAM, new Fields("targetTaskId", "state"));

    }

    @Override
    public void shardReassignment(int sourceTaskIndex, int targetTaskIndex, int shardId) throws org.apache.thrift.TException {
        Slave.getInstance().logOnMaster(String.format("Shard reassignment %d: %d --> %d is called!", shardId, sourceTaskIndex, targetTaskIndex));
        long startTime = System.currentTimeMillis();
        try {
            if(sourceTaskIndex >= downStreamTaskIds.size()) {
                Slave.getInstance().logOnMaster("Invalid source task index!");
            }
            if(targetTaskIndex >= downStreamTaskIds.size()) {
                Slave.getInstance().logOnMaster("Invalid target task index!");
            }
            if(shardId > Config.NumberOfShard) {
                Slave.getInstance().logOnMaster("Invalid target task index!");
            }

            if(routingTable.getBucketToRouteMapping().get(shardId)!=sourceTaskIndex) {
                Slave.getInstance().logOnMaster(String.format("Shard %d does not belong to %d.", shardId, sourceTaskIndex));
                return;
            }

            Slave.getInstance().logOnMaster(String.format("Begin to migrate shard %d from %d to %d!", shardId, sourceTaskIndex, targetTaskIndex));

            int sourceTaskId = downStreamTaskIds.get(sourceTaskIndex);

            sourceTaskIdToPendingTupleCleanedSemaphore.put(sourceTaskIndex, new Semaphore(0));

            Slave.getInstance().logOnMaster(String.format("Controller: sending pausing"));

            targetTaskIdToWaitingStateMigrationSemaphore.put(targetTaskIndex, new Semaphore(0));

            collector.emit(PocTopology.UPSTREAM_COMMAND_STREAM, new Values("pausing", sourceTaskIndex, targetTaskIndex, shardId));

            Slave.getInstance().logOnMaster(String.format("Waiting..."));
            sourceTaskIdToPendingTupleCleanedSemaphore.get(sourceTaskIndex).acquire();
            Slave.getInstance().logOnMaster(String.format("pending Tuples are cleaned!"));


            targetTaskIdToWaitingStateMigrationSemaphore.get(targetTaskIndex).acquire();
            Slave.getInstance().logOnMaster(String.format("Shard reassignment of shard %d from %d to %d is ready!", shardId, sourceTaskId, targetTaskIndex));

            sourceTaskIndexToResumingWaitingSemaphore.put(sourceTaskIndex, new Semaphore(0));
            collector.emit(PocTopology.UPSTREAM_COMMAND_STREAM, new Values("resuming", sourceTaskIndex, targetTaskIndex, shardId));
            sourceTaskIndexToResumingWaitingSemaphore.get(sourceTaskIndex).acquire();

            routingTable.reassignBucketToRoute(shardId, targetTaskIndex);

            Slave.getInstance().logOnMaster(String.format("Shard reassignment is completed! (%d ms)", System.currentTimeMillis() - startTime));


        } catch (InterruptedException e) {
            e.printStackTrace();
            Slave.getInstance().logOnMaster(e.getMessage());
        }
    }

    @Override
    public void scalingIn() throws TException {

    }

    @Override
    public void scalingOut() throws TException {

    }
    private boolean getSkewness() {
        Histograms histograms = new Histograms();
        for(int taskId: taskToHistogram.keySet()) {
            histograms.merge(taskToHistogram.get(taskId));
        }
        try {
            double workloadFactor = ElasticScheduler.getSkewnessFactor(histograms, routingTable);
            return workloadFactor >= Config.taskLevelLoadBalancingThreshold;
        } catch (Exception e) {
            return false;
        }

    }
    @Override
    public void loadBalancing() throws TException {

        SmartTimer.getInstance().start("Load Balancing", "Algorithm");
        Histograms histograms = new Histograms();
        for(int taskId: taskToHistogram.keySet()) {
            histograms.merge(taskToHistogram.get(taskId));
        }

        try {
//            double workloadFactor = ElasticScheduler.getSkewnessFactor(histograms, routingTable);
//            workloadFactor >= Config.taskLevelLoadBalancingThreshold;
            boolean skewness = getSkewness();


            if(skewness) {

//                ShardReassignmentPlan plan = getCompleteShardToTaskMapping(taskId, histograms, numberOfRoutes, balancedHashRouting.getBucketToRouteMapping());
                ShardReassignmentPlan plan = ElasticScheduler.getMinimizedShardToTaskReassignment(0, routingTable.getNumberOfRoutes(), routingTable.getBucketToRouteMapping(), histograms);

                SmartTimer.getInstance().stop("Load Balancing", "Algorithm");
                SmartTimer.getInstance().start("Load Balancing", "Reassignments");
                if(!plan.getReassignmentList().isEmpty()) {
                    for(ShardReassignment reassignment: plan.getReassignmentList()) {
                        shardReassignment(reassignment.originalRoute, reassignment.newRoute, reassignment.shardId);
                    }
                } else {
                    System.out.println("Shard Assignment is not modified after optimization.");
                }
                SmartTimer.getInstance().stop("Load Balancing", "Reassignments");
                Slave.getInstance().logOnMaster(SmartTimer.getInstance().getTimerString("Load Balancing"));
                Slave.getInstance().logOnMaster(plan.getReassignmentList().size() + " shard reassignments has be performed for load balancing! ");
            } else {
//                Slave.getInstance().logOnMaster("No shard reassignment will be performed!");
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String queryRoutingTable() throws TException {
        return null;
    }

    private void createLoadBalancingThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
//                    Thread.sleep(10000);
                    while(true) {
                        Thread.sleep(1000);
                        loadBalancing();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
