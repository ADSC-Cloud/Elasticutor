package storm.starter;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.common.ShardWorkload;
import backtype.storm.elasticity.common.SubTaskWorkload;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.routing.TwoTireRouting;
import backtype.storm.elasticity.routing.RoutingTableUtils;
import backtype.storm.elasticity.scheduler.ElasticScheduler;
import backtype.storm.elasticity.scheduler.ShardReassignment;
import backtype.storm.elasticity.scheduler.ShardReassignmentPlan;
import backtype.storm.elasticity.state.KeyValueState;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.elasticity.utils.timer.SmartTimer;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Created by Robert on 4/5/16.
 */
public class ResourceCentricControllerBolt implements IRichBolt, ResourceCentricControllerService.Iface {

    OutputCollector collector;

    Map<Integer, Histograms> taskToHistogram;

    Map<Integer, Double> taskToRate;

    Map<Integer, Long> taskToLatency;

    TwoTireRouting routingTable;

    List<Integer> downstreamTaskIds;

    List<Integer> upstreamTaskIds;

    Map<Integer, Semaphore> sourceTaskIdToPendingTupleCleanedSemphore = new ConcurrentHashMap<>();

    Map<Integer, Semaphore> targetTaskIdToWaitingStateMigrationSemphore = new ConcurrentHashMap<>();

    Map<Integer, Semaphore> sourceTaskIndexToResumingWaitingSemphore = new ConcurrentHashMap<>();

    long[] generatorTaskProgress;

    long progress;

    final long progressTolerance = 200;

    @Override
    public void prepare(Map stormConf, TopologyContext context, final OutputCollector collector) {
        this.collector = collector;

        taskToHistogram = new HashMap<>();
        taskToRate = new HashMap<>();
        taskToLatency = new HashMap<>();

        upstreamTaskIds = context.getComponentTasks(ResourceCentricZipfComputationTopology.GeneratorBolt);

        downstreamTaskIds = context.getComponentTasks(ResourceCentricZipfComputationTopology.ComputationBolt);

        routingTable = new TwoTireRouting(downstreamTaskIds.size());

        generatorTaskProgress = new long[upstreamTaskIds.size()];

        progress = 0;


        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(1000);
                        collector.emit(ResourceCentricZipfComputationTopology.UpstreamCommand, new Values("getHistograms", 0, 0, 0));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();


        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(5000);
                        Histograms histograms = new Histograms();
                        for(int taskId: taskToHistogram.keySet()) {
                            histograms.merge(taskToHistogram.get(taskId));
                        }
//                        Slave.getInstance().logOnMaster(String.format("Rate: %f, Latency: %d", getRate(), getLatency()));

//                        Slave.getInstance().logOnMaster(histograms.toString());

                        Map<Integer, Integer> bucketToRoute = routingTable.getBucketToRouteMapping();

                        long[] routeCount = new long[routingTable.getNumberOfRoutes()];

                        for(int bucket: histograms.histograms.keySet()) {
                            routeCount[bucketToRoute.get(bucket)] += histograms.histograms.get(bucket);
                        }

                        long sum = 0;
                        long min = Long.MAX_VALUE;
                        long max = Long.MIN_VALUE;
                        String str = "";
                        for(long count: routeCount) {

                            sum += count;
                            str += count + " ";
                            min = Math.min(min, count);
                            max = Math.max(max, count);
                        }
//                        Slave.getInstance().logOnMaster(str);
//                        Slave.getInstance().logOnMaster(String.format("Max: %d Min: %d avg: %d |p = %2.4f", max, min, sum/routeCount.length, sum/routeCount.length / (double)max));


                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

        createThriftThread(this);

//        createAutomaticScalingThread();

//        createLoadBalancingThread();
//        createSeedUpdatingThread();
    }

    @Override
    public void execute(Tuple input) {
        String streamId = input.getSourceStreamId();
        if(streamId.equals("statics")) {
            int sourceTaskId = input.getInteger(0);
            Histograms histograms = (Histograms)input.getValue(1);
            taskToHistogram.put(sourceTaskId, histograms);
        } else if (streamId.equals(ResourceCentricZipfComputationTopology.StateMigrationStream)) {
//            Slave.getInstance().logOnMaster("Will forward the state!");
            int sourceTaskOffset = input.getInteger(0);
            int targetTaskOffset = input.getInteger(1);
            int shardId = input.getInteger(2);
            KeyValueState state = (KeyValueState) input.getValue(3);
            sourceTaskIdToPendingTupleCleanedSemphore.get(sourceTaskOffset).release();
            collector.emitDirect(downstreamTaskIds.get(targetTaskOffset), ResourceCentricZipfComputationTopology.StateUpdateStream, new Values(targetTaskOffset, state));
        } else if (streamId.equals(ResourceCentricZipfComputationTopology.StateReadyStream)) {
            int targetTaskOffset = input.getInteger(0);
            targetTaskIdToWaitingStateMigrationSemphore.get(targetTaskOffset).release();
        } else if (streamId.equals(ResourceCentricZipfComputationTopology.FeedbackStream)) {
            String command = input.getString(0);
            if(command.equals("resumed")) {
                int sourceTaskIndex = input.getInteger(1);
                sourceTaskIndexToResumingWaitingSemphore.get(sourceTaskIndex).release();
            }
        } else if (streamId.equals(ResourceCentricZipfComputationTopology.RateAndLatencyReportStream)) {
            int taskId = input.getInteger(0);
            double rate = input.getDouble(1);
            long latency = input.getLong(2);
            taskToLatency.put(taskId, latency);
            taskToRate.put(taskId, rate);
        } else if (streamId.equals(ResourceCentricZipfComputationTopology.CountReportSteram)) {
            int taskid = input.getInteger(0);
            long localProgress = input.getLong(1);
            generatorTaskProgress[taskid] = localProgress;
            long min = Long.MAX_VALUE;
            int i = 0;
            for(long p: generatorTaskProgress) {
                min = Math.min(p, min);
//                Slave.getInstance().logOnMaster(String.format("Progress of Task %d: %d", i++, p ));
            }
//            Slave.getInstance().logOnMaster(String.format("progress: %d, min : %d", progress, min));
            if(min > progress) {
                progress = min;
//                Slave.getInstance().logOnMaster(String.format("CountPermission is updated to %d", progress + progressTolerance));
                collector.emit(ResourceCentricZipfComputationTopology.CountPermissionStream, new Values(progress + progressTolerance));
            }
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(ResourceCentricZipfComputationTopology.UpstreamCommand, new Fields("Command", "arg1", "arg2", "arg3"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.StateUpdateStream, new Fields("targetTaskId", "state"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.SeedUpdateStream, new Fields("seed"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.CountPermissionStream, new Fields("permissionCount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

//    @Override
//    public void shardReassignment(int sourceTaskIndex, int targetTaskIndex, int shardId) throws TException {
//        try {
//            if(sourceTaskIndex >= downstreamTaskIds.size())
//                return;
//            if(targetTaskIndex >= downstreamTaskIds.size())
//                return;
//            if(shardId > Config.NumberOfShard)
//                return;
//
//            int sourceTaskId = downstreamTaskIds.get(sourceTaskIndex);
//
//            sourceTaskIdToPendingTupleCleanedSemphore.put(sourceTaskId, new Semaphore(0));
//
//            collector.emit(ResourceCentricZipfComputationTopology.UpstreamCommand, new Values("pausing", sourceTaskId, targetTaskIndex, shardId));
//
//            sourceTaskIdToPendingTupleCleanedSemphore.get(sourceTaskId).acquire();
//
//            targetTaskIdToWaitingStateMigrationSemphore.put(targetTaskIndex, new Semaphore(0));
//
//            targetTaskIdToWaitingStateMigrationSemphore.get(targetTaskIndex).acquire();
//
//            Slave.getInstance().logOnMaster(String.format("Shard reassignment of shard %d from %d to %d is ready!", shardId, sourceTaskId, targetTaskIndex));
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }

    private void createThriftThread(final ResourceCentricControllerBolt bolt) {
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
                }

            }
        }).start();
    }

    @Override
    public void shardReassignment(int sourceTaskIndex, int targetTaskIndex, int shardId) throws org.apache.thrift.TException {
        Slave.getInstance().logOnMaster(String.format("Shard reassignment of shard %d: %d --> %d is called!", shardId, sourceTaskIndex, targetTaskIndex));
        long startTime = System.currentTimeMillis();
        try {
            if(sourceTaskIndex >= downstreamTaskIds.size()) {
                Slave.getInstance().logOnMaster("Invalid source task index!");
                return;
            }
            if(targetTaskIndex >= downstreamTaskIds.size()) {
                Slave.getInstance().logOnMaster("Invalid target task index!");
                return;
            }
            if(shardId > Config.NumberOfShard) {
                Slave.getInstance().logOnMaster("Invalid shard index!");
                return;
            }

            if(routingTable.getBucketToRouteMapping().get(shardId)!=sourceTaskIndex) {
                Slave.getInstance().logOnMaster(String.format("Shard %d does not belong to %d.", shardId, sourceTaskIndex));
                return;
            }

            Slave.getInstance().logOnMaster(String.format("Begin to migrate shard %d from %d to %d!", shardId, sourceTaskIndex, targetTaskIndex));

            int sourceTaskId = downstreamTaskIds.get(sourceTaskIndex);

            sourceTaskIdToPendingTupleCleanedSemphore.put(sourceTaskIndex, new Semaphore(0));
            sourceTaskIndexToResumingWaitingSemphore.put(sourceTaskIndex, new Semaphore(0));

            Slave.getInstance().logOnMaster(String.format("Controller: sending pausing"));

            collector.emit(ResourceCentricZipfComputationTopology.UpstreamCommand, new Values("pausing", sourceTaskIndex, targetTaskIndex, shardId));

            sourceTaskIdToPendingTupleCleanedSemphore.get(sourceTaskIndex).acquire();

            targetTaskIdToWaitingStateMigrationSemphore.put(targetTaskIndex, new Semaphore(0));

            targetTaskIdToWaitingStateMigrationSemphore.get(targetTaskIndex).acquire();

            Slave.getInstance().logOnMaster(String.format("Shard reassignment of shard %d from %d to %d is ready!", shardId, sourceTaskId, targetTaskIndex));

            collector.emit(ResourceCentricZipfComputationTopology.UpstreamCommand, new Values("resuming", sourceTaskIndex, targetTaskIndex, shardId));
            sourceTaskIndexToResumingWaitingSemphore.get(sourceTaskIndex).acquire();

            routingTable.reassignBucketToRoute(shardId, targetTaskIndex);

            Slave.getInstance().logOnMaster(String.format("Shard reassignment is completed! (%d ms)", System.currentTimeMillis() - startTime));

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void scalingIn() throws TException {

        SmartTimer.getInstance().start("Scaling In", "Algorithm");
        TwoTireRouting twoTireRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);

        int targetSubtaskId = twoTireRouting.getNumberOfRoutes() -1;
        int numberOfSubtasks = twoTireRouting.getNumberOfRoutes();

        Histograms histograms = new Histograms();
        for(int taskId: taskToHistogram.keySet()) {
            histograms.merge(taskToHistogram.get(taskId));
        }

        Map<Integer, Integer> shardToRoutingMapping = twoTireRouting.getBucketToRouteMapping();

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

        Set<ShardWorkload> shardsForTargetSubtask = subtaskToShards.get(targetSubtaskId);
        List<ShardWorkload> sortedShards = new ArrayList<>(shardsForTargetSubtask);
        Collections.sort(sortedShards, ShardWorkload.createReverseComparator());

        ShardReassignmentPlan plan = new ShardReassignmentPlan();

        Comparator<SubTaskWorkload> subTaskComparator = SubTaskWorkload.createReverseComparator();
        subTaskWorkloads.remove(subTaskWorkloads.size() - 1);
        for(ShardWorkload shardWorkload: sortedShards) {
            Collections.sort(subTaskWorkloads, subTaskComparator);
            SubTaskWorkload subtaskWorkloadToMoveIn = subTaskWorkloads.get(0);
            plan.addReassignment(0, shardWorkload.shardId, targetSubtaskId, subtaskWorkloadToMoveIn.subtaskId);
            subtaskWorkloadToMoveIn.increaseOrDecraeseWorkload(shardWorkload.workload);
        }

        SmartTimer.getInstance().stop("Scaling In", "Algorithm");
        SmartTimer.getInstance().start("Scaling In", "Reassignments");

        for(ShardReassignment reassignment: plan.getReassignmentList()) {
            Slave.getInstance().logOnMaster("=============== START ===============");
            shardReassignment(reassignment.originalRoute, reassignment.newRoute, reassignment.shardId);
            Slave.getInstance().logOnMaster("=============== END ===============");
        }
        SmartTimer.getInstance().stop("Scaling In", "Reassignments");
        Slave.getInstance().logOnMaster(SmartTimer.getInstance().getTimerString("Scaling In"));
        Slave.getInstance().logOnMaster(String.format("%d shard reassignments conducted!", plan.getReassignmentList().size()));


        routingTable.scalingIn();
    }

    @Override
    public void scalingOut() throws TException {
        SmartTimer.getInstance().start("Scaling Out", "Algorithm");
        Histograms histograms = new Histograms();
        for(int taskId: taskToHistogram.keySet()) {
            histograms.merge(taskToHistogram.get(taskId));
        }

        TwoTireRouting twoTireRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);

        Map<Integer, Integer> shardToRoutingMapping = twoTireRouting.getBucketToRouteMapping();

        int newSubtaskId = routingTable.scalingOut();

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
                        plan.addReassignment(0, shardWorkload.shardId, subTaskWorkload.subtaskId, newSubtaskId);
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

        SmartTimer.getInstance().stop("Scaling Out", "Algorithm");
        SmartTimer.getInstance().start("Scaling Out", "Reassignments");
        for(ShardReassignment reassignment: plan.getReassignmentList()) {
            Slave.getInstance().logOnMaster("=============== START ===============");
            shardReassignment(reassignment.originalRoute, reassignment.newRoute, reassignment.shardId);
            Slave.getInstance().logOnMaster("=============== END ===============");
        }
        SmartTimer.getInstance().stop("Scaling Out", "Reassignments");
        Slave.getInstance().logOnMaster(SmartTimer.getInstance().getTimerString("Scaling Out"));
        Slave.getInstance().logOnMaster(String.format("Scaled out with %d reassignments", plan.getReassignmentList().size()));

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

//                ShardReassignmentPlan plan = getCompleteShardToTaskMapping(executorId, histograms, numberOfRoutes, balancedHashRouting.getBucketToRouteMapping());
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
        return routingTable.toString();
    }

    Double getRate() {
        double rate = 0;
        String str = "";
        for(double r: taskToRate.values()) {
            rate += r;
            str += r + "\n";
        }
//        Slave.getInstance().logOnMaster(str);
        return rate;
    }

    Long getLatency() {
        long sumedLatnecy = 0;
        for(long l: taskToLatency.values()) {
            sumedLatnecy +=l;
        }
        if(taskToLatency.size() > 0) {
            return sumedLatnecy / taskToLatency.size();
        }
        return 1L;
    }

    public int getDesirableParallelism() {
        final double overProvisioningFactor = 0.5;

        double inputRate = getRate();

        Long averageLatency = getLatency();
        double performanceFactor = 1;
        try {
            TwoTireRouting twoTireRouting = (TwoTireRouting) RoutingTableUtils.getBalancecHashRouting(routingTable);
            if(twoTireRouting == null){
                return 1;
            }
            Histograms histograms = twoTireRouting.getBucketsDistribution();
            performanceFactor = ElasticScheduler.getPerformanceFactor(histograms, twoTireRouting);
        } catch (Exception e) {
            e.printStackTrace();
        }
        double processingRatePerProcessor = 1 / (averageLatency / 1000000000.0);
        int desirableParallelism = (int)Math.ceil(inputRate / (processingRatePerProcessor * performanceFactor) + overProvisioningFactor);
//        Slave.getInstance().sendMessageToMaster("Task " + _taskId + ": input rate=" + String.format("%.5f ",inputRate) + "rate per task=" +String.format("%.5f", processingRatePerProcessor) + " latency: "+ String.format("%.5f ms", averageLatency/1000000.0));
//        Slave.getInstance().sendMessageToMaster(String.format("Task %d: input rate=%.5f rate per task=%.5f latency: %.5f ms performance factor=%.2f", _taskId, inputRate, processingRatePerProcessor, averageLatency/1000000.0, performanceFactor));
        return desirableParallelism;
    }

    private void createAutomaticScalingThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        Thread.sleep(1000);
                        int currentParallelism = routingTable.getNumberOfRoutes();
                        int desirableParallleism = getDesirableParallelism();

                        if(currentParallelism < desirableParallleism) {
                            scalingOut();
                        } else if (currentParallelism > desirableParallleism) {
                            if(getSkewness()) {
                                loadBalancing();
                            } else
                                scalingIn();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

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

    private void createSeedUpdatingThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        Thread.sleep(15000);
                        collector.emit(ResourceCentricZipfComputationTopology.SeedUpdateStream, new Values(new Random().nextInt()));
                        Slave.getInstance().logOnMaster("New seed is generated!");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
