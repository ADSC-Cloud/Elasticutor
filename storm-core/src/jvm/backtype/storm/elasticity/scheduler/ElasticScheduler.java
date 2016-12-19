package backtype.storm.elasticity.scheduler;

import backtype.storm.elasticity.actors.Master;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.common.RouteId;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.message.actormessage.ExecutorScalingInRequestMessage;
import backtype.storm.elasticity.message.actormessage.ExecutorScalingOutRequestMessage;
import backtype.storm.elasticity.resource.ResourceManager;
import backtype.storm.elasticity.routing.TwoTireRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.routing.RoutingTableUtils;
import backtype.storm.elasticity.scheduler.algorithm.LocalityAndMigrationCostAwareScheduling;
import backtype.storm.elasticity.scheduler.algorithm.SchedulerComparisonHelper;
import backtype.storm.elasticity.scheduler.algorithm.actoin.ScalingInAction;
import backtype.storm.elasticity.scheduler.algorithm.actoin.ScalingOutAction;
import backtype.storm.elasticity.scheduler.algorithm.actoin.SchedulingAction;
import backtype.storm.elasticity.scheduler.algorithm.actoin.TaskMigrationAction;
import backtype.storm.elasticity.utils.FirstFitDoubleDecreasing;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.elasticity.utils.PartitioningMinimizedMovement;
import backtype.storm.generated.TaskNotExistException;
import backtype.storm.utils.Utils;
import org.apache.thrift.TException;
import org.eclipse.jetty.util.ArrayQueue;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/11/15.
 */
public class ElasticScheduler {

    Master master;

    ResourceManager resourceManager;

    final Object lock = new Object();

    static private ElasticScheduler instance;

    LinkedBlockingQueue<Object> scalingRequestQueue = new LinkedBlockingQueue<>();

    LinkedBlockingQueue<Integer> pendingTaskLevelLoadBalancingQueue = new LinkedBlockingQueue<>();

    ElasticExecutorStatusManager elasticExecutorStatusManager = new ElasticExecutorStatusManager();

    MetaDataManager metaDataManager;

    public ElasticScheduler(Map conf) {


        Config.overrideFromStormConf(conf);

        master = Master.createActor();
        resourceManager = new ResourceManager();
        instance = this;

        if(Config.EnableWorkerLevelLoadBalancing) {
            enableWorkerLevelLoadBalancing();
        }

        if(Config.EnableSubtaskLevelLoadBalancing) {
            enableSubtaskLevelLoadBalancing();
        }

        if(Config.EnableAutomaticScaling) {
//            enableAutomaticScaling();
            createLocalityAndDataIntensivenessAwareScheduling();
        }


//        createScalingInAndOutTestingThread(17);

        metaDataManager = new MetaDataManager();


    }

    static public ElasticScheduler getInstance() {
        return instance;
    }

    private void enableWorkerLevelLoadBalancing() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        Thread.sleep(Config.WorkerLevelLoadBalancingCycleInSecs * 1000);
                        synchronized (lock) {
                            Set<Integer> taskIds = master._elasticTaskIdToWorkerLogicalName.keySet();
                            for(Integer task: taskIds) {
                                try {
//                                    workerLevelLoadBalancing(task);
                                } catch (Exception e ) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                } catch (InterruptedException e ) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public void registerElasticExecutor(int taskId, String hostIp) {

        if(!ResourceManager.instance().computationResource.allocateProcessOnGivenNode(hostIp)) {
            //TODO: Solve this situation.
        }

        elasticExecutorStatusManager.registerNewElasticExecutor(taskId, hostIp);

    }

    public void unregisterElasticExecutor(int taskid) {
        for(String core: elasticExecutorStatusManager.getAllocatedCoresForATask(taskid)) {
            resourceManager.computationResource.returnProcessor(core);
        }
        elasticExecutorStatusManager.unregisterElasticExecutor(taskid);
    }

//    public ElasticExecutorInfo getElasticExecutorInfo(int taskid) {
//        return elasticExecutorStatusManager.getTaskIdToInfo().get(taskid);
//    }

    public ElasticExecutorStatusManager getElasticExecutorStatusManager() {
        return elasticExecutorStatusManager;
    }

    private void enableSubtaskLevelLoadBalancing() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        Thread.sleep(Config.SubtaskLevelLoadBalancingCycleInMilliSecs);
                        Set<Integer> taskIds = master._elasticTaskIdToWorkerLogicalName.keySet();
                        for(Integer task: taskIds) {
                            try {
                                pendingTaskLevelLoadBalancingQueue.offer(task);
                            } catch (Exception e ) {
                                e.printStackTrace();
                            }
                        }


                    }
                } catch (InterruptedException e ) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        int taskId = pendingTaskLevelLoadBalancingQueue.take();
                        synchronized (lock) {
                            long start = System.currentTimeMillis();
                            optimizeBucketToRoutingMapping(taskId);
                            System.out.println("Load-balancing in " + (System.currentTimeMillis() - start)/1000.0 + " s");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
        }).start();
    }

    private void createLocalityAndDataIntensivenessAwareScheduling() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                LocalityAndMigrationCostAwareScheduling scheduling = new LocalityAndMigrationCostAwareScheduling();
                final int sleepTimeInMilliseconds = Config.ElasticSchedulingCycleInMillisecond;
                final double dataIntensivenessThreshold = 1024 * 1024 * 1024;
                while (true) {
                    try {
                        Utils.sleep(sleepTimeInMilliseconds);
                        synchronized (lock) {
                            Map<Integer, ElasticExecutorInfo> currentElasticExecutorInfos = ElasticScheduler.getInstance().getElasticExecutorStatusManager().getInfoSnapshot();
                            List<ElasticExecutorInfo> elasticExecutorInfos = new ArrayList<>(currentElasticExecutorInfos.values());

                            if(Config.DisableStateSizeInfo) {
                                SchedulerComparisonHelper.disableStateSizeInfo(elasticExecutorInfos);
                            }

                            if(Config.DisableDataIntensivenessInfo) {
                                SchedulerComparisonHelper.disableDataIntensivenessInfo(elasticExecutorInfos);
                            }

                            System.out.println("Snapshot: " + currentElasticExecutorInfos.values());
                            System.out.println(ResourceManager.instance().computationResource.toString());

                            List<SchedulingAction> actions = scheduling.schedule(elasticExecutorInfos, ElasticScheduler.getInstance().resourceManager.computationResource.getFreeCPUCores(), dataIntensivenessThreshold);
                            System.out.println("The following actions will be performed: " + actions);

                            for (SchedulingAction action : actions) {
                                if (action instanceof ScalingOutAction) {
                                    ScalingOutAction scalingOutAction = (ScalingOutAction) action;
                                    Master.getInstance().handleExecutorScalingOutRequest(scalingOutAction.getTaskID(), scalingOutAction.targetIP);
                                } else if (action instanceof ScalingInAction) {
                                    Master.getInstance().handleExecutorScalingInRequest(action.getTaskID());
                                } else if (action instanceof TaskMigrationAction) {
                                    TaskMigrationAction taskMigrationAction = (TaskMigrationAction) action;
                                    final String targetWorkerLogicalName = Master.getInstance().getAWorkerLogicalNameOnAGivenIp(taskMigrationAction.targetIP);
                                    Master.getInstance().resourceAwareMigrateTask(targetWorkerLogicalName, taskMigrationAction.getTaskID(), taskMigrationAction.routeID);
                                }
                                System.out.println(action + " is performed!");
                            }

                            System.out.println("========== DONE =========");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    private void enableAutomaticScaling() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {

                        Object request = scalingRequestQueue.take();
                        System.out.println("Received one request! " + scalingRequestQueue.size() + " pending");
                        if(request instanceof ExecutorScalingInRequestMessage) {
                            System.out.println("Handling Scaling in request!");
                            ExecutorScalingInRequestMessage requestMessage = (ExecutorScalingInRequestMessage)request;
//                            final boolean skewed = isWorkloadSkewed(requestMessage.taskID);
                            final boolean skewed = false;
                            if(skewed) {
                                System.out.println("Scaling in request on " + requestMessage.taskID + " is ignored, as skewness is detected!");
                                pendingTaskLevelLoadBalancingQueue.add(requestMessage.taskID);
                            } else
                                synchronized (lock) {
                                    System.out.println("Scaling in command will be called!");
                                    long start = System.currentTimeMillis();
                                    master.handleExecutorScalingInRequest(requestMessage.taskID);
                                    System.out.println("Scaling in " + (System.currentTimeMillis() - start)/1000.0 + " s");
                                }
                        }
                        if(request instanceof ExecutorScalingOutRequestMessage) {
                            ExecutorScalingOutRequestMessage requestMessage = (ExecutorScalingOutRequestMessage)request;
                            System.out.println("Scaling out " + requestMessage.taskId);
                            synchronized (lock) {
                                long start = System.currentTimeMillis();
                                master.handleExecutorScalingOutRequest(requestMessage.taskId);
                                System.out.println("Scaling out " + (System.currentTimeMillis() - start)/1000.0 + " s");
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public String optimizeBucketToRoutingMapping(int taskId) throws TaskNotExistException, RoutingTypeNotSupportedException, TException {
        return optimizeBucketToRoutingMapping(taskId, Config.taskLevelLoadBalancingThreshold);
    }

    public void addScalingRequest(Object request) {
        scalingRequestQueue.offer(request);
    }


    public static double getSkewnessFactor(Histograms histograms, TwoTireRouting twoTireRouting) throws TaskNotExistException, RoutingTypeNotSupportedException {
//        RoutingTable routingTable = master.getRoutingTable(executorId);
//        TwoTireRouting twoTireRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);
//        if(twoTireRouting == null) {
//            throw new RoutingTypeNotSupportedException("Only support TwoTireRouting family routing table!");
//        }
//            System.out.println("routing Table: " + twoTireRouting.toString());



        // 2. get Distribution;
//        Histograms histograms = master.getBucketDistribution(executorId);
//            System.out.println("Histograms: " + histograms.toString());

        // 3. evaluate the skewness
        Map<Integer, Integer> shardToRouteMapping = twoTireRouting.getBucketToRouteMapping();
        final int numberOfRoutes = twoTireRouting.getNumberOfRoutes();
        long[] routeLoads = new long[numberOfRoutes];

        for(Integer shard: shardToRouteMapping.keySet()) {
            //            System.out.println("\n\n shard:" + shard);
            //            System.out.println("numberOfRoutes: " + numberOfRoutes);
            //            System.out.println("shardToRouteMapping.get(shard): "+ shardToRouteMapping.get(shard) + "\n");
            //            System.out.println("routeLoads[shardToRouteMapping.get(shard)]" + routeLoads[shardToRouteMapping.get(shard)] + "\n");
            //            System.out.println("histograms.histogramsToArrayList().get(shard)" + histograms.histogramsToArrayList().get(shard) + "\n");
            routeLoads[shardToRouteMapping.get(shard)] += histograms.histogramsToArrayList().get(shard);
        }

        long loadSum = 0;
        long loadMin = Long.MAX_VALUE;
        long loadMax = Long.MIN_VALUE;
        for(Long i: routeLoads) {
            loadSum += i;
        }
        for(Long i: routeLoads) {
            if(loadMin > i){
                loadMin = i;
            }
        }
        for(Long i: routeLoads) {
            if(loadMax < i) {
                loadMax = i;
            }
        }

        double averageLoad = loadSum / (double)numberOfRoutes;
//            boolean skewness = (loadMax - averageLoad)/averageLoad > 0.8;

        return (loadMax - loadMin) / (double)loadMax;
    }

    public static double getPerformanceFactor(TwoTireRouting twoTireRouting) throws TaskNotExistException, RoutingTypeNotSupportedException {
        return getPerformanceFactor(twoTireRouting.getBucketsDistribution(), twoTireRouting);
    }

        /**
         * Given the statics collected from the routing table, this function predicts the ratio of actual performance
         * to the optimal performance. Performance ratio is from 0 to 1. The higher ratio is, the better load balance
         * is achieved.
         * @param histograms statics on the bucket level
         * @param twoTireRouting routing table, containing the shard to task mapping.
         * @return the performance factor
         * @throws TaskNotExistException
         * @throws RoutingTypeNotSupportedException
         */
    public static double getPerformanceFactor(Histograms histograms, TwoTireRouting twoTireRouting) throws TaskNotExistException, RoutingTypeNotSupportedException {
//        RoutingTable routingTable = master.getRoutingTable(executorId);
//        TwoTireRouting twoTireRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);
//        if(twoTireRouting == null) {
//            throw new RoutingTypeNotSupportedException("Only support TwoTireRouting family routing table!");
//        }
//            System.out.println("routing Table: " + twoTireRouting.toString());



        // 2. get Distribution;
//        Histograms histograms = master.getBucketDistribution(executorId);
//            System.out.println("Histograms: " + histograms.toString());

        // 3. evaluate the skewness
        Map<Integer, Integer> shardToRouteMapping = twoTireRouting.getBucketToRouteMapping();
        final int numberOfRoutes = twoTireRouting.getNumberOfRoutes();
        long[] routeLoads = new long[numberOfRoutes];

        for(Integer shard: shardToRouteMapping.keySet()) {
            //            System.out.println("\n\n shard:" + shard);
            //            System.out.println("numberOfRoutes: " + numberOfRoutes);
            //            System.out.println("shardToRouteMapping.get(shard): "+ shardToRouteMapping.get(shard) + "\n");
            //            System.out.println("routeLoads[shardToRouteMapping.get(shard)]" + routeLoads[shardToRouteMapping.get(shard)] + "\n");
            //            System.out.println("histograms.histogramsToArrayList().get(shard)" + histograms.histogramsToArrayList().get(shard) + "\n");
            routeLoads[shardToRouteMapping.get(shard)] += histograms.histogramsToArrayList().get(shard);
        }

        return getPerformanceFactor(routeLoads);
    }

    static public double getPerformanceFactor(long[] routeLoads) {
        long loadSum = 0;
        long loadMin = Long.MAX_VALUE;
        long loadMax = Long.MIN_VALUE;
        for(Long i: routeLoads) {
            loadSum += i;
        }


        double ratio = 1;

        double standardLoad = loadSum / (double)routeLoads.length;

        for(Long i: routeLoads) {
            if(i != 0)
                ratio = Math.min(ratio, standardLoad / i);
        }

        return ratio;
    }

    static public long[] getRouteLoads(TwoTireRouting twoTireRouting) {
        Histograms histograms = twoTireRouting.getBucketsDistribution();
        Map<Integer, Integer> shardToRouteMapping = twoTireRouting.getBucketToRouteMapping();
        final int numberOfRoutes = twoTireRouting.getNumberOfRoutes();
        long[] routeLoads = new long[numberOfRoutes];
        for(Integer shard: shardToRouteMapping.keySet()) {
            routeLoads[shardToRouteMapping.get(shard)] += histograms.histogramsToArrayList().get(shard);
        }
        return routeLoads;
    }

    static public long getMaxShardLoad(TwoTireRouting twoTireRouting) {
        long ret = Long.MIN_VALUE;
        for(long i: twoTireRouting.getBucketsDistribution().histogramsToArrayList()) {
            ret = Math.max(ret, i);
        }
        return ret;
    }

    public boolean isWorkloadSkewed(int taskId) {
        try {
            RoutingTable routingTable = master.getRoutingTable(taskId);
            TwoTireRouting twoTireRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);
            if(twoTireRouting == null) {
                throw new RoutingTypeNotSupportedException("Only support TwoTireRouting family routing table!");
            }

            // 2. get Distribution;
            Histograms histograms = master.getBucketDistribution(taskId);

            double workloadFactor = getSkewnessFactor(histograms, twoTireRouting);
            return workloadFactor >= Config.taskLevelLoadBalancingThreshold;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public String optimizeBucketToRoutingMapping(int taskId, double threshold) throws TaskNotExistException, RoutingTypeNotSupportedException, TException {

        synchronized (lock) {
//            System.out.println("Task-level Load Balance model is called on Executor " + executorId);
            // 1. get routingTable
            RoutingTable routingTable = master.getRoutingTable(taskId);
            TwoTireRouting twoTireRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);
            if(twoTireRouting == null) {
//                throw new RoutingTypeNotSupportedException("Only support TwoTireRouting family routing table!");
                System.out.println("Only support TwoTireRouting family routing table!");
                return "Only support TwoTireRouting family routing table!";
            }

            // 2. get Distribution;
            Histograms histograms = master.getBucketDistribution(taskId);

//            System.out.println(histograms);

            double workloadFactor = getSkewnessFactor(histograms, twoTireRouting);
            double performanceFactor = getPerformanceFactor(histograms, twoTireRouting);
            boolean skewness = workloadFactor >= threshold;

//            System.out.println("Workload distribution:\n");
//            for(int i = 0; i < routeLoads.length; i++ ){
//                System.out.println(i + ": " + routeLoads[i]);
//            }
//            System.out.print("Workload factor: " + workloadFactor + "  Performance factor: " + performanceFactor);
//            System.out.println("  Threshold: " + threshold);

            if(workloadFactor > 0.99) {
//                System.out.println(histograms);
//                System.out.println(master.queryDistribution(executorId));
            }

            if(skewness) {

//                ShardReassignmentPlan plan = getCompleteShardToTaskMapping(executorId, histograms, numberOfRoutes, twoTireRouting.getBucketToRouteMapping());
                try {
                    ShardReassignmentPlan plan = getMinimizedShardToTaskReassignment(taskId, routingTable.getNumberOfRoutes(), twoTireRouting.getBucketToRouteMapping(), histograms);

//                    System.out.println(plan);

                    if(!plan.getReassignmentList().isEmpty()) {
                        applyShardToRouteReassignment(plan);
                    } else {
                        System.out.println("Shard Assignment is not modified after optimization.");
                    }
                    System.out.println(plan.getReassignmentList().size() + " shard reassignments has be performed for load balancing! ");
                    return plan.toString();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Mapping: " + twoTireRouting.getBucketToRouteMapping());
                    System.out.println("# of Routes: " + routingTable.getNumberOfRoutes());

                    Slave.getInstance().sendMessageToMaster(e.getMessage());
                    Slave.getInstance().sendMessageToMaster("Mapping: " + twoTireRouting.getBucketToRouteMapping());
                    Slave.getInstance().sendMessageToMaster("# of Routes: " + routingTable.getNumberOfRoutes());
                    return "ERROR!!!";
                }
            } else {
//                System.out.println("No shard reassignment will be performed!");
                return "The workload is not skewed!";
            }
        }

    }

    public static ShardReassignmentPlan getMinimizedShardToTaskReassignment(int taskId, int numberOfRoutes, Map<Integer, Integer> oldMapping,  Histograms histograms) {
        ShardReassignmentPlan plan = new ShardReassignmentPlan();

        PartitioningMinimizedMovement solver = new PartitioningMinimizedMovement(numberOfRoutes, oldMapping, histograms.histograms );

        Map<Integer, Integer> optimziedShardToTaskMapping = solver.getSolution();

        for(int shardID: optimziedShardToTaskMapping.keySet()){
            final int newTask = optimziedShardToTaskMapping.get(shardID);
            final int oldTask = oldMapping.get(shardID);
            if(newTask!=oldTask) {
                plan.addReassignment(taskId,shardID,oldTask,newTask);
//                System.out.println("Move " + shardID + " from " + oldTask + " to " + newTask + "\n");
            }
        }

        return plan;
    }

    ShardReassignmentPlan getCompleteShardToTaskMapping(int taskId, Histograms histograms, int numberOfRoutes, Map<Integer, Integer> oldMapping ){


        FirstFitDoubleDecreasing binPackingSolver = new FirstFitDoubleDecreasing(histograms.histogramsToArrayList(), numberOfRoutes);
        if(binPackingSolver.getResult() != numberOfRoutes) {
            System.out.println("Fail to solve the bin packing problem!");
            return null;
        }
        System.out.println(binPackingSolver.toString());


        Map<Integer, Integer> newMapping = binPackingSolver.getBucketToPartitionMap();
        ShardReassignmentPlan plan = new ShardReassignmentPlan();

        for(Integer bucket: oldMapping.keySet()) {
            if(!oldMapping.get(bucket).equals(newMapping.get(bucket)) ) {
                int oldRoute = oldMapping.get(bucket);
                int newRoute = newMapping.get(bucket);
                plan.addReassignment(taskId, bucket, oldRoute, newRoute);
//                System.out.println("Move " + bucket + " from " + oldRoute + " to " + newRoute + "\n");
            }
        }


        return plan;
    }

    void applyShardToRouteReassignment(ShardReassignmentPlan plan) throws TException{
        int totalMovements = plan.getReassignmentList().size();
        int i = 0;
        for(ShardReassignment reassignment: plan.getReassignmentList()) {
//            System.out.println("\n===================START========================");
//            System.out.println("Begin to conduct the " + i++ + "th movements, " + totalMovements + " in total!");
            String from = master.getRouteHosterName(reassignment.taskId, reassignment.originalRoute);
            String to = master.getRouteHosterName(reassignment.taskId, reassignment.newRoute);
//            System.out.println("Movement: " + reassignment.toString());
//            System.out.println("From " + from + " to " + to);
            master.reassignBucketToRoute(reassignment.taskId, reassignment.shardId, reassignment.originalRoute, reassignment.newRoute);
//            System.out.println("=====================END========================\n");
        }
    }

    void applySubtaskReassignmentPlan(SubtaskReassignmentPlan plan) throws TException {
        int totalMovements = plan.getSubTaskReassignments().size();
        int i = 0;
        for(SubtaskReassignment reassignment: plan.getSubTaskReassignments()) {
//            System.out.println("\n===================START========================");
//            System.out.println("Begin to conduct the " + i++ + "th movements, " + totalMovements + " in total!");
//            System.out.println("Move " + reassignment.executorId + "." + reassignment.routeId + " from " + reassignment.originalHost + " to " + reassignment.targetHost);
            master.migrateTasks(reassignment.targetHost, reassignment.taskId, reassignment.routeId);
//            System.out.println("=====================END========================\n");
        }
        System.out.println(totalMovements + " subtask movements are completed!");
    }

    public String naiveWorkerLevelLoadBalancing(int taskId) throws TException {
        SubtaskReassignmentPlan plan = new SubtaskReassignmentPlan();

        synchronized (lock) {
            ArrayList<String> workers = new ArrayList<>();
            workers.addAll(resourceManager.systemCPULoad.keySet());
            int workerIndex = 0;
            Map<String, String> taskIdRouteToWorkers = master._taskidRouteToWorker;
            for(String xdy: taskIdRouteToWorkers.keySet()) {
                if(!taskIdRouteToWorkers.get(xdy).equals(workers.get(workerIndex))) {
                    RouteId routeId = new RouteId(xdy);
                    plan.addSubtaskReassignment(taskIdRouteToWorkers.get(xdy), Master.getInstance().getIpForWorkerLogicalName(workers.get(workerIndex)), routeId.TaskId, routeId.Route);
                }
                workerIndex = (workerIndex + 1) % workers.size();
            }
            applySubtaskReassignmentPlan(plan);
        }

        return plan.toString();
    }

    public String workerLevelLoadBalancing(int taskId) throws TException {
        if(!master._elasticTaskIdToWorkerLogicalName.containsKey(taskId))
            throw new TaskNotExistException("Task " + taskId + " does not exists!");

        Map<String, Double> workerLoad = resourceManager.getWorkerCPULoadCopy();
        Map<String, String> taskIdRouteToWorkers = master._taskidRouteToWorker;

        SubtaskReassignmentPlan totalPlan = new SubtaskReassignmentPlan();

        synchronized (lock) {
            for(String worker: workerLoad.keySet()) {
                if(workerLoad.get(worker) >= Config.WorkloadHighWaterMark) {
                    SubtaskReassignmentPlan localPlan = releaseLoadOnWorker(taskId, Master.getInstance().getIpForWorkerLogicalName(worker), workerLoad, taskIdRouteToWorkers);
                    totalPlan.concat(localPlan);
    //                totalPlan.con
                }
            }

            System.out.println("Load balancing plan: " + totalPlan);
            applySubtaskReassignmentPlan(totalPlan);


            Map<String, ArrayList<String>> workerToSubtaskRoute = new HashMap<>();
            for(String xdy: taskIdRouteToWorkers.keySet()) {
                String worker = taskIdRouteToWorkers.get(xdy);
                if(!workerToSubtaskRoute.containsKey(worker)) {
                    workerToSubtaskRoute.put(worker, new ArrayList<String>());
                }
                workerToSubtaskRoute.get(worker).add(xdy);
            }
            System.out.println("Existing assignment:");
            for(String worker: workerToSubtaskRoute.keySet()) {
                String str = "";
                str += worker + ": ";
                for(String xdy: workerToSubtaskRoute.get(worker)) {
                    str += xdy + " ";
                }
                System.out.println(str);
            }
        }
        return totalPlan.toString();
    }

    SubtaskReassignmentPlan releaseLoadOnWorker(int taskId, String worker, Map<String, Double> workerLoad, Map<String, String> taskIdRouteToWorkers) {

        SubtaskReassignmentPlan plan = new SubtaskReassignmentPlan();

        String hostWorker = master._elasticTaskIdToWorkerLogicalName.get(taskId);

        Map<String, Queue<String>> workerToTaskRoutes = new HashMap<>();

        for(String taskIdRoute: taskIdRouteToWorkers.keySet()) {
            String w = taskIdRouteToWorkers.get(taskIdRoute);
            if(!workerToTaskRoutes.containsKey(w)) {
                workerToTaskRoutes.put(w, new ArrayQueue<String>());
            }
            workerToTaskRoutes.get(w).add(taskIdRoute);
        }

        int movements = (int) Math.ceil(workerLoad.get(worker) - Config.WorkloadHighWaterMark);


        //first try to move subtask to the original host
        if(!hostWorker.equals(worker) && workerLoad.get(hostWorker)<=Config.WorkloadLowWaterMark) {
            while(workerLoad.get(worker) > Config.WorkloadHighWaterMark // target worker is overloaded
                    && workerLoad.get(hostWorker) + 1 <= Config.WorkloadHighWaterMark // hostWorker is idle
                    && workerToTaskRoutes.get(worker).size() > 0 //there are subtasks on the target worker
                    ) {
                String taskIdRoute = workerToTaskRoutes.get(worker).poll();
                int tid = Integer.parseInt(taskIdRoute.split(".")[0]);
                int route = Integer.parseInt(taskIdRoute.split(".")[1]);
                assert(tid == taskId);
                plan.addSubtaskReassignment(worker, hostWorker, tid, route);
                workerLoad.put(worker, workerLoad.get(worker) - 1);
                workerLoad.put(hostWorker, workerLoad.get(hostWorker) + 1);
            }

        }

        for(String w: workerLoad.keySet()) {
            while(workerLoad.get(worker) > Config.WorkloadHighWaterMark
                    && workerLoad.get(w) + 1 <= Config.WorkloadHighWaterMark
                    && workerToTaskRoutes.get(worker).size() > 0
                    ) {
                String taskIdRoute = workerToTaskRoutes.get(worker).poll();
                System.out.println("taskIdRoute: " + taskIdRoute);
                int tid = Integer.parseInt(taskIdRoute.split("\\.")[0]);
                int route = Integer.parseInt(taskIdRoute.split("\\.")[1]);
                assert(tid == taskId);
                plan.addSubtaskReassignment(worker, w, tid, route);
                workerLoad.put(worker, workerLoad.get(worker) - 1);
                workerLoad.put(w, workerLoad.get(w) + 1);
            }
        }

        return plan;
    }


    private void createScalingInAndOutTestingThread(final int task) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                final int maxParallelism = 15;
                final int minParallelism = 4;
                boolean scalingOut = true;
                while(true) {
                    Utils.sleep(200);
                    try {
                        System.out.println("Test: try to get the lock!");
                        synchronized (lock) {
                            System.out.println("Test: got the lock!");
                            RoutingTable routingTable = master.getRoutingTable(17);
                            int parallelism = routingTable.getNumberOfRoutes();
                            if(parallelism < minParallelism) {
                                scalingOut = true;
                            } else if (parallelism > maxParallelism){
                                scalingOut = false;
                            }

                            if(scalingOut) {
                                master.handleExecutorScalingOutRequest(task);
                            } else {
                                master.handleExecutorScalingInRequest(task);
                            }
                        }
                    }
                    catch (TaskNotExistException e) {
                        System.out.println(String.format("Task %d does not exist!", task));
//                        e.printStackTrace();;
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
        }).start();
    }

    public MetaDataManager getMetaDataManager() {
        return metaDataManager;
    }

}
