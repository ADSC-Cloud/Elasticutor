package backtype.storm.elasticity.scheduler.algorithm;

import backtype.storm.elasticity.scheduler.ElasticExecutorInfo;
import backtype.storm.elasticity.scheduler.algorithm.actoin.ScaingOutAction;
import backtype.storm.elasticity.scheduler.algorithm.actoin.ScalingInAction;
import backtype.storm.elasticity.scheduler.algorithm.actoin.SchedulingAction;
import backtype.storm.elasticity.scheduler.algorithm.actoin.TaskMigrationAction;

import java.util.*;
import java.util.function.Predicate;

/**
 * Created by robert on 16-8-15.
 */
public class LocalityAndMigrationCostAwareScheduling {
    public List<SchedulingAction> schedule(Collection<ElasticExecutorInfo> executorInfos, List<String> freeCPUCores, double dataIntensivenessThreshold) {

        // create a virtual elastic executor with all the free CPU cores.
        final int virtualExecutorTaskId = -100;
        ElasticExecutorInfo idleElasticExecutor = new ElasticExecutorInfo(virtualExecutorTaskId, "no_node");
        idleElasticExecutor.updateDesirableParallelism(0);
        idleElasticExecutor.scalingIn();
        executorInfos.add(idleElasticExecutor);
        if(freeCPUCores.size() != 0) {
            for(String ip: freeCPUCores) {
                idleElasticExecutor.scalingOut(ip);
            }
        }

        List<ElasticExecutorInfo> sortedExecutors = new ArrayList<>(executorInfos);
        Collections.sort(sortedExecutors, ElasticExecutorInfo.createDataIntensivenessReverseComparator());

        List<ElasticExecutorInfo> overProvisionedExecutors = new ArrayList<>();
        for(ElasticExecutorInfo info: sortedExecutors) {
            if(info.getCurrentParallelism() > info.getDesirableParallelism()) {
//                idleElasticExecutor.scalingOut(info.scalingIn());
                overProvisionedExecutors.add(info);
            }
        }

        List<ElasticExecutorInfo> nonDataIntensiveElasticExecutors = new ArrayList<>();
        for(ElasticExecutorInfo info: sortedExecutors) {
            if(info.getDataIntensiveness() < dataIntensivenessThreshold) {
                nonDataIntensiveElasticExecutors.add(info);
            }
        }


        List<SchedulingAction> actions = new ArrayList<>();

        for(ElasticExecutorInfo executor: sortedExecutors) {

            if(executor.getDataIntensiveness() >= dataIntensivenessThreshold) {
                // data-intensive executor only accepts CPU cores on the host node.
                final String hostIp = executor.getHostIp();
                while(executor.getDesirableParallelism() > executor.getCurrentParallelism()) {
                    double overhead = Double.MAX_VALUE;
                    ScalingInAction scalingInAction = null;
                    ElasticExecutorInfo targetExecutor = null;
                    for(ElasticExecutorInfo overProvisionedOne: overProvisionedExecutors) {
                        if(overProvisionedOne.getAllocatedCores().contains(hostIp)) {
                            final double overheadInEvaluation = scalingInOverhead(overProvisionedOne.getAllocatedCores(), hostIp, overProvisionedOne.getStateSize());
                            if(overhead > overheadInEvaluation) {
                                if(overProvisionedOne.getAllocatedCores().get(overProvisionedOne.getAllocatedCores().size() - 1).equals(hostIp))
                                    scalingInAction = new ScalingInAction(overProvisionedOne.getTaskId(), overProvisionedOne.getRouteIdForACore(hostIp));
                                else
                                    scalingInAction = new ScalingInAction(overProvisionedOne.getTaskId(), overProvisionedOne.getRouteIdForACore(hostIp), false);
                                overhead = overheadInEvaluation;
                                targetExecutor = overProvisionedOne;
                            }
                        }
                    }
                    if(scalingInAction != null) {
                        if(scalingInAction.direct) {
                            actions.add(scalingInAction);
                        } else {
                            // core swap between the last route ant the route on the target CPU core, and then scale in.
                            String ipForLastRoute = targetExecutor.getAllocatedCores().get(targetExecutor.getAllocatedCores().size() - 1);
                            final int taskid = targetExecutor.getTaskId();
                            int routeAllocatedOnDesirableCore = targetExecutor.getRouteIdForACore(hostIp);
                            int lastRoute = targetExecutor.getAllocatedCores().size() - 1;
                            actions.add(new TaskMigrationAction(taskid, routeAllocatedOnDesirableCore, ipForLastRoute));
                            actions.add(new TaskMigrationAction(taskid, lastRoute, hostIp));
                            scalingInAction.direct = true;
                            scalingInAction.route = lastRoute;
                            actions.add(scalingInAction);
                            targetExecutor.getAllocatedCores().set(lastRoute, hostIp);
                            targetExecutor.getAllocatedCores().set(routeAllocatedOnDesirableCore, ipForLastRoute);
                        }
                        targetExecutor.scalingIn();
                        if(targetExecutor.getCurrentParallelism() == targetExecutor.getDesirableParallelism()) {
                            overProvisionedExecutors.remove(targetExecutor);
                        }

                        actions.add(new ScaingOutAction(executor.getTaskId(),hostIp));
                        executor.scalingOut(hostIp);


                    } else {
                        System.out.println("Scheduling algorithm fails!");
                        return null;
                    }

                }
            } else {
                while(executor.getCurrentParallelism() < executor.getDesirableParallelism()) {
                    double overhead = Double.MAX_VALUE;
                    ScalingInAction scalingInAction = null;
                    ScaingOutAction scalingOutAction = null;
                    String targetCore = null;
                    ElasticExecutorInfo scalingInExecutor = null;
                    for(ElasticExecutorInfo overProvisionedOne: overProvisionedExecutors) {
                        final Set<String> setOfAvailableCores = new HashSet<>(overProvisionedOne.getAllocatedCores());
                        for(String core: setOfAvailableCores) {
                            final double scalingInOverhead = scalingInOverhead(overProvisionedOne.getAllocatedCores(), core, overProvisionedOne.getStateSize());
                            final double scalingOutOverhead = scalingOutOverhead(executor.getAllocatedCores(), core, executor.getStateSize());
                            if(scalingInOverhead + scalingOutOverhead < overhead) {
                                // this is the best core swap we found so far.
                                overhead = scalingInOverhead + scalingOutOverhead;
                                if(overProvisionedOne.getAllocatedCores().get(overProvisionedOne.getAllocatedCores().size() - 1).equals(core)) {
                                    scalingInAction = new ScalingInAction(overProvisionedOne.getTaskId(), overProvisionedOne.getRouteIdForACore(core));
                                } else {
                                    scalingInAction = new ScalingInAction(overProvisionedOne.getTaskId(), overProvisionedOne.getRouteIdForACore(core), false);
                                }
                                scalingOutAction = new ScaingOutAction(executor.getTaskId(), core);
                                targetCore = core;
                                scalingInExecutor = overProvisionedOne;

                            }
                        }
                    }
                    if(scalingInAction != null) {
                        if(scalingInAction.direct) {
                            actions.add(scalingInAction);
                        } else {
                            final String ipForLastRoute = scalingInExecutor.getAllocatedCores().get(scalingInExecutor.getAllocatedCores().size() - 1);
                            final int scalingInExecutorTaskID = scalingInExecutor.getTaskId();
                            final int routeAllocatedOnTargetCore = scalingInExecutor.getRouteIdForACore(targetCore);
                            final int lastRouteOfScalingInExecutor = scalingInExecutor.getAllocatedCores().size() - 1;
                            actions.add(new TaskMigrationAction(scalingInExecutorTaskID, routeAllocatedOnTargetCore, ipForLastRoute));
                            actions.add(new TaskMigrationAction(scalingInExecutorTaskID, lastRouteOfScalingInExecutor, targetCore));
                            scalingInAction.direct = true;
                            scalingInAction.route = lastRouteOfScalingInExecutor;
                            actions.add(scalingInAction);
                            scalingInExecutor.getAllocatedCores().set(lastRouteOfScalingInExecutor, targetCore);
                            scalingInExecutor.getAllocatedCores().set(routeAllocatedOnTargetCore, ipForLastRoute);
                        }
                        scalingInExecutor.scalingIn();
                        if(scalingInExecutor.getCurrentParallelism() == scalingInExecutor.getDesirableParallelism()) {
                            overProvisionedExecutors.remove(scalingInExecutor);
                        }

                        actions.add(scalingOutAction);
                        executor.scalingOut(targetCore);
                    } else {
                        System.out.println("Scheduling algorithm fails!");
                        return null;
                    }

                }
            }

        }

        // clear up all the operations on the virtual elastic executor before returning the result.
//        for(SchedulingAction action: actions) {
//            if(action.getTaskID() == virtualExecutorTaskId)
//                actions.remove(action);
//        }

        actions.removeIf(new Predicate<SchedulingAction>() {
            public boolean test(SchedulingAction schedulingAction) {
                return schedulingAction.getTaskID() == virtualExecutorTaskId;
            }
        });

        return actions;
    }



    double scalingInOverhead(List<String> allocatedCores, String targetCore, long stateSize) {
        if(stateSize == 0)
            return 0.0;
        List<String> colocatedCores = new ArrayList<>();
        for(String core: allocatedCores) {
            if(core.equals(targetCore)) {
                colocatedCores.add(core);
            }
        }
        final double fractionOfStateMigrationGoesThroughNetwork = (1.0 - (colocatedCores.size() - 1.0) / (allocatedCores.size() -1.0));
        final double stateMigratedSize = stateSize / (double) allocatedCores.size();
        if(allocatedCores.get(allocatedCores.size() - 1).equals(targetCore)) {
            return stateMigratedSize * fractionOfStateMigrationGoesThroughNetwork;
        } else
            return stateMigratedSize * fractionOfStateMigrationGoesThroughNetwork + stateSize / (allocatedCores.size() - 1.0) ;
    }

    double scalingOutOverhead(List<String> allocatedCores, String targetCore, long stateSize) {
        List<String> colocatedCores = new ArrayList<>();
        for(String core: allocatedCores) {
            if(core.equals(targetCore)) {
                colocatedCores.add(core);
            }
        }
        final double fractionOfStateMigrationGoesThroughNetwork = (1.0 - colocatedCores.size() / (double)allocatedCores.size());
        final double stateMigratedSize = stateSize / (allocatedCores.size() + 1.0);
        return fractionOfStateMigrationGoesThroughNetwork * stateMigratedSize;
    }

    static public void main(String[] args) {
        check1();
        check2();
        check3();
        check4();
    }

    static void check1() {
        List<ElasticExecutorInfo> executorInfos = new ArrayList<>();
        ElasticExecutorInfo executorInfo1 = new ElasticExecutorInfo(0, "192.168.1.192", 1024, 500.0);
        executorInfo1.updateDesirableParallelism(2);
        executorInfos.add(executorInfo1);
        List<String> freeCPUCores = new ArrayList<>();
        freeCPUCores.add("10.10.10.10");
        freeCPUCores.add("192.168.1.192");
        freeCPUCores.add("20.20.20.20");
        LocalityAndMigrationCostAwareScheduling scheduling = new LocalityAndMigrationCostAwareScheduling();
        List<SchedulingAction> actions = scheduling.schedule(executorInfos, freeCPUCores, 600.0);
        boolean pass = true;
        if(!(actions.get(0) instanceof ScaingOutAction)||!((ScaingOutAction)actions.get(0)).targetIP.equals("192.168.1.192"))
            pass = false;
        if(!(actions.size() == 1))
            pass = false;
        if(pass)
            System.out.println("Check 1 passed!");
        else
            System.out.println("Check 1 failed!");
    }

    static void check2() {
        List<ElasticExecutorInfo> executorInfos = new ArrayList<>();
        ElasticExecutorInfo executorInfo1 = new ElasticExecutorInfo(0, "192.168.1.192", 1024, 500.0);
        executorInfo1.updateDesirableParallelism(2);
        executorInfos.add(executorInfo1);

        ElasticExecutorInfo executorInfo2 = new ElasticExecutorInfo(1, "10.10.10.10", 2048, 400.0);
        executorInfo2.updateDesirableParallelism(1);
        executorInfo2.scalingOut("192.168.1.192");
        executorInfo2.scalingOut("10.10.10.10");

        executorInfos.add(executorInfo2);

        List<String> freeCPUCores = new ArrayList<>();
        freeCPUCores.add("10.10.10.10");
        freeCPUCores.add("10.10.10.12");
        freeCPUCores.add("20.20.20.20");
        LocalityAndMigrationCostAwareScheduling scheduling = new LocalityAndMigrationCostAwareScheduling();
        List<SchedulingAction> actions = scheduling.schedule(executorInfos, freeCPUCores, 300.0);

        String expectedResult = "[Task migration: 1.1 -> 10.10.10.10, Task migration: 1.2 -> 192.168.1.192, Scaling in: 1.2, Scaling out: 0 -> 192.168.1.192]";
        if(actions.toString().equals(expectedResult))
            System.out.println("Check 2 passed!");
        else
            System.out.println("Check 2 failed!");
    }

    static void check3() {
        List<ElasticExecutorInfo> executorInfos = new ArrayList<>();
        ElasticExecutorInfo executorInfo1 = new ElasticExecutorInfo(0, "192.168.1.192", 1024, 200.0);
        executorInfo1.updateDesirableParallelism(2);
        executorInfos.add(executorInfo1);

        ElasticExecutorInfo executorInfo2 = new ElasticExecutorInfo(1, "10.10.10.10", 2048, 100.0);
        executorInfo2.updateDesirableParallelism(1);
        executorInfo2.scalingOut("192.168.1.192");
        executorInfo2.scalingOut("192.168.1.192");
        executorInfo2.scalingOut("192.168.1.192");
        executorInfo2.scalingOut("10.10.10.10");
        executorInfos.add(executorInfo2);

        ElasticExecutorInfo executorInfo3 = new ElasticExecutorInfo(2, "10.10.10.10", 2048, 100.0);
        executorInfo3.updateDesirableParallelism(1);
        executorInfo3.scalingOut("192.168.1.192");
        executorInfo3.scalingOut("192.168.1.192");
        executorInfo3.scalingOut("10.10.10.10");
        executorInfos.add(executorInfo3);

        List<String> freeCPUCores = new ArrayList<>();
        freeCPUCores.add("10.10.10.10");
        freeCPUCores.add("10.10.10.12");
        freeCPUCores.add("20.20.20.20");
        LocalityAndMigrationCostAwareScheduling scheduling = new LocalityAndMigrationCostAwareScheduling();
        List<SchedulingAction> actions = scheduling.schedule(executorInfos, freeCPUCores, 150.0);

        System.out.println(actions);

        String expectedResult = "[Task migration: 1.3 -> 10.10.10.10, Task migration: 1.4 -> 192.168.1.192, Scaling in: 1.4, Scaling out: 0 -> 192.168.1.192]";
        if(actions.toString().equals(expectedResult))
            System.out.println("Check 3 passed!");
        else
            System.out.println("Check 3 failed!");
    }

    static void check4() {
        List<ElasticExecutorInfo> executorInfos = new ArrayList<>();
        ElasticExecutorInfo executorInfo1 = new ElasticExecutorInfo(0, "192.168.1.192", 8024, 200.0);
        executorInfo1.scalingOut("10.10.10.10");
        executorInfo1.scalingOut("10.10.10.10");
        executorInfo1.updateDesirableParallelism(4);
        executorInfos.add(executorInfo1);

        ElasticExecutorInfo executorInfo2 = new ElasticExecutorInfo(1, "10.10.10.10", 2048, 100.0);
        executorInfo2.updateDesirableParallelism(1);
        executorInfo2.scalingOut("192.168.1.192");
        executorInfo2.scalingOut("192.168.1.192");
        executorInfo2.scalingOut("192.168.1.192");
        executorInfo2.scalingOut("10.10.10.10");
        executorInfos.add(executorInfo2);

        ElasticExecutorInfo executorInfo3 = new ElasticExecutorInfo(2, "10.10.10.10", 2048, 100.0);
        executorInfo3.updateDesirableParallelism(1);
        executorInfo3.scalingOut("192.168.1.192");
        executorInfo3.scalingOut("192.168.1.192");
        executorInfo3.scalingOut("10.10.10.10");
        executorInfos.add(executorInfo3);

        List<String> freeCPUCores = new ArrayList<>();
        freeCPUCores.add("10.10.10.12");
        freeCPUCores.add("20.20.20.20");
        LocalityAndMigrationCostAwareScheduling scheduling = new LocalityAndMigrationCostAwareScheduling();
        List<SchedulingAction> actions = scheduling.schedule(executorInfos, freeCPUCores, 250.0);

        System.out.println(actions);

        String expectedResult = "[Scaling in: 1.4, Scaling out: 0 -> 10.10.10.10]";
        if(actions.toString().equals(expectedResult))
            System.out.println("Check 4 passed!");
        else
            System.out.println("Check 4 failed!");
    }
}
