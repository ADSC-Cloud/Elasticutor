package storm.starter;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.routing.BalancedHashRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.utils.surveillance.ThroughputMonitor;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.collections.ArrayStack;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.util.*;

/**
 * Created by acelzj on 03/05/16.
 */
public class ResourceCentricGeneratorBolt implements IRichBolt{

    ZipfDistribution _distribution;
    OutputCollector _collector;
    int _numberOfElements;
    double _exponent;
    Thread _emitThread;
    transient ThroughputMonitor monitor;
    transient BalancedHashRouting routingTable;

    private int numberOfComputingTasks;
    private List<Integer> downStreamTaskIds;
    private List<Long> pendingPruncutationUpdates;

    private int _emit_cycles;
    private int taskId;
    private int taskIndex;
    int _prime;

    final boolean enableMannualACK = false;

    final private int puncutationGenrationFrequency = 400;
    final private int numberOfPendingTuple = 2000;
    private long currentPuncutationLowWaterMarker = 0;
//    private long currentPuncutationLowWaterMarker = 10000000L;
//    private long progressPermission = 200;
    private long progressPermission = Long.MAX_VALUE;

   final int[] primes = {104179, 104183, 104207, 104231, 104233, 104239, 104243, 104281, 104287, 104297,
     104309, 104311, 104323, 104327, 104347, 104369, 104381, 104383, 104393, 104399,
           104417, 104459, 104471, 104473, 104479, 104491, 104513, 104527, 104537, 104543,
           104549, 104551, 104561, 104579, 104593, 104597, 104623, 104639, 104651, 104659,
           104677, 104681, 104683, 104693, 104701, 104707, 104711, 104717, 104723, 104729};

    emitKey _emitKey;

    public class ChangeDistribution implements Runnable {

        @Override
        public void run() {
            while (true) {
                Utils.sleep(15000);
                Random rand = new Random(1);
                System.out.println("distribution has been changed");
                _prime = primes[rand.nextInt(primes.length)];
                Slave.getInstance().logOnMaster("distribution has been changed");
            }
        }
    }

    public ResourceCentricGeneratorBolt(int emit_cycles){
        _emit_cycles = emit_cycles;
        _prime = 41;
    }

    public class emitKey implements Runnable {

        boolean terminating = false;
        boolean terminated = false;

        public void terminate() {
            terminating = true;
            while(!terminated) {
                Utils.sleep(1);
            }
        }

        public void run() {
            try {
                long count = 0;
                while (true) {
                    while (count >= progressPermission && !terminating) {
                        Thread.sleep(1);
                    }

                    if(enableMannualACK)
                        while (count >= currentPuncutationLowWaterMarker + numberOfPendingTuple && !terminating) {
                            Thread.sleep(1);
                        }

                    if (terminating) {
                        terminated = true;
                        terminating = false;
                        break;
                    }
                    Random random = new Random();

                    Thread.sleep(_emit_cycles);
                    int key = _distribution.sample();
                    key = ((key + _prime) * 577) % 13477;

                    int pos = routingTable.route(key);
                    int targetTaskId = downStreamTaskIds.get(pos);

                    if (count % puncutationGenrationFrequency == 0) {
                        _collector.emitDirect(targetTaskId, ResourceCentricZipfComputationTopology.PuncutationEmitStream, new Values(count, taskId));
//                         Slave.getInstance().logOnMaster(String.format("PUNC %d is sent to %d", count, targetTaskId));
                    }


                    _collector.emitDirect(targetTaskId, new Values(String.valueOf(key)));


//                    _collector.emit(new Values(String.valueOf(key)));
                    monitor.rateTracker.notify(1);

                    count++;
                    if (count % 1000 == 0) {
//                    Slave.getInstance().logOnMaster(String.format("Task %d: %d", taskId, count));
                    }
                    if (count % 50 == 0) {
                        _collector.emit(ResourceCentricZipfComputationTopology.CountReportSteram, new Values(taskIndex, count));
                    }
                }
            } catch (InterruptedException ee) {
//                Slave.getInstance().sendMessageToMaster("I was interrupted!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("numberOfTask"));
        declarer.declareStream("statics", new Fields("taskId", "Histogram"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.StateMigrationCommandStream, new Fields("sourceTaskId","targetTaskId", "shardId"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.FeedbackStream, new Fields("command", "arg1"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.CountReportSteram, new Fields("taskid", "count"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.PuncutationEmitStream, new Fields("puncutation", "taskid"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        this.taskId = context.getThisTaskId();

        taskIndex = -1;

        for(int i = 0; i < context.getComponentTasks(ResourceCentricZipfComputationTopology.GeneratorBolt).size(); i++) {
            if(taskId == context.getComponentTasks(ResourceCentricZipfComputationTopology.GeneratorBolt).get(i)) {
                taskIndex = i;
            }
        }

        downStreamTaskIds = context.getComponentTasks(ResourceCentricZipfComputationTopology.ComputationBolt);

        numberOfComputingTasks = downStreamTaskIds.size();

        routingTable = new BalancedHashRouting(numberOfComputingTasks);

        _numberOfElements = 1000;
        _exponent = 1;

        _distribution = new ZipfDistribution(_numberOfElements, _exponent);

        monitor = new ThroughputMonitor(""+context.getThisTaskId());
        _emitKey = new emitKey();
        _emitThread = new Thread(_emitKey);
        _emitThread.start();
        pendingPruncutationUpdates = new ArrayList<>();
//        new Thread(new ChangeDistribution()).start();
    }

    public Map getComponentConfiguration(){ return new HashedMap();}

    public void setNumberOfElements(Tuple tuple) {
//        System.out.println(tuple.getString(0));
        _numberOfElements = Integer.parseInt(tuple.getString(0));
    }

    public void setExponent(Tuple tuple) {
//        System.out.println(tuple.getString(1));
        _exponent = Double.parseDouble(tuple.getString(1));
    }

    public void cleanup() { }

    public void execute(Tuple tuple){
      //  setNumberOfElements(tuple);
      //  setExponent(tuple);
        if(tuple.getSourceStreamId().equals(Utils.DEFAULT_STREAM_ID)) {
            _numberOfElements = Integer.parseInt(tuple.getString(0));
            _exponent = Double.parseDouble(tuple.getString(1));
            int seed = tuple.getInteger(2);
            _distribution = new ZipfDistribution(_numberOfElements, _exponent);
            _prime = primes[seed % (primes.length)];
            Slave.getInstance().logOnMaster(String.format("Prime is changed to %d on task %d, keys = %d, exp = %1.2f", _prime, taskId, _numberOfElements, _exponent ));
        } else if (tuple.getSourceStreamId().equals(ResourceCentricZipfComputationTopology.UpstreamCommand)) {
            String command  = tuple.getString(0);
            if(command.equals("getHistograms")) {
                _collector.emit("statics", new Values(taskId, routingTable.getBucketsDistribution()));
            } else if (command.equals("pausing")) {
//                Slave.getInstance().logOnMaster("Received pausing command on " + taskId);
                int sourceTaskOffset = tuple.getInteger(1);
                int targetTaskOffset = tuple.getInteger(2);
                int shardId = tuple.getInteger(3);
                System.out.println("Begin to terminate emit thread..");
                _emitKey.terminate();
                System.out.println("Terminated!");
//                _emitThread.interrupt();
//                try {
//                    _emitThread.join();
////                    Slave.getInstance().logOnMaster("Sending thread is paused on " + taskId);
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                routingTable.reassignBucketToRoute(shardId, targetTaskOffset);
//                Slave.getInstance().logOnMaster("Routing table is updated on " + taskId);
                _collector.emitDirect(downStreamTaskIds.get(sourceTaskOffset), ResourceCentricZipfComputationTopology.StateMigrationCommandStream, new Values(sourceTaskOffset, targetTaskOffset, shardId));
            } else if (command.equals("resuming")) {
                int sourceTaskIndex = tuple.getInteger(1);
                _emitKey = new emitKey();
                _emitThread = new Thread(_emitKey);
                _emitThread.start();

//                Slave.getInstance().logOnMaster("Routing thread is resumed!");
                _collector.emit(ResourceCentricZipfComputationTopology.FeedbackStream, new Values("resumed", sourceTaskIndex));
            }
        } else if (tuple.getSourceStreamId().equals(ResourceCentricZipfComputationTopology.SeedUpdateStream)) {
            _prime = primes[Math.abs(tuple.getInteger(0) % primes.length)];
            Slave.getInstance().logOnMaster(String.format("Prime is changed to %d on task %d", _prime, taskId ));
        } else if (tuple.getSourceStreamId().equals(ResourceCentricZipfComputationTopology.CountPermissionStream)) {
            progressPermission = Math.max(progressPermission, tuple.getLong(0));
//            Slave.getInstance().logOnMaster(String.format("Progress on task %d is updated to %d", taskId, progressPermission));
        } else if (tuple.getSourceStreamId().equals(ResourceCentricZipfComputationTopology.PuncutationFeedbackStreawm)) {
            long receivedPuncutation = tuple.getLong(0);
            if(currentPuncutationLowWaterMarker + puncutationGenrationFrequency == receivedPuncutation) {
                currentPuncutationLowWaterMarker = receivedPuncutation;
//                Slave.getInstance().sendMessageToMaster(String.format("Pending is updated to %d.", currentPuncutationLowWaterMarker));
                // resolve pending puntucations
                Collections.sort(pendingPruncutationUpdates);
                boolean updated = true;
                while(updated && pendingPruncutationUpdates.size() > 0) {
                    if(pendingPruncutationUpdates.get(0) == currentPuncutationLowWaterMarker + puncutationGenrationFrequency) {
                        currentPuncutationLowWaterMarker = pendingPruncutationUpdates.get(0);
                        pendingPruncutationUpdates.remove(0);
                        updated = true;
//                        Slave.getInstance().sendMessageToMaster(String.format("Pending is updated to %d by history.", currentPuncutationLowWaterMarker));
                    } else if(pendingPruncutationUpdates.get(0) < currentPuncutationLowWaterMarker + puncutationGenrationFrequency) {
                        // clean the old punctuation.
                        pendingPruncutationUpdates.remove(0);
                    } else {
                        updated = false;
                    }
                }

            } else {
                pendingPruncutationUpdates.add(receivedPuncutation);
//                Slave.getInstance().sendMessageToMaster(String.format("%d is added into pending history!", receivedPuncutation));
            }

//            currentPuncutationLowWaterMarker = Math.max(currentPuncutationLowWaterMarker, tuple.getLong(0));
//            Slave.getInstance().logOnMaster(String.format("PRUC is updated to %d", currentPuncutationLowWaterMarker));
        }
    }

}
