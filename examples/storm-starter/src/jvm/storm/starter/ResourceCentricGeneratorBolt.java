package storm.starter;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.routing.TwoTireRouting;
import backtype.storm.elasticity.utils.surveillance.ThroughputMonitor;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.collections.map.HashedMap;
import storm.starter.util.*;
import storm.trident.operation.builtin.Debug;

import java.util.*;

/**
 * Created by acelzj on 03/05/16.
 */
public class ResourceCentricGeneratorBolt implements IRichBolt{

    KeyGenerator _generator;
    OutputCollector _collector;
    int _numberOfElements;
    double _exponent;
    Thread _emitThread;
    transient ThroughputMonitor monitor;
    transient TwoTireRouting routingTable;
    transient Permutation permutation;

    private int numberOfComputingTasks;
    private List<Integer> downStreamTaskIds;
    private List<Long> pendingPruncutationUpdates;

    private int payloadSize;

    private int _emit_cycles;
    private int taskId;
    private int taskIndex;
    int _prime;

    final boolean enableMannualACK = true;

    transient private BackPressure backPressure;

    final private int puncutationGenrationFrequency = 5000;
    final private int numberOfPendingTuple = 100000;
    private volatile long currentPuncutationLowWaterMarker = 0;
//    private long currentPuncutationLowWaterMarker = 10000000L;
//    private long progressPermission = 200;

   final int[] primes = {104179, 104183, 104207, 104231, 104233, 104239, 104243, 104281, 104287, 104297,
     104309, 104311, 104323, 104327, 104347, 104369, 104381, 104383, 104393, 104399,
           104417, 104459, 104471, 104473, 104479, 104491, 104513, 104527, 104537, 104543,
           104549, 104551, 104561, 104579, 104593, 104597, 104623, 104639, 104651, 104659,
           104677, 104681, 104683, 104693, 104701, 104707, 104711, 104717, 104723, 104729};

    emitKey _emitKey;

//    public class ChangeDistribution implements Runnable {
//
//        @Override
//        public void run() {
//            while (true) {
//                Utils.sleep(30000);
//                Random rand = new Random(1);
//                System.out.println("distribution has been changed");
//                _prime = primes[rand.nextInt(primes.length)];
//                Slave.getInstance().logOnMaster("distribution has been changed");
//            }
//        }
//    }

    public ResourceCentricGeneratorBolt(int emit_cycles, int numberOfKeys, double exponent, int payloadSize){
        _emit_cycles = emit_cycles;
        this._numberOfElements = numberOfKeys;
        this._exponent = exponent;
        _prime = 41;
        this.payloadSize = payloadSize;
    }

    public class emitKey implements Runnable {

        volatile boolean terminating = false;
        volatile boolean terminated = false;
        volatile DebugInfo debugInfo = new DebugInfo();

        public class DebugInfo{
            long count;
            String position;
        }

        public void terminate() {
            terminating = true;
            while(!terminated) {
                Utils.sleep(1);
            }
        }

        public void run() {
            try {
                Long backPressureTupleId = 0L;
                long count = 0;
                debugInfo.count = 0;
                byte[] payload = new byte[payloadSize];
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            while (true) {
                                Thread.sleep(5000);
                                System.out.println(String.format("emit debug info: count: %d, position: %s", debugInfo.count, debugInfo.position));
                                System.out.println(String.format("BackPressure info: %s", backPressure.toString()));
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
                while (true) {
                    debugInfo.position = "emit bk 1";

//                    if(enableMannualACK) {
//                        backPressure.tryAcquireNextTupleId();
//                        while (count >= currentPuncutationLowWaterMarker + numberOfPendingTuple && !terminating) {
//                            Thread.sleep(1);
//                        }
//                    }

                    if (enableMannualACK) {
                        backPressureTupleId = backPressure.tryAcquireNextTupleId();
                        while (backPressureTupleId == null && !terminating) {
                            Thread.sleep(1);
                            backPressureTupleId = backPressure.tryAcquireNextTupleId();
                        }
                    }



                    debugInfo.position = "emit bk 2";
                    if (terminating) {
                        terminated = true;
                        terminating = false;
                        break;
                    }
                    debugInfo.position = "emit bk 3";
                    Thread.sleep(_emit_cycles);
                    int key = generate();
                    key = permutation.get(key);

                    int pos = routingTable.route(key).originalRoute;
                    if (pos < 0 || pos >= downStreamTaskIds.size()) {
                        System.out.println(String.format("ERROR pos: key : %d, pos: %d", key, pos));
                        Slave.getInstance().logOnMaster(String.format("ERROR pos: key : %d, pos: %d", key, pos));
                    }
                    int targetTaskId = downStreamTaskIds.get(pos);

                    if(enableMannualACK) {
//                        if (count % puncutationGenrationFrequency == 0) {
//                            _collector.emitDirect(targetTaskId, ResourceCentricZipfComputationTopology.PuncutationEmitStream, new Values(count, taskId));
//                            Slave.getInstance().logOnMaster(String.format("[PUNC:] PUNC %d is sent to %d", count, targetTaskId));
//                        }
                        if (backPressureTupleId % puncutationGenrationFrequency ==0) {
                            _collector.emitDirect(targetTaskId, ResourceCentricZipfComputationTopology.PuncutationEmitStream, new Values(count, taskId));
                            Slave.getInstance().logOnMaster(String.format("[PUNC:] PUNC %d is sent to %d", count, targetTaskId));
                        }
                    }

                    debugInfo.position = "emit bk 4";
                    _collector.emitDirect(targetTaskId, new Values(key, payload));
                    debugInfo.position = "emit bk 5";

                    monitor.rateTracker.notify(1);

                    count++;
                    if (count % 10000 == 0) {
                    Slave.getInstance().logOnMaster(String.format("Task %d: generates %d tuples.", taskId, count));
                    }
                    if (count % 50 == 0) {
                        _collector.emit(ResourceCentricZipfComputationTopology.CountReportSteram, new Values(taskIndex, count));
                    }
                    debugInfo.position = "emit bk 6";
                }
            } catch (InterruptedException ee) {
//                Slave.getInstance().sendMessageToMaster("I was interrupted!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private synchronized int generate() {
        return _generator.generate();
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "payload"));
        declarer.declareStream("statics", new Fields("executorId", "Histogram"));
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

        routingTable = new TwoTireRouting(numberOfComputingTasks);

        permutation = new Permutation(_numberOfElements, _prime);

        createOrUpdateGenerator();

        monitor = new ThroughputMonitor(""+context.getThisTaskId());
        _emitKey = new emitKey();
        _emitThread = new Thread(_emitKey);
        _emitThread.start();
        pendingPruncutationUpdates = new ArrayList<>();
//        new Thread(new ChangeDistribution()).start();
        backPressure = new BackPressure(puncutationGenrationFrequency, numberOfPendingTuple);
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

    private synchronized void createOrUpdateGenerator() {
        if(_exponent > 0) {
            Slave.getInstance().logOnMaster("Zipf generator is used!");
            _generator = new ZipfKeyGenerator(_numberOfElements, _exponent);
        } else {
            Slave.getInstance().logOnMaster("Round robin generator is used!");
            _generator = new RoundRobinKeyGenerator(_numberOfElements);
        }
    }

    public void execute(Tuple tuple){
      //  setNumberOfElements(tuple);
      //  setExponent(tuple);
        if(tuple.getSourceStreamId().equals(Utils.DEFAULT_STREAM_ID)) {
            _numberOfElements = Integer.parseInt(tuple.getString(0));
            _exponent = Double.parseDouble(tuple.getString(1));
            int seed = tuple.getInteger(2);
            createOrUpdateGenerator();
            _prime = primes[seed % (primes.length)];
            permutation.shuffle(seed);
            Slave.getInstance().logOnMaster(String.format("Prime is changed to %d on task %d, keys = %d, exp = %1.2f", _prime, taskId, _numberOfElements, _exponent ));
        } else if (tuple.getSourceStreamId().equals(ResourceCentricZipfComputationTopology.UpstreamCommand)) {
            String command  = tuple.getString(0);
            if(command.equals("getHistograms")) {
                _collector.emit("statics", new Values(taskId, routingTable.getBucketsDistribution()));
            } else if (command.equals("pausing")) {
//                Slave.getInstance().logOnMaster("Received pausing command on " + executorId);
                int sourceTaskOffset = tuple.getInteger(1);
                int targetTaskOffset = tuple.getInteger(2);
                int shardId = tuple.getInteger(3);
                System.out.println("Begin to terminate emit thread..");
                _emitKey.terminate();
                System.out.println("Terminated!");
//                _emitThread.interrupt();
//                try {
//                    _emitThread.join();
////                    Slave.getInstance().logOnMaster("Sending thread is paused on " + executorId);
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                routingTable.reassignBucketToRoute(shardId, targetTaskOffset);
//                Slave.getInstance().logOnMaster("Routing table is updated on " + executorId);
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
            permutation.shuffle(_prime);
            Slave.getInstance().logOnMaster(String.format("Prime is changed to %d on task %d", _prime, taskId ));
        } else if (tuple.getSourceStreamId().equals(ResourceCentricZipfComputationTopology.CountPermissionStream)) {
//            Slave.getInstance().logOnMaster(String.format("Progress on task %d is updated to %d", executorId, progressPermission));
        } else if (tuple.getSourceStreamId().equals(ResourceCentricZipfComputationTopology.PuncutationFeedbackStreawm)) {
            long receivedPunctuation = tuple.getLong(0);
            backPressure.ack(receivedPunctuation);
            /////////////////// new mechanism ///////////////////
//            if(currentPuncutationLowWaterMarker + puncutationGenrationFrequency == receivedPunctuation) {
//                currentPuncutationLowWaterMarker = receivedPunctuation;
////                Slave.getInstance().sendMessageToMaster(String.format("[PUNC:] Pending is updated to %d.", currentPuncutationLowWaterMarker));
//                // resolve pending puntucations
//                Collections.sort(pendingPruncutationUpdates);
//                boolean updated = true;
//                while(updated && pendingPruncutationUpdates.size() > 0) {
//                    if(pendingPruncutationUpdates.get(0) == currentPuncutationLowWaterMarker + puncutationGenrationFrequency) {
//                        currentPuncutationLowWaterMarker = pendingPruncutationUpdates.get(0);
//                        pendingPruncutationUpdates.remove(0);
//                        updated = true;
////                        Slave.getInstance().sendMessageToMaster(String.format("[PUNC:] Pending %d is updated by history.", currentPuncutationLowWaterMarker));
//                    } else if(pendingPruncutationUpdates.get(0) < currentPuncutationLowWaterMarker + puncutationGenrationFrequency) {
//                        long value = pendingPruncutationUpdates.get(0);
//                        // clean the old punctuation.
//                        pendingPruncutationUpdates.remove(0);
//                        updated = true;
////                        Slave.getInstance().sendMessageToMaster(String.format("[PUNC:] old %d is removed!", value));
//                    } else {
//                        updated = false;
//                    }
//                }
//
//            } else {
//                pendingPruncutationUpdates.add(receivedPunctuation);
////                Slave.getInstance().sendMessageToMaster(String.format("[PUNC:] %d is added into pending history!", receivedPunctuation));
//            }
            /////////////////// old mechanism ///////////////////


//            currentPuncutationLowWaterMarker = Math.max(currentPuncutationLowWaterMarker, tuple.getLong(0));
//            Slave.getInstance().logOnMaster(String.format("PRUC is updated to %d", currentPuncutationLowWaterMarker));
        }
    }

}
