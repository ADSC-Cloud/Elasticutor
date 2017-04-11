package storm.starter; /**
 * Created by acelzj on 03/05/16.
 */

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.state.KeyValueState;
import backtype.storm.elasticity.utils.FrequencyRestrictor;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RateTracker;
import backtype.storm.utils.Utils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;

public class ResourceCentricComputationBolt extends BaseElasticBolt{
    int sleepTimeInNanoSeconds;

    List<Integer> upstreamTaskIds;

    private int taskId;

    int receivedMigrationCommand;

    ArrayBlockingQueue<Long> latencyHistory;

    transient RateTracker rateTracker;
    transient ElasticOutputCollector outputCollector;

    private int recordLatencyEveryNTuples;
    private long count;


    public ResourceCentricComputationBolt(int sleepTimeInNanoSeconds) {
        this.sleepTimeInNanoSeconds = sleepTimeInNanoSeconds;
    }

    @Override
    public void execute(Tuple tuple, ElasticOutputCollector collector) {
//        System.out.println("execute");
//        utils.sleep(sleepTimeInMilics);
        long start = System.nanoTime();
        if(outputCollector == null)
            outputCollector = collector;
        String streamId = tuple.getSourceStreamId();
        if(streamId.equals(Utils.DEFAULT_STREAM_ID)) {
//            final long currentTime = System.nanoTime();
//            Utils.sleep(sleepTimeInNanoSeconds);
//            final long executionLatency = System.nanoTime() - currentTime;
            if(count++ % recordLatencyEveryNTuples == 0) {
                latencyHistory.offer((long)sleepTimeInNanoSeconds);
                if (latencyHistory.size() > Config.numberOfLatencyHistoryRecords) {
                    latencyHistory.poll();
                }
                if(rateTracker!=null)
                    rateTracker.notify(recordLatencyEveryNTuples);
            }
//            String number = tuple.getString(0);
//            Integer count = (Integer) getValueByKey(number);
//            if (count == null)
//                count = 0;
//            count++;
//            setValueByKey(number, count);
//            collector.emit(tuple, new Values(number, count));

//            ElasticTopologySimulator.ComputationSimulator.compute((long)sleepTimeInNanoSeconds - (System.nanoTime() - start));
//            Utils.sleep(((long)sleepTimeInNanoSeconds)/1000000);
            if (sleepTimeInNanoSeconds <= 1000000) {
                if (count % (1000000 / sleepTimeInNanoSeconds) == 0) {
                    Utils.sleep(1);
                }
            } else {
                Utils.sleep(sleepTimeInNanoSeconds / 1000000);
            }


        } else if (streamId.equals(ResourceCentricZipfComputationTopology.StateMigrationCommandStream)) {
            receivedMigrationCommand++;
            int sourceTaskOffset = tuple.getInteger(0);
            int targetTaskOffset = tuple.getInteger(1);
            int shardId = tuple.getInteger(2);
            if(receivedMigrationCommand == 1) {
//                Slave.getInstance().logOnMaster(String.format("Task %d Received StateMigrationCommand %d: %d--->%d.", executorId, shardId, sourceTaskOffset, targetTaskOffset));
            }
            if(receivedMigrationCommand==upstreamTaskIds.size()) {
//                Slave.getInstance().logOnMaster(String.format("Task %d Received StateMigrationCommand %d: %d--->%d.", executorId, shardId, sourceTaskOffset, targetTaskOffset));

                // received the migration command from each of the upstream tasks.
                receivedMigrationCommand = 0;
                KeyValueState state = getState();

                state.getState().put("key", new byte[1024 * 32]);

                Slave.getInstance().logOnMaster("State migration starts!");
                collector.emit(ResourceCentricZipfComputationTopology.StateMigrationStream, tuple, new Values(sourceTaskOffset, targetTaskOffset, shardId, state));
            }
        } else if (streamId.equals(ResourceCentricZipfComputationTopology.StateUpdateStream)) {
//            Slave.getInstance().logOnMaster("Recieved new state!");
            int targetTaskOffset = tuple.getInteger(0);
            KeyValueState state = (KeyValueState) tuple.getValue(1);
            getState().update(state);
            Slave.getInstance().logOnMaster("State is updated!");
            collector.emit(ResourceCentricZipfComputationTopology.StateReadyStream, tuple, new Values(targetTaskOffset));

        } else if (streamId.equals(ResourceCentricZipfComputationTopology.PuncutationEmitStream)) {
            long pruncutation = tuple.getLong(0);
            int taskid = tuple.getInteger(1);
            collector.emitDirect(taskid, ResourceCentricZipfComputationTopology.PuncutationFeedbackStreawm, new Values(pruncutation));
            Slave.getInstance().logOnMaster(String.format("[PUNC:] PRUN %d is sent back to %d by %d", pruncutation, taskid, this.taskId));
        }
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number", "count"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.StateMigrationStream, new Fields("sourceTaskId", "targetTaskId", "shardId", "state"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.StateReadyStream, new Fields("targetTaskId"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.RateAndLatencyReportStream, new Fields("TaskId", "rate", "latency"));
        declarer.declareStream(ResourceCentricZipfComputationTopology.PuncutationFeedbackStreawm, new Fields("punctuation"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();
        upstreamTaskIds = context.getComponentTasks("generator");
        receivedMigrationCommand = 0;
        latencyHistory = new ArrayBlockingQueue<>(Config.numberOfLatencyHistoryRecords);
        rateTracker = new RateTracker(1000,5);

        taskId = context.getThisTaskId();

        recordLatencyEveryNTuples = (int)(1 / Config.latencySampleRate);

        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    Utils.sleep(1000);
                    if(outputCollector==null)
                        continue;
                    Long sum = 0L;
                    for(Long latency: latencyHistory) {
                        sum += latency;
                    }
                    long latency = 1;
                    if(latencyHistory.size()!=0) {
                        latency = sum / latencyHistory.size();
                    }
                    outputCollector.emit(ResourceCentricZipfComputationTopology.RateAndLatencyReportStream, null, new Values(taskId, rateTracker.reportRate(), latency));
                }
            }
        }).start();

    }

    @Override
    public Serializable getKey(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Utils.DEFAULT_STREAM_ID))
            return tuple.getInteger(0);
        else
            return -1;
    }

}
