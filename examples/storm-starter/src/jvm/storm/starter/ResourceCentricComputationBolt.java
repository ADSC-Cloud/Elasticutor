package storm.starter; /**
 * Created by acelzj on 03/05/16.
 */

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.state.KeyValueState;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RateTracker;
import backtype.storm.utils.Utils;
import storm.starter.util.ComputationSimulator;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ResourceCentricComputationBolt extends BaseElasticBolt{
    int sleepTimeInMilics;

    List<Integer> upstreamTaskIds;

    ConcurrentLinkedQueue<Long> latencyHistory;

    transient RateTracker rateTracker;

    int receivedMigrationCommand;

    transient ElasticOutputCollector outputCollector;
    private int taskId;

    public ResourceCentricComputationBolt(int sleepTimeInSecs) {
        this.sleepTimeInMilics = sleepTimeInSecs;
    }

    @Override
    public void execute(Tuple tuple, ElasticOutputCollector collector) {
//        System.out.println("execute");
//        utils.sleep(sleepTimeInMilics);
        if(outputCollector == null)
            outputCollector = collector;
        String streamId = tuple.getSourceStreamId();
        if(streamId.equals(Utils.DEFAULT_STREAM_ID)) {
            final long currentTime = System.nanoTime();
//            ComputationSimulator.compute(sleepTimeInMilics * 1000000);
            Utils.sleep(sleepTimeInMilics);
            final long executionLatency = System.nanoTime() - currentTime;
            latencyHistory.offer(executionLatency);
            if(latencyHistory.size() > Config.numberOfLatencyHistoryRecords) {
                latencyHistory.poll();
            }
            String number = tuple.getString(0);
            Integer count = (Integer) getValueByKey(number);
            if (count == null)
                count = 0;
            count++;
            setValueByKey(number, count);
            if(rateTracker!=null)
                rateTracker.notify(1);
//            collector.emit(tuple, new Values(number, count));
        } else if (streamId.equals(ResourceCentricZipfComputationTopology.StateMigrationCommandStream)) {
            receivedMigrationCommand++;
            int sourceTaskOffset = tuple.getInteger(0);
            int targetTaskOffset = tuple.getInteger(1);
            int shardId = tuple.getInteger(2);
            if(receivedMigrationCommand == 1) {
//                Slave.getInstance().logOnMaster(String.format("Task %d Received StateMigrationCommand %d: %d--->%d.", taskId, shardId, sourceTaskOffset, targetTaskOffset));
            }
            if(receivedMigrationCommand==upstreamTaskIds.size()) {
//                Slave.getInstance().logOnMaster(String.format("Task %d Received StateMigrationCommand %d: %d--->%d.", taskId, shardId, sourceTaskOffset, targetTaskOffset));

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
//            Slave.getInstance().logOnMaster(String.format("PRUN %d is sent back to %d by %d", pruncutation, taskid, taskId));
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
        latencyHistory = new ConcurrentLinkedQueue<>();
        rateTracker = new RateTracker(1000,5);

        taskId = context.getThisTaskId();

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
        return (Serializable) tuple.getValue(0);
    }

}
