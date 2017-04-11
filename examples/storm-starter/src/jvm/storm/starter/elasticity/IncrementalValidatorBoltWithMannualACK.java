package storm.starter.elasticity;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.util.BackPressure;
import storm.starter.util.KeyGenerator;
import storm.starter.util.RoundRobinKeyGenerator;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by robert on 22/12/16.
 */
public class IncrementalValidatorBoltWithMannualACK extends BaseRichBolt {

    private OutputCollector collector;
    private Map<Integer, Long> keyToCount;
    private int numberOfKeys;
    private KeyGenerator generator;
    private int taskId;

    private int ackFrequency = 5000;
    private long ackMaxPending = 100000;
    BackPressure backPressure;

    public IncrementalValidatorBoltWithMannualACK(int numberOfKeys, int ackFrequency, long ackMaxPending) {
        this.numberOfKeys = numberOfKeys;
        this.ackFrequency = ackFrequency;
        this.ackMaxPending = ackMaxPending;
    }


//    @Override
//    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//
//    }
//
//    @Override
//    public void nextTuple() {
//        final Integer key = generator.generate();
//        final Long count = keyToCount.get(key);
//        keyToCount.put(key, count + 1);
//        if (acked)
//            collector.emit(new Values(key, count), new Object());
//        else
//            collector.emit(new Values(key, count));
//    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "count"));
        declarer.declareStream("backpressure", new Fields("tupleId", "taskId"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        final OutputCollector outputCollector = collector;
        keyToCount = new HashMap<>();
        for(int i = 0; i < numberOfKeys; i++) {
            keyToCount.put(i, 0L);
        }
        generator = new RoundRobinKeyGenerator(numberOfKeys);

        backPressure = new BackPressure(ackFrequency, ackMaxPending);

        this.taskId = context.getThisTaskId();

        new Thread(new Runnable() {
            @Override
            public void run() {
                Long tupleId = 0L;
                try {
                    while (true) {
                        try {
                            tupleId = backPressure.acquireNextTupleId();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        final Integer key = generator.generate();
                        final Long count = keyToCount.get(key);
                        keyToCount.put(key, count + 1);
                        outputCollector.emit(new Values(key, count));

                        if (tupleId % ackFrequency == 0) {
                            outputCollector.emit("backpressure", new Values(tupleId, taskId));
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }).start();
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals("backpressure")) {
            Long tupleId = input.getLong(0);
            backPressure.ack(tupleId);
        }
    }
}
