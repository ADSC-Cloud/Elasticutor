package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.ElasticTaskHolder;
import backtype.storm.elasticity.state.KeyValueState;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import storm.starter.spout.InputGeneratorSpout;

import java.text.DecimalFormat;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by robert on 1/8/16.
 */
public class ElasticTopologySimulator {
    public static class ComputationSimulator {
        public static long compute(int timeInNanosecond) {
            final long start = System.nanoTime();
            long seed = start;
            while(System.nanoTime() - start < timeInNanosecond) {
                seed = (long) Math.sqrt(new Random().nextInt());
            }
            return seed;
        }
    }
    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class ElasticBolt extends BaseElasticBolt {

        int computationCostPerTupleInNanoseconds;
        int warmupTimeInSeconds;
        long startTime;
        int payloadSize;

        public ElasticBolt(int computationCostPerTupleInNanoseconds, int payloadSize, int warmupTimeInSeconds) {
            this.computationCostPerTupleInNanoseconds = computationCostPerTupleInNanoseconds;
            this.payloadSize = payloadSize;
            this.warmupTimeInSeconds = warmupTimeInSeconds;
        }

        public ElasticBolt(int computationCostPerTupleInNanoseconds, int warmupInSeconds) {
            this(computationCostPerTupleInNanoseconds, 0 , warmupInSeconds);
        }

        public ElasticBolt(int computationCostPerTupleInNanoseconds) {
            this(computationCostPerTupleInNanoseconds, 0);
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            declareStatefulOperator();
            startTime = System.currentTimeMillis();
        };

        @Override
        public void execute(Tuple tuple, ElasticOutputCollector collector) {

            if(System.currentTimeMillis() - startTime >= warmupTimeInSeconds * 1000)
                ComputationSimulator.compute(computationCostPerTupleInNanoseconds);

            Long key = tuple.getLong(0);
            Object[] value = (Object[])getValueByKey(key);
            if (value == null) {
                value = new Object[2];
                value[0] = 0L;
                if(payloadSize>0)
                    value[1] = new byte[payloadSize];
            }
            value[0] = (long)value[0] +1;
            setValueByKey(key, value);

            collector.emit(new Values(key, value[0], tuple.getLong(1)));



//            Long count = (Long)getValueByKey(key);
//            if (count == null)
//                count = 0L;
//            count++;
//            setValueByKey(key,count);
//
//            collector.emit(tuple, new Values(key, count, tuple.getLong(1)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "payload", "timestamp"));
        }

        @Override
        public Object getKey(Tuple tuple) {
            return tuple.getLong(0);
        }
    }

    public static class FinishingBolt extends BaseBasicBolt {

        private AtomicLong latencyCount;

        private AtomicLong latencySum;

        Random random;


        public void prepare(Map stormConf, TopologyContext context) {
            latencyCount = new AtomicLong(0);
            latencySum = new AtomicLong(0);
            final Integer taskId = context.getThisTaskId();
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        DecimalFormat format = new DecimalFormat("####.#######");
//                        while(true) {
//                            Thread.sleep(1000);
//                            double averageLatency = 0;
//                            if(latencyCount.get()!=0) {
//                                averageLatency = (double)latencySum.get()/latencyCount.get();
//                            }
//
//                            ElasticTaskHolder.instance().sendMessageToMaster("Task " + taskId + ": " + format.format(averageLatency) + " ms" + " (" + latencyCount.get() + " tuples evaluated)");
//                            latencySum.set(0);
//                            latencyCount.set(0);
//                        }
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            }).start();
            random = new Random();

        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
//            System.out.println(input.getString(0)+"--->"+input.getInteger(1));
            final long startTimestamp = input.getLong(2);
            final long latency = System.currentTimeMillis() - startTimestamp;

//            if(random.nextInt(1000)<10) {
                latencyCount.addAndGet(1);
                latencySum.addAndGet(latency);
//            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 5) {
            System.out.println("args: topology-name state-size-in-KB sleep-date-in-microseconds value-layload-size warm-up-date-in-seconds");
            System.out.println("Please correct the arguments and try again!");
            return;
        }

        final int numberOfDistinctCount = Integer.parseInt(args[1])*1024;
        final int computationTimePerTupleInNanoSeconds = Integer.parseInt(args[2]) * 1000;
        final int payloadSize = Integer.parseInt(args[3]);
        final int warmUpTimeInSeconds = Integer.parseInt(args[4]);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new InputGeneratorSpout(numberOfDistinctCount, warmUpTimeInSeconds), 2);

        builder.setBolt("count", new ElasticBolt(computationTimePerTupleInNanoSeconds, payloadSize, warmUpTimeInSeconds), 1).fieldsGrouping("spout", new Fields("key"));
        builder.setBolt("FinishingBolt", new FinishingBolt(),1).globalGrouping("count");

        Config conf = new Config();
//        if(args.length>2&&args[2].equals("debug"))
//            conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(8);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }

    }
}

