package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.spout.RandomSentenceSpout;
import storm.starter.util.ComputationSimulator;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;

/**
 * Created by robert on 12/7/15.
 */
public class WordCountTopologyElastic {

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

    public static class WordCount extends BaseElasticBolt {

        int sleepTimeInMilics;

        public WordCount(int sleepTimeInSecs) {
            this.sleepTimeInMilics = sleepTimeInSecs;

        }

        @Override
        public void execute(Tuple tuple, ElasticOutputCollector collector) {
//        utils.sleep(sleepTimeInMilics);
//            Utils.sleep(sleepTimeInMilics/1000/1000);
            ComputationSimulator.compute(sleepTimeInMilics);
//            System.out.println("Time: " + sleepTimeInMilics);
            String word = tuple.getString(0);
            Integer count = (Integer)getValueByKey(word);
            if (count == null)
                count = 0;
            count++;
            setValueByKey(word,count);
            collector.emit(tuple,new Values(word, count));
        }



        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            declareStatefulOperator();

//            /**
//             * This thread periodically changes the sleep date of the execute() function,
//             * to test the elasticity of the system, i.e., the ability to scale in and out
//             * according to the current workload.
//             */
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    int i = 1;
//                    int old = sleepTimeInMilics;
//                    while(true) {
//                        Utils.sleep(5000);
//                        sleepTimeInMilics = old * i;
//                        Slave.getInstance().sendMessageToMaster("# sleep date: " + sleepTimeInMilics);
//                        i = (i+1)%15 + 1;
//                    }
//                }
//            }).start();
        }

        @Override
        public Serializable getKey(Tuple tuple) {
            return tuple.getString(0);
        }
    }

    public static class Printer extends BaseBasicBolt {

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
//      System.out.println(input.getString(0)+"--->"+input.getInteger(1));
        }

//        @Override
//        public Object getKey(Tuple tuple) {
//            return tuple.getString(0);
//        }
//
//        @Override
//        public void execute(Tuple input, ElasticOutputCollector collector) {
//            System.out.println(input.getString(0)+"--->"+input.getInteger(1));
//        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) throws Exception {

        if(args.length == 0) {
            System.out.println("args: topology-name sleep-date-in-millis [debug|any other]");
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new MyWordCount.WordGenerationSpout(0), 1);

        builder.setBolt("count", new WordCount(Integer.parseInt(args[1])), 1).fieldsGrouping("spout", new Fields("word"));
        builder.setBolt("print", new Printer(),2).globalGrouping("count");

        Config conf = new Config();
        if(args.length>2&&args[2].equals("debug"))
            conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(8);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("word-count", conf, builder.createTopology());
//
//            Thread.sleep(1000000);
//
//            cluster.shutdown();
        }
    }
}
