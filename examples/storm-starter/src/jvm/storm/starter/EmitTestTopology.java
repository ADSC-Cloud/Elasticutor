package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by robert on 15/4/16.
 */
public class EmitTestTopology {

    static public class MySpout extends BaseRichSpout {

        SpoutOutputCollector collector;
        int number = 0;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("number"));
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
//            Utils.sleep(1000);
            collector.emit(new Values(number++), new Object());
            System.out.println("Emit: " + number);
        }
    }

    static public class MyBolt extends BaseRichBolt {


        List<Integer> downstreamTasks;
        OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            downstreamTasks = context.getComponentTasks("printer");
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            Utils.sleep(1);
            int number = input.getInteger(0);
            int targetTaskId = downstreamTasks.get(number % downstreamTasks.size());
            collector.emitDirect(targetTaskId, new Values(number));
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("number"));
        }
    }

    static public class MyElasticBolt extends BaseElasticBolt {
        List<Integer> downstreamTasks;
        @Override
        public Serializable getKey(Tuple tuple) {
            return new Random().nextInt();
        }

        @Override
        public void execute(Tuple input, ElasticOutputCollector collector) {
            Utils.sleep(1);
            int number = input.getInteger(0);
            int targetTaskId = downstreamTasks.get(number % downstreamTasks.size());
            collector.emitDirect(targetTaskId, new Values(number));
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("number"));
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            declareStatefulOperator();
            downstreamTasks = context.getComponentTasks("printer");
        }

    }

    static public class Printer extends BaseRichBolt {

        int taskId;


        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            taskId = context.getThisTaskId();
        }

        @Override
        public void execute(Tuple input) {
            System.out.println("Task" + taskId + ": " + input.getInteger(0));
        }
    }

    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new MySpout());

        builder.setBolt("mybolt", new MyElasticBolt(),1).shuffleGrouping("spout");

        builder.setBolt("printer", new Printer(),2).directGrouping("mybolt");


        Config conf = new Config();
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }

}
