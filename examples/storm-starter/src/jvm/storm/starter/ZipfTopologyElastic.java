package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.surveillance.ThroughputMonitor;

import java.io.FileNotFoundException;
import org.apache.commons.math3.*;
import storm.starter.util.ComputationSimulator;

import java.io.Serializable;
import java.util.*;
/**
 * Created by acelzj on 19/04/16.
 */
public class ZipfTopologyElastic {
    public static class Computation extends BaseElasticBolt {

        int sleepTimeInMilics;

        public Computation(int sleepTimeInSecs) {
            this.sleepTimeInMilics = sleepTimeInSecs;
        }

        @Override
        public void execute(Tuple tuple, ElasticOutputCollector collector) {
            System.out.println("execute");
//        utils.sleep(sleepTimeInMilics);
            ComputationSimulator.compute(sleepTimeInMilics*1000000);
            String number = tuple.getString(0);
            Integer count = (Integer)getValueByKey(number);
            if (count == null)
                count = 0;
            count++;
            setValueByKey(number,count);
            collector.emit(tuple,new Values(number, count));
        }



        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("number", "count"));
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            declareStatefulOperator();
        }

        @Override
        public Serializable getKey(Tuple tuple) {
            return tuple.getString(0);
        }
    }

    public static class Printer extends BaseBasicBolt {

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
      System.out.println(input.getString(0)+"--->"+input.getInteger(1));

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

        if(args.length < 2) {
            System.out.println("the number of args should be at least 2");
        }
        builder.setSpout("spout", new ZipfGeneratorSpout(Integer.parseInt(args[1])),1);

        builder.setBolt("count", new Computation(Integer.parseInt(args[2])),1).fieldsGrouping("spout", new Fields("worker_nums"));
   //     builder.setBolt("print", new Printer(),1).globalGrouping("count");

        Config conf = new Config();
     //   if(args.length>2&&args[2].equals("debug"))
     //       conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(8);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
      /*  else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("zipf-compute", conf, builder.createTopology());

            Thread.sleep(1000000);

            cluster.shutdown();
        }*/
    }
}
