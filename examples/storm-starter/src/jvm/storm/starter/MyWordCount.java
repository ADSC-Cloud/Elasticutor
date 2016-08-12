package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
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
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by robert on 8/18/15.
 */




public class MyWordCount {
    public static class WordGenerationSpout extends BaseRichSpout {

        private int count = 0;

//        private ThroughputMonitor monitor;

        public WordGenerationSpout() {
            this(0);
        }

        public WordGenerationSpout(int emit_cycles){
            _emit_cycles=emit_cycles;
            _random.setSeed(System.currentTimeMillis());
            String wordset = "5. You are not required to accept this License, since you have not signed it. However,\n" +
                    "nothing else grants you permission to modify or distribute the Program or its\n" +
                    "derivative works. These actions are prohibited by law if you do not accept this License.\n" +
                    "Therefore, by modifying or distributing the Program (or any work based on the Program),\n" +
                    "you indicate your acceptance of this License to do so, and all its terms and conditions\n" +
                    "for copying, distributing or modifying the Program or works based on it.\n" +
                    "\n" +
                    "6. Each time you redistribute the Program (or any work based on the Program), the\n" +
                    "recipient automatically receives a license from the original licensor to copy,\n" +
                    "distribute or modify the Program subject to these terms and conditions. You may not\n" +
                    "impose any further restrictions on the recipients' exercise of the rights granted\n" +
                    "herein. You are not responsible for enforcing compliance by third parties to this\n" +
                    "License.\n" +
                    "\n" +
                    "7. If, as a consequence of a court judgment or allegation of patent infringement or\n" +
                    "for any other reason (not limited to patent issues), conditions are imposed on you\n" +
                    "(whether by court order, agreement or otherwise) that contradict the conditions of\n" +
                    "this License, they do not excuse you from the conditions of this License. If you cannot\n" +
                    "distribute so as to satisfy simultaneously your obligations under this License and\n" +
                    "any other pertinent obligations, then as a consequence you may not distribute the\n" +
                    "Program at all. For example, if a patent license would not permit royalty-free\n" +
                    "redistribution of the Program by all those who receive copies directly or indirectly\n" +
                    "through you, then the only way you could satisfy both it and this License would be\n" +
                    "to refrain entirely from distribution of the Program.\n" +
                    "\n" +
                    "If any portion of this section is held invalid or unenforceable under any particular\n" +
                    "circumstance, the balance of the section is intended to apply and the section as a\n" +
                    "whole is intended to apply in other circumstances.\n" +
                    "\n" +
                    "It is not the purpose of this section to induce you to infringe any patents or other\n" +
                    "property right claims or to contest validity of any such claims; this section has\n" +
                    "the sole purpose of protecting the integrity of the free software distribution system,\n" +
                    "which is implemented by public license practices. Many people have made generous\n" +
                    "contributions to the wide range of software distributed through that system in\n" +
                    "reliance on consistent application of that system; it is up to the author/donor to\n" +
                    "decide if he or she is willing to distribute software through any other system and\n" +
                    "a licensee cannot impose that choice.\n" +
                    "\n" +
                    "This section is intended to make thoroughly clear what is believed to be a consequence\n" +
                    "of the rest of this License.\n" +
                    "\n" +
                    "8. If the distribution and/or use of the Program is restricted in certain countries\n" +
                    "either by patents or by copyrighted interfaces, the original copyright holder who\n" +
                    "places the Program under this License may add an explicit geographical distribution\n" +
                    "limitation excluding those countries, so that distribution is permitted only in or\n" +
                    "among countries not thus excluded. In such case, this License incorporates the\n" +
                    "limitation as if written in the body of this License.\n" +
                    "\n" +
                    "9. The Free Software Foundation may publish revised and/or new versions of the General\n" +
                    "Public License from time to time. Such new versions will be similar in spirit to the\n" +
                    "present version, but may differ in detail to address new problems or concerns.\n" +
                    "\n" +
                    "Each version is given a distinguishing version number. If the Program specifies a\n" +
                    "version number of this License which applies to it and \"any later version\", you have\n" +
                    "the option of following the terms and conditions either of that version or of any\n" +
                    "later version published by the Free Software Foundation. If the Program does not\n" +
                    "specify a version number of this License, you may choose any version ever published\n" +
                    "by the Free Software Foundation.\n" +
                    "10. If you wish to incorporate parts of the Program into other free programs whose" +
                    "distribution conditions are different, write to the author to ask for permission." +
                    "For software which is copyrighted by the Free Software Foundation, write to the Free" +
                    "Software Foundation; we sometimes make exceptions for this. Our decision will be" +
                    "guided by the two goals of preserving the free status of all derivatives of our free" +
                    "software and of promoting the sharing and reuse of software generally";


            _dictionary.addAll(Arrays.asList(wordset.split(" ")));
//            _dictionary.add("One");
//            _dictionary.add("Two");
//            _dictionary.add("Three");
//            _dictionary.add("Four");
        }
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
            _collector=collector;
//            monitor = new ThroughputMonitor(""+context.getThisTaskId());
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "payload"));
        }
        @Override
        public void nextTuple(){
            Utils.sleep(_emit_cycles);
//            long start = System.currentTimeMillis();
//            System.out.print("sending--->");
            _collector.emit(new Values(_dictionary.get(_random.nextInt(_dictionary.size())), _random.nextInt(_dictionary.size())));
            count++;
//            monitor.rateTracker.notify(1);
//            System.out.format("sent %d %d ms\n",count,System.currentTimeMillis() - start);
        }
        SpoutOutputCollector _collector;
        int _emit_cycles;
        Random _random =new Random();
        Vector<String> _dictionary = new Vector<>();
    }
    public static class MyCounter extends BaseElasticBolt {

        @Override
        public Object getKey(Tuple tuple) {
            return tuple.getString(0);
        }

        @Override
        public void execute(Tuple input, ElasticOutputCollector collector) {
            Utils.sleep(100);
            Object key = getKey(input);
            Long count = (Long)getValueByKey(key);
            if (count == null)
                count = 0L;
            count++;
            setValueByKey(key,count);
            collector.emit(input,new Values(key, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer){
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            declareStatefulOperator();
        }


        @Override
        public void cleanup(){
        }
    }

    public static class ReportBolt extends BaseBasicBolt {


        private long threshold;
        public ReportBolt(long th) {
            threshold = th;
        }
        public void execute(Tuple tuple, BasicOutputCollector collector){
            if(tuple.getLong(1)>threshold)
                System.out.println("[Result:]" + tuple.getString(0)+"--->"+tuple.getInteger(1));
        }


        public void declareOutputFields(OutputFieldsDeclarer d){
            d.declare(new Fields("result"));
        }
    }

    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new WordGenerationSpout(), 1);
        builder.setBolt("counter",new MyCounter(),1).fieldsGrouping("spout", new Fields("word"));
        builder.setBolt("printer", new ReportBolt(10000000000000L),1).fieldsGrouping("counter", new Fields("word"));


        Config conf = new Config();
        conf.setDebug(true);
        conf.put("backpressure.disruptor.high.watermark", 0.9);
        conf.put("backpressure.disruptor.low.watermark",0.85);

        if(args != null && args.length >0){
            conf.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }
            catch (Exception e){
                System.out.print("Error!");
            }
        }
        else {


            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Word-count", conf, builder.createTopology());

            Utils.sleep(10000000);
            cluster.shutdown();
        }

    }
}
